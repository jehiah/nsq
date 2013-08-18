// This is an NSQ client that reads the specified topic/channel
// and re-publishes the messages to destination nsqd via TCP

package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/bitly/go-hostpool"
	"github.com/bitly/nsq/nsq"
	"github.com/bitly/nsq/util"
	"log"
	"math"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"
	"math/rand"
)

const (
	ModeRoundRobin = iota
	ModeHostPool
)

var (
	showVersion        = flag.Bool("version", false, "print version string")
	topic              = flag.String("topic", "", "nsq topic")
	channel            = flag.String("channel", "nsq_to_http", "nsq channel")
	destTopic          = flag.String("destination-topic", "", "destination nsq topic")
	maxInFlight        = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	verbose            = flag.Bool("verbose", false, "enable verbose logging")
	throttleFraction   = flag.Float64("throttle-fraction", 1.0, "publish only a fraction of messages")
	numPublishers      = flag.Int("n", 2, "number of concurrent publishers")
	statusEvery        = flag.Int("status-every", 250, "the # of requests between logging status (per handler), 0 disables")
	mode               = flag.String("mode", "", "the upstream request mode options: round-robin (default), hostpool")
	maxBackoffDuration = flag.Duration("max-backoff-duration", 120*time.Second, "the maximum backoff duration")

	nsqdTCPAddrs       = util.StringArray{}
	lookupdHTTPAddrs   = util.StringArray{}
	destNsqdTCPAddrs   = util.StringArray{}

	tlsEnabled            = flag.Bool("tls", false, "enable TLS")
	tlsInsecureSkipVerify = flag.Bool("tls-insecure-skip-verify", false, "disable TLS server certificate validation")
)

func init() {
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&destNsqdTCPAddrs, "destination-nsqd-tcp-address", "destination nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

type Durations []time.Duration

func (s Durations) Len() int {
	return len(s)
}

func (s Durations) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Durations) Less(i, j int) bool {
	return s[i] < s[j]
}

type PublishHandler struct {
	addresses util.StringArray
	writers   map[string]*Writer
	mode      int
	counter   uint64
	hostPool  hostpool.HostPool
	reqs      Durations
	id        int
}

func (ph *PublishHandler) HandleMessage(m *nsq.Message, respChan chan *nsq.FinishedMessage) error {
	var startTime time.Time

	transaction := &publishMessage{
		topic: *destTopic,
		msg: m,
		fin: respChan
	}
	finishThisMessage := func(err error) {
		respChan <- &nsq.FinishedMessage{
			Id:             m.Id,
			RequeueDelayMs: (time.Duration(90) * time.Second * m.Attempts) / time.Milisecond,
			Success:        err == nil,
		}
	}

	if *throttleFraction < 1.0 && rand.Float64() > *throttleFraction {
	        return nil
	}

	if *statusEvery > 0 {
		startTime = time.Now()
	}

	switch ph.mode {
	case ModeRoundRobin:
		idx := ph.counter % uint64(len(ph.addresses))
		writer := ph.writers[ph.addresses[idx]]
		_, _, err := writer.Publish(*destTopic, m.Body)
		if err != nil {
			finishThisMessage(err)
		} else {
			ph.counter++
		}
	case ModeHostPool:
		hostPoolResponse := ph.hostPool.Get()
		writer := ph.writers[hostPoolResponse.Host()]
		_, _, err := writer.Publish(*destTopic, m.Body)
		hostPoolResponse.Mark(err)
		if err != nil {
			finishThisMessage(err)
		}
	}

	if *statusEvery > 0 {
		duration := time.Now().Sub(startTime)
		ph.reqs = append(ph.reqs, duration)
	}

	if *statusEvery > 0 && len(ph.reqs) >= *statusEvery {
		var total time.Duration
		for _, v := range ph.reqs {
			total += v
		}
		avgMs := (total.Seconds() * 1000) / float64(len(ph.reqs))

		sort.Sort(ph.reqs)
		p95Ms := percentile(95.0, ph.reqs, len(ph.reqs)).Seconds() * 1000
		p99Ms := percentile(99.0, ph.reqs, len(ph.reqs)).Seconds() * 1000

		log.Printf("handler(%d): finished %d requests - 99th: %.02fms - 95th: %.02fms - avg: %.02fms",
			ph.id, *statusEvery, p99Ms, p95Ms, avgMs)

		ph.reqs = ph.reqs[:0]
	}

	return nil
}

func percentile(perc float64, arr []time.Duration, length int) time.Duration {
	indexOfPerc := int(math.Ceil(((perc / 100.0) * float64(length)) + 0.5))
	if indexOfPerc >= length {
		indexOfPerc = length - 1
	}
	return arr[indexOfPerc]
}

func main() {
	var selectedMode int

	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_nsq v%s\n", util.BINARY_VERSION)
		return
	}

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic and --channel are required")
	}

	if *destTopic == "" {
		*destTopic = *topic
	}

	if *maxInFlight < 0 {
		log.Fatalf("--max-in-flight must be > 0")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatalf("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatalf("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	if len(destNsqdTCPAddrs) == 0 {
		log.Fatalf("--destination-nsqd-tcp-address required")
	}

	switch *mode {
	case "round-robin":
		selectedMode = ModeRoundRobin
	case "hostpool":
		selectedMode = ModeHostPool
	}

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	r, err := nsq.NewReader(*topic, *channel)
	if err != nil {
		log.Fatalf(err.Error())
	}
	r.SetMaxInFlight(*maxInFlight)
	r.SetMaxBackoffDuration(*maxBackoffDuration)
	r.VerboseLogging = *verbose

	if *tlsEnabled {
		r.TLSv1 = true
		r.TLSConfig = &tls.Config{
			InsecureSkipVerify: *tlsInsecureSkipVerify,
		}
	}

	writers := make(map[string]*nsq.Writer)
	for _, addr := range destNsqdTCPAddrs {
		writer := NewWriter(int((nsq.DefaultClientTimeout / 2) / time.Millisecond))
		writer.ConnectToNSQ(addr)
		writers[addr] = writer
	}

	handler := &PublishHandler{
		addresses: destNsqdTCPAddrs,
		writers:   writers,
		mode:      selectedMode,
		reqs:      make(Durations, 0, *statusEvery),
		id:        i,
		hostPool:  hostpool.New(destNsqdTCPAddrs),
	}
	for i := 0; i < *numPublishers; i++ {
		r.AddAsyncHandler(handler)
	}

	for _, addrString := range nsqdTCPAddrs {
		err := r.ConnectToNSQ(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	for _, addrString := range lookupdHTTPAddrs {
		log.Printf("lookupd addr %s", addrString)
		err := r.ConnectToLookupd(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	for {
		select {
		case <-r.ExitChan:
			return
		case <-termChan:
			r.Stop()
		}
	}
}