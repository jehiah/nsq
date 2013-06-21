// This is an NSQ client that reads the specified topic/channel
// and relays it to one (or more) NSQD instances. This is particularly
// useful for relaying data between datacenters, or to relay to a new topic

package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"github.com/bitly/go-hostpool"
	"github.com/bitly/nsq/nsq"
	"github.com/bitly/nsq/util"
	"log"
	"math"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"
)

const (
	ModeRoundRobin = iota
	ModeHostPool
)

var (
	showVersion        = flag.Bool("version", false, "print version string")
	topic              = flag.String("topic", "", "nsq topic")
	channel            = flag.String("channel", "nsq_to_http", "nsq channel")
	maxInFlight        = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	verbose            = flag.Bool("verbose", false, "enable verbose logging")
	mode               = flag.String("mode", "", "the upstream request mode options: round-robin, hostpool")
	throttleFraction   = flag.Float64("throttle-fraction", 1.0, "publish only a fraction of messages")
	statusEvery        = flag.Int("status-every", 250, "the # of requests between logging status, 0 disables")
	maxBackoffDuration = flag.Duration("max-backoff-duration", 120*time.Second, "the maximum backoff duration")

	nsqdTCPAddrs     = util.StringArray{}
	lookupdHTTPAddrs = util.StringArray{}
	nsqDestTCPAddrs  = util.StringArray{}
)

func init() {
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address [source] (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Var(&nsqdDestTCPAddrs, "dest-nsqd-tcp-address", "nsqd TCP address [destination] (may be given multiple times)")
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

type Publisher interface {
	Publish(string, []byte) error
}

type PublishHandler struct {
	Publisher
	addresses util.StringArray
	counter   uint64
	mode      int
	hostPool  hostpool.HostPool
	reqs      Durations
	id        int
}

func (ph *PublishHandler) HandleMessage(m *nsq.Message) error {
	var startTime time.Time

	if *throttleFraction < 1.0 && rand.Float64() > *throttleFraction {
		return nil
	}

	if *statusEvery > 0 {
		startTime = time.Now()
	}

	switch ph.mode {
	case ModeRoundRobin:
		idx := ph.counter % uint64(len(ph.addresses))
		err := ph.Publish(ph.addresses[idx], m.Body)
		if err != nil {
			return err
		}
		ph.counter++
	case ModeHostPool:
		hostPoolResponse := ph.hostPool.Get()
		err := ph.Publish(hostPoolResponse.Host(), m.Body)
		hostPoolResponse.Mark(err)
		if err != nil {
			return err
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
	var publisher Publisher
	var addresses util.StringArray
	var selectedMode int

	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_nsq v%s\n", util.BINARY_VERSION)
		return
	}

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic and --channel are required")
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

	if len(nsqdDestTCPAddres) == 0 {
		log.Fatalf("--dest-nsqd-tcp-address required")
	}

	switch *mode {
	case "round-robin":
		selectedMode = ModeRoundRobin
	case "hostpool":
		selectedMode = ModeHostPool
	default:
		log.Fatalf("unknown --mode=%v", *mode)
	}

	if *throttleFraction > 1.0 || *throttleFraction < 0.0 {
		log.Fatalf("ERROR: --throttle-fraction must be between 0.0 and 1.0")
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

	for i := 0; i < *numPublishers; i++ {
		handler := &PublishHandler{
			Publisher: publisher,
			addresses: addresses,
			mode:      selectedMode,
			reqs:      make(Durations, 0, *statusEvery),
			id:        i,
			hostPool:  hostpool.New(addresses),
		}
		r.AddHandler(handler)
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
