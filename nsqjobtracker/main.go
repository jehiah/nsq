package main

import (
	"../util"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
)

var (
	showVersion      = flag.Bool("version", false, "print version string")
	httpAddress      = flag.String("http-address", "0.0.0.0:4172", "<addr>:<port> to listen on for HTTP clients")
	dataPath         = flag.String("data-path", "", "path to store disk-backed messages")
	lookupdHTTPAddrs = util.StringArray{}
	nsqdHTTPAddrs    = util.StringArray{}
	nsqdTCPAddrs     = util.StringArray{}
)

func init() {
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Var(&nsqdHTTPAddrs, "nsqd-http-address", "nsqd HTTP address (may be given multiple times)")
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
}

func main() {
	var waitGroup util.WaitGroupWrapper

	flag.Parse()

	log.Printf("nsqjobtracker v%s", util.BINARY_VERSION)
	if *showVersion {
		return
	}

	if len(nsqdHTTPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatalf("--nsqd-http-address or --lookupd-http-address required.")
	}

	if len(nsqdHTTPAddrs) != 0 && len(lookupdHTTPAddrs) != 0 {
		log.Fatalf("use --nsqd-http-address or --lookupd-http-address not both")
	}
	if len(nsqdHTTPAddrs) != len(nsqdTCPAddrs) {
		log.Fatalf("use the same number of entries for --nsqd-http-address and --nsqd-tcp-address")
	}

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)

	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, os.Interrupt)

	httpAddr, err := net.ResolveTCPAddr("tcp", *httpAddress)
	if err != nil {
		log.Fatal(err)
	}
	jt := NewJobTracker(*dataPath)
	jt.nsqdHTTPAddresses = nsqdHTTPAddrs
	jt.lookupdHTTPAddresses = lookupdHTTPAddrs
	httpListener, err := net.Listen("tcp", httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", httpAddr, err.Error())
	}
	waitGroup.Wrap(func() { jt.httpServer(httpListener) })

	<-exitChan

	httpListener.Close()
	waitGroup.Wait()
	jt.Sync()
}
