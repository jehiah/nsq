// This is a client that writes out to a file, and optionally rolls the file

package main

import (
	"../../nsq"
	"../../util"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
	"snappy"
	"hash/crc32"
)

var (
	filenamePattern  = "%s.%s.%d-%02d-%02d_%02d.log" // topic.host.YYY-MM-DD_HH.log
	outputDir        = flag.String("output-dir", "/tmp", "directory to write output files to")
	topic            = flag.String("topic-name", "", "nsq topic")
	channel          = flag.String("channel-name", "nsq_to_file", "nsq channel")
	snappyCompress   = flag.Bool("snappy-compress", true, "snappy compres files (will append .snz to filename)")
	buffer           = flag.Int("buffer", 1000, "number of messages to buffer in channel and disk before sync/ack")
	nsqAddresses     = util.StringArray{}
	lookupdAddresses = util.StringArray{}
)

var MAX_BLOCK_SIZE := 65536

func init() {
	flag.Var(&nsqAddresses, "nsq-address", "nsq address (may be given multiple times)")
	flag.Var(&lookupdAddresses, "lookupd-address", "lookupd address (may be given multiple times)")
}

type FileLogger struct {
	out      *os.File
	filename string
	logChan  chan *Message
}

type Message struct {
	*nsq.Message
	returnChannel chan *nsq.FinishedMessage
}

type SyncMsg struct {
	m             *nsq.FinishedMessage
	returnChannel chan *nsq.FinishedMessage
}

func (l *FileLogger) HandleMessage(m *nsq.Message, responseChannel chan *nsq.FinishedMessage) {
	l.logChan <- &Message{m, responseChannel}
}

func flushSnappyData(f io.Writer, b bytes.Buffer, c bytes.Buffer) {
	// the framing is documented https://github.com/kubo/snzip#snz-file-format
	compressedBytes = snappy.Encode(compressedBytes, uncompressedBytes.Bytes()))
	crc := crc32.ChecksumIEEE(uncompressedBytes.Bytes())
	header = make([]byte, 10)
	count := binary.PutUvarint(header, uint64(len(compressedBytes)))
	f.Write(header[:count])
	f.Write()
	f.Write(compressedBytes)
	uncompressedBytes.Reset()
}

func (f *FileLogger) WriteLoop() {
	var pos = 0
	var output = make([]*SyncMsg, *buffer)
	var compressedBytes = make([]byte, 0)
	var uncompressedBuffer = new(bytes.Buffer)
	
	var sync = false
	var ticker = time.Tick(time.Duration(30) * time.Second)
	for {
		select {
		case <-ticker:
			if pos != 0 || f.out != nil {
				updateFile(f)
				sync = true
			}
		case m := <-f.logChan:
			if updateFile(f) {
				sync = true
			}
			if *snappyCompress {
				uncompressedBuffer.Write(m.Body)
				uncompressedBuffer.WriteString("\n")
				if uncompressedBytes.Len() > MAX_BLOCK_SIZE - 2048{
					sync = true
				}
			} else {
				f.out.Write(m.Body)
				f.out.WriteString("\n")
			}
			x := &nsq.FinishedMessage{m.Id, 0, true}
			output[pos] = &SyncMsg{x, m.returnChannel}
			pos++
		}

		if sync || pos >= *buffer {
			if pos > 0 {
				log.Printf("syncing %d records to disk", pos)
				if *snappyCompress {
					compressedBytes = snappy.Encode(compressedBytes, uncompressedBytes.Bytes()))
					f.out.Write(binary.PutUvarint(dst, uint64(len(compressedBytes))))
					f.out.Write(compressedBytes)
					uncompressedBytes.Reset()
				}
				f.out.Sync()
				for pos > 0 {
					pos--
					m := output[pos]
					m.returnChannel <- m.m
					output[pos] = nil
				}
			}
			sync = false
		}
	}
}

func main() {
	flag.Parse()

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic-name and --channel-name are required")
	}

	if *buffer < 0 {
		log.Fatalf("--buffer must be > 0")
	}

	if len(nsqAddresses) == 0 && len(lookupdAddresses) == 0 {
		log.Fatalf("--nsq-address or --lookupd-address required.")
	}
	if len(nsqAddresses) != 0 && len(lookupdAddresses) != 0 {
		log.Fatalf("use --nsq-address or --lookupd-address not both")
	}

	f := &FileLogger{
		logChan: make(chan *Message, *buffer),
	}

	r, _ := nsq.NewReader(*topic, *channel)
	r.BufferSize = *buffer

	r.AddAsyncHandler(f)
	go f.WriteLoop()

	for _, addrString := range nsqAddresses {
		addr, _ := net.ResolveTCPAddr("tcp", addrString)
		err := r.ConnectToNSQ(addr)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	for _, addrString := range lookupdAddresses {
		log.Printf("lookupd addr %s", addrString)
		addr, _ := net.ResolveTCPAddr("tcp", addrString)
		err := r.ConnectToLookupd(addr)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	<-r.ExitChan

}

func updateFile(f *FileLogger) bool {
	t := time.Now()

	hostname, _ := os.Hostname()
	shortHostname := strings.Split(hostname, ".")
	filename := fmt.Sprintf(filenamePattern, *topic, shortHostname[0], t.Year(), t.Month(), t.Day(), t.Hour())
	if filename != f.filename || f.out == nil {
		log.Printf("old %s new %s", f.filename, filename)
		// roll it
		if f.out != nil {
			if *snappyCompress {
				// close snappy file with a zero lenth frame
				d := binary.PutUvarint(dst, uint64(0))
				f.out.Write(d)
			}
			f.out.Close()
		}
		os.MkdirAll(*outputDir, 777)

		
		if *snappyCompress && !filename.HasSuffix(".snz") {
			// compress files need to be unique
			var tempname string
			for i := 1; i < 60; i += 1 {
				if i == 1 {
					tempname = fmt.Sprintf("%s/%s", *outputDir, filename)
				} else {
					tempname = fmt.Sprintf("%s/%s-%d", *outputDir, filename, i)
				}
				if !tempname.HasSuffix(".snz") {
					tempname = fmt.Sprintf("%s.snz", tempname)
				}
				newfile, err := os.OpenFile(fmt.Sprintf("%s/%s", *outputDir, filename), os.O_WRONLY|os.O_CREATE, 0666)
				if err && os.IsExist(err) {
					continue
				} else if err {
					log.Fatal("unable to open file", tempname, err)
				}
				break
			}
			snappyHeader :=  []byte{'S', 'N', 'Z', 'l', '\x01'} // https://github.com/kubo/snzip#snz-file-format
			newfile.Write(snappyHeader)
		} else {
			log.Printf("opening %s/%s", *outputDir, filename)
			newfile, err := os.OpenFile(fmt.Sprintf("%s/%s", *outputDir, filename), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		}
		f.out = newfile
		if err != nil {
			log.Fatal(err)
		}
		f.filename = filename
		return true
	}
	return false
}
