package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Manifest struct {
	topic   string
	file    string
	entries []*ManifestEntry
}

type ManifestEntry struct {
	fileName    string
	recordCount int64
	timePeriod  time.Time
}

// GzipDiskQueue implements the BackendQueue interface
// providing an interface to externally provided topic archives
type GzipDiskQueue struct {
	sync.RWMutex

	// instatiation time metadata
	name        string
	dataPath    string
	archivePath string
	topic       string
	syncEvery   int64 // number of writes per sync
	exitFlag    int32

	// run-time state (also persisted to disk)
	readPos int64
	depth   int64

	startRange  time.Time
	endRange    time.Time
	files       []*GzipFile
	currentFile *GzipFile

	// exposed via ReadChan()
	readChan chan []byte

	// internal channels
	emptyChan         chan int
	emptyResponseChan chan error
	exitChan          chan int
	exitSyncChan      chan int
}

type GzipFile struct {
	*ManifestEntry
	file       *os.File
	rawReader  *bufio.Reader
	gzipReader *gzip.Reader
	position   int64
}

func (g *GzipFile) Open() error {
	f, err := os.OpenFile(g.fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	g.file = f
	g.gzipReader, err = gzip.NewReader(f)
	if err != nil {
		g.file.Close()
		return err
	}
	g.rawReader = bufio.NewReaderSize(g.gzipReader, 1024*1024*5)
	// TODO if position is set, skip ahead to that position

	return nil
}

func (g *GzipFile) Close() error {
	if g.file != nil {
		err := g.gzipReader.Close()
		if err != nil {
			return err
		}
		g.file.Close()
		g.file = nil
	}
	return nil
}

// reads a manifest file of the name topic.MANIFEST
// where each line is a `timestamp + \t + record_count + \t + filename +\n`
// any files referenced in the manifest that don't exist will just be skipped
// filenames are relative to the path of the manifest file
func OpenManifestFile(p string, topic string) (*Manifest, error) {
	manifestFile := path.Join(p, fmt.Sprintf("%s.MANIFEST", topic))
	f, err := os.OpenFile(manifestFile, os.O_RDONLY, 0600)
	defer f.Close()
	if err != nil {
		return nil, err
	}
	m := &Manifest{topic: topic, file: manifestFile}

	reader := bufio.NewReader(f)
	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			// if ! EOF log
			break
		}
		chunks := strings.SplitN(string(line), "\t", 3)
		if len(chunks) != 3 {
			log.Printf("MANIFEST(%s) invalid record %s (%d chunks)", manifestFile, line, len(chunks))
			continue
		}

		ts, err := strconv.Atoi(chunks[0])
		if err != nil {
			log.Printf("MANIFEST(%s) invalid record %s. (%s not timestamp) %s", manifestFile, line, chunks[0], err.Error())
			continue
		}

		records, err := strconv.Atoi(chunks[1])
		if err != nil {
			log.Printf("MANIFEST(%s): invalid record %s. (%s not integer) %s", manifestFile, line, chunks[1], err.Error())
			continue
		}

		filePath := path.Join(p, chunks[2])

		f, err = os.OpenFile(filePath, os.O_RDONLY, 0600)
		if err != nil {
			log.Printf("MANIFEST(%s): unable to open %s. skipping", manifestFile, filePath)
			continue
		}
		f.Close()
		e := &ManifestEntry{
			fileName:    filePath,
			recordCount: int64(records),
			timePeriod:  time.Unix(int64(ts), 0),
		}
		m.entries = append(m.entries, e)
	}
	return m, nil
}

// ...-YYYYMMDD-YYYYMMDD
var channelTimeFrameFormat = regexp.MustCompile(`^[-\.a-zA-Z0-9_]+-([0-9]{8})-([0-9]{8}|now)$`)

func IsGzipDiskChannel(topic string, archivePath, channel string) bool {
	// if it's a valid channel name, and a topic.MANIFEST file exists
	if !channelTimeFrameFormat.MatchString(channel) {
		return false
	}
	manifestFile := path.Join(archivePath, fmt.Sprintf("%s.MANIFEST", topic))
	f, err := os.OpenFile(manifestFile, os.O_RDONLY, 0600)
	defer f.Close()
	if err != nil {
		return true
	}
	return false
}

// NewDiskQueue instantiates a new instance of DiskQueue, retrieving metadata
// from the filesystem and starting the read ahead goroutine
// this expects a topic.MANIFEST file in archivePath with records of `filename + : + len(records)\n`
// or an archived metadata state
func NewGzipDiskQueue(topic string, channel string, dataPath string, archivePath string, syncEvery int64) BackendQueue {
	d := GzipDiskQueue{
		topic:             topic,
		name:              channel,
		dataPath:          dataPath,
		archivePath:       archivePath,
		readChan:          make(chan []byte),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
		syncEvery:         syncEvery,
	}
	err := d.parseStartEndTime(channel)
	if err != nil {
		log.Fatalf("invalid channel %s - %s", channel, err.Error())
	}
	// no need to lock here, nothing else could possibly be touching this instance
	err = d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		log.Printf("ERROR: diskqueue(%s) failed to retrieveMetaData - %s", d.name, err.Error())
	} else if err != nil {
		manifest, err := OpenManifestFile(archivePath, topic)
		if err != nil {
			log.Printf("ERROR: DISKQUEUE(%s) failed opening manifest file %s", d.name, err.Error())
		}
		for _, e := range manifest.entries {
			if (e.timePeriod.After(d.startRange) && e.timePeriod.Before(d.endRange)) || e.timePeriod.Equal(d.startRange) {
				d.files = append(d.files, &GzipFile{ManifestEntry: e})
				d.depth += e.recordCount
			}
		}
	}
	log.Printf("DISKQUEUE(%s) found %d data files %d messages", d.name, len(d.files), d.depth)
	go d.ioLoop()
	return &d
}

func (d *GzipDiskQueue) parseStartEndTime(channel string) error {
	matches := channelTimeFrameFormat.FindStringSubmatch(channel)
	var start time.Time
	var end time.Time
	if len(matches) != 3 {
		return errors.New("invalid start/stop timeframe in channel")
	}
	start, err := time.Parse("20060102", matches[1])
	if err != nil {
		return err
	}
	if matches[2] == "now" {
		end = time.Now()
	} else {
		end, err = time.Parse("20060102", matches[2])
		if err != nil {
			return err
		}
	}
	d.startRange = start
	d.endRange = end
	return nil
}

// Depth returns the depth of the queue
func (d *GzipDiskQueue) Depth() int64 {
	return atomic.LoadInt64(&d.depth)
}

// ReadChan returns the []byte channel for reading data
func (d *GzipDiskQueue) ReadChan() chan []byte {
	return d.readChan
}

// Put writes a []byte to the queue
func (d *GzipDiskQueue) Put(data []byte) error {
	return errors.New("not implemented for gzipdiskqueue")
}

// Close cleans up the queue and persists metadata
func (d *GzipDiskQueue) Close() error {
	d.Lock()
	defer d.Unlock()

	log.Printf("DISKQUEUE(%s): closing", d.name)

	d.exitFlag = 1

	close(d.exitChan)
	// ensure that ioLoop has exited
	<-d.exitSyncChan

	if d.currentFile != nil {
		d.currentFile.Close()
		d.currentFile = nil
	}
	return d.sync()
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *GzipDiskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.emptyChan <- 1
	return <-d.emptyResponseChan
}

func (d *GzipDiskQueue) doEmpty() error {
	log.Printf("DISKQUEUE(%s): emptying", d.name)

	// close files
	if d.currentFile != nil {
		d.currentFile.Close()
		d.currentFile = nil
	}

	d.files = nil
	atomic.StoreInt64(&d.depth, 0)

	err := d.sync()
	if err != nil {
		log.Printf("ERROR: diskqueue(%s) failed to sync - %s", d.name, err.Error())
		return err
	}

	return nil
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (d *GzipDiskQueue) readOne() ([]byte, error) {
	var err error

	if d.currentFile == nil {
		if len(d.files) == 0 {
			return nil, errors.New("no files to read")
		}
		d.currentFile = d.files[0]
		d.files = d.files[1:]
		err := d.currentFile.Open()
		if err != nil {
			log.Printf("DISKQUEUE(%s): failed opening %s", d.name, d.currentFile.fileName)
			return nil, err
		}
		log.Printf("DISKQUEUE(%s): readOne() opened %s", d.name, d.currentFile.fileName)
	}

	line, _, err := d.currentFile.rawReader.ReadLine()
	if err != nil {
		d.currentFile.Close()
		d.currentFile = nil
		return nil, err
	}

	readBuf := make([]byte, len(line))
	copy(readBuf, line)
	return readBuf, nil
}

// sync fsyncs the current writeFile and persists metadata
func (d *GzipDiskQueue) sync() error {
	return d.persistMetaData()
}

// retrieveMetaData initializes state from the filesystem
func (d *GzipDiskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	// TODO
	// _, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
	// 	&d.depth,
	// 	&d.readFileNum, &d.readPos,
	// 	&d.writeFileNum, &d.writePos)
	// if err != nil {
	// 	return err
	// }
	// d.nextReadFileNum = d.readFileNum
	// d.nextReadPos = d.readPos

	return nil
}

// persistMetaData atomically writes state to the filesystem
// TODO
func (d *GzipDiskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fileName + ".tmp"

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	// TODO
	// _, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
	// 	atomic.LoadInt64(&d.depth),
	// 	d.readFileNum, d.readPos,
	// 	d.writeFileNum, d.writePos)
	// if err != nil {
	// 	f.Close()
	// 	return err
	// }
	f.Sync()
	f.Close()

	// atomically rename
	return os.Rename(tmpFileName, fileName)
}

func (d *GzipDiskQueue) metaDataFileName() string {
	return path.Join(d.dataPath, fmt.Sprintf("%s.diskqueue.meta.dat", d.name))
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
func (d *GzipDiskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var count int64
	var r chan []byte

	for {
		count++
		// dont sync all the time :)
		if count == d.syncEvery {
			err := d.sync()
			if err != nil {
				log.Printf("ERROR: diskqueue(%s) failed to sync - %s", d.name, err.Error())
			}
			count = 0
		}

		if len(d.files) != 0 || d.currentFile != nil {
			dataRead, err = d.readOne()
			if err != nil {
				if d.currentFile != nil {
					log.Printf("ERROR: reading from diskqueue(%s) at %d of %s - %s",
						d.name, d.currentFile.position, d.currentFile.fileName, err.Error())
				}
				// TODO: we assume that all read errors are recoverable...
				// it will probably turn out that this is a terrible assumption
				// as this could certainly result in an infinite busy loop
				runtime.Gosched()
				continue
			}
			r = d.readChan
		} else {
			r = nil
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write) 
		// in a select are skipped, we set r to d.readChan only when there is data to read
		// and reset it to nil after writing to the channel
		case r <- dataRead:
			log.Printf("returned data from readOne() - %v. depth: %d", dataRead, d.Depth())
			atomic.AddInt64(&d.currentFile.position, 1)
			atomic.AddInt64(&d.depth, -1)
		case <-d.emptyChan:
			d.emptyResponseChan <- d.doEmpty()
		case <-d.exitChan:
			goto exit
		}
	}

exit:
	log.Printf("DISKQUEUE(%s): closing ... ioLoop", d.name)
	d.exitSyncChan <- 1
}
