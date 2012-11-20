package main

import (
	"compress/gzip"
	"fmt"
	"github.com/bmizerany/assert"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"
	"time"
)

func newFile(filename string, dir string) (*os.File, error) {
	filePath := path.Join(dir, filename)
	os.Remove(filePath)
	writeFile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	return writeFile, nil
}

func writeRecords(w io.Writer, count int) {
	for i := 0; i < count; i += 1 {
		// random string
		w.Write([]byte("message\n"))
	}
}

func TestGzipDiskQueue(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	log.SetOutput(os.Stdout)

	filename := "topic.server.2012-11-19_10.log.gz"
	recordCount := 10

	archiveDir := os.TempDir()
	log.Printf("archiveDir is %s", archiveDir)
	file, err := newFile(filename, archiveDir)
	assert.Equal(t, err, nil)
	gzipWriter := gzip.NewWriter(file)
	writeRecords(gzipWriter, recordCount)
	gzipWriter.Close()
	file.Close()

	file, err = newFile("topic.MANIFEST", archiveDir)
	assert.Equal(t, err, nil)
	file.WriteString(fmt.Sprintf("%s:%d\n", filename, recordCount))
	file.Close()

	channel := "testgz-20121118-20121119"
	dq := NewGzipDiskQueue("topic", channel, os.TempDir(), archiveDir, 2500)
	assert.NotEqual(t, dq, nil)
	assert.Equal(t, dq.Depth(), int64(recordCount))

	msg := []byte("message")
	err = dq.Put(msg)
	assert.NotEqual(t, err, nil)

	for i := 0; i < recordCount; i += 1 {
		msgOut := <-dq.ReadChan()
		assert.Equal(t, msgOut, msg)
	}
	time.Sleep(5 * time.Millisecond)
	assert.Equal(t, dq.Depth(), int64(0))
}
