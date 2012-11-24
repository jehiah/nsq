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
		// TODO: random/longer message
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

	os.Remove(path.Join(archiveDir, "topic.MANIFEST"))
	assert.Equal(t, IsGzipDiskTopicChannel("topic", "asdf-channel-20120101-now", archiveDir), false)
	file, err = newFile("topic.MANIFEST", archiveDir)
	assert.Equal(t, err, nil)
	ts, _ := time.Parse("2006-01-02 03", "2012-11-18 10")
	file.WriteString(fmt.Sprintf("%d\t%d\t%s\n", ts.Unix(), recordCount, filename))
	file.Close()
	assert.Equal(t, IsGzipDiskTopicChannel("topic", "asdf-channel-20120101-now", archiveDir), true)
	assert.Equal(t, IsGzipDiskTopicChannel("topic", "job-channel-20120101-20121119", archiveDir), true)

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
