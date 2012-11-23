package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
)

type Job struct {
	ID          string   `json:"id"`
	Started     int64    `json:"started_at"`
	Stopped     int64    `json:"stopped_at,omitempty"`
	WorkerCount int      `json:"worker_count"`
	Name        string   `json:"name"`
	Topics      []string `json:"topics"`
	NSQPrefix   string   `json:"nsq_prefix"`
}

func (j *Job) String() string {
	return fmt.Sprintf("<Job %s(%d) %s>", j.NSQPrefix, j.WorkerCount, j.Topics)
}

type JobTracker struct {
	sync.Mutex
	Jobs     map[string]*Job `json:"jobs"`
	LastID   int             `json:"last_id"`
	fileName string
}

func loadJobTrackerFromFile(f *os.File) (*JobTracker, error) {
	body, err := ioutil.ReadAll(f)
	if err != nil {
		log.Printf("failed loading %s", err.Error())
		return nil, err
	}
	jt := &JobTracker{Jobs: make(map[string]*Job)}
	err = json.Unmarshal(body, jt)
	if err != nil {
		log.Printf("failed loading json %s", err.Error())
		return nil, err
	}
	return jt, nil
}

func NewJobTracker(dataPath string) *JobTracker {
	var jt *JobTracker
	fileName := path.Join(dataPath, "jobs.json")
	log.Printf("opening %s", fileName)
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err == nil {
		defer f.Close()
		jt, err = loadJobTrackerFromFile(f)
		if err != nil {
			log.Fatalf("failed loading from file %s %s", fileName, err.Error())
		}
	} else {
		jt = &JobTracker{Jobs: make(map[string]*Job)}
	}
	jt.fileName = fileName
	return jt
}

func base62(n int) string {
	s := ""
	alphabet := "0123456789abcdefghijklmnopqrstuvABCDEFGHIJKLMNOPQRSTUVWXYZ"
	base := len(alphabet)
	var r int
	for n > 0 {
		r = n % base
		n = n / base
		s = fmt.Sprintf("%s%c", s, alphabet[r])
	}
	return s
}

func (jt *JobTracker) nextJobID() string {
	jt.Lock()
	defer jt.Unlock()
	jt.LastID += 1
	s := base62(jt.LastID)
	return s
}

func (jt *JobTracker) Sync() error {
	tmpFileName := fmt.Sprintf("%s.tmp", jt.fileName)
	f, err := os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	body, err := json.Marshal(jt)
	if err != nil {
		f.Close()
		return err
	}
	f.Write(body)
	err = f.Sync()
	if err != nil {
		f.Close()
		return err
	}
	f.Close()
	return os.Rename(tmpFileName, jt.fileName)
}
