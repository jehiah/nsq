package main

import (
	"../nsq"
	"encoding/json"
	"fmt"
	"github.com/bitly/go-simplejson"
	"time"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"sync"
)

type Job struct {
	ID                string   `json:"id"`
	Started           int64    `json:"started_at"`
	Stopped           int64    `json:"stopped_at,omitempty"`
	WorkerCount       int      `json:"worker_count"`
	Name              string   `json:"name"`
	Topics            []string `json:"topics"`
	Timeframe         string   `json:"timeframe"` // TODO: in theory it'd be nice to have topic dependant timeframess
	NSQPrefix         string   `json:"nsq_prefix"`
	NsqdHTTPAddresses []string `json:"nsqd_http_addresses"` // todo base on lookupd, or record as private here
}

func (j *Job) String() string {
	return fmt.Sprintf("<Job %s(%d) %s>", j.NSQPrefix, j.WorkerCount, j.Topics)
}

type JobTracker struct {
	sync.Mutex           `json:"-"`
	Jobs                 map[string]*Job `json:"jobs"`
	LastID               int             `json:"last_id"`
	nsqdHTTPAddresses    []string        `json:"-"`
	lookupdHTTPAddresses []string        `json:"-"`
	fileName             string          `json:"-"`
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

func (j *Job) Start() {
	log.Printf("New %s", j)
	// TODO: use lookupd when present
	// TODO: check nsqd's first for topic before creating
	for _, t := range j.Topics {
		channel := fmt.Sprintf("%s-%s", j.NSQPrefix, j.Timeframe)
		for _, addr := range j.NsqdHTTPAddresses {
			endpoint := fmt.Sprintf("http://%s/create_channel?topic=%s&channel=%s",
				addr, url.QueryEscape(t), url.QueryEscape(channel))
			log.Printf("NSQD: querying %s", endpoint)
			_, err := nsq.ApiRequest(endpoint)
			if err != nil {
				log.Printf("ERROR: nsqd %s - %s", endpoint, err.Error())
				continue
			}
		}
	}

	// todo: set the nsqaddr's in the job, and .start() or something like that.
	go j.WatchSourceChannelCompletion()

}

func (j *Job) WatchSourceChannelCompletion() {
	// TODO: poll nsqd's (using a waitgroup?)
	// TODO: what do do after this is done?

	log.Printf("JOB(%s): starting poll of nsqd channels for %s", j.NSQPrefix, j.Topics)
	finishedTopics := make(map[string]bool)
	for _, t := range j.Topics {
		finishedTopics[t] = false
	}

	for {
		finishedAll := true
		for t, finished := range finishedTopics {
			if finished {
				continue
			}
			channelName := fmt.Sprintf("%s-%s", j.NSQPrefix, j.Timeframe)
			finished = true
			for _, addr := range j.NsqdHTTPAddresses {
				endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
				log.Printf("NSQD: querying %s", endpoint)
				stats, err := nsq.ApiRequest(endpoint)
				if err != nil {
					log.Printf("ERROR: nsqd %s - %s", endpoint, err.Error())
					continue
				}
				if !isTopicChannelFinished(stats, t, channelName) {
					finished = false
				}
			}
			if finished == false {
				finishedAll = false
			} else {
				log.Printf("JOB(%s) source channel for %s:%s finished", j.NSQPrefix, t, channelName)
				finishedTopics[t] = finished
			}
		}
		if finishedAll {
			break
		}
		time.Sleep(time.Duration(30) * time.Second)
	}
	log.Printf("JOB(%s): finished polling nsqd's", j.NSQPrefix)
	j.Stopped = time.Now().Unix()

}

func isTopicChannelFinished(data *simplejson.Json, selectedTopic string, selectedChannel string) bool {
	topics, _ := data.Get("topics").Array()
	for _, topicInfo := range topics {
		topicInfo := topicInfo.(map[string]interface{})
		topicName := topicInfo["topic_name"].(string)
		if selectedTopic != topicName {
			continue
		}
		channels := topicInfo["channels"].([]interface{})
		for _, c := range channels {
			c := c.(map[string]interface{})
			channelName := c["channel_name"].(string)
			if selectedChannel != channelName {
				continue
			}
			deferredCount := int64(c["deferred_count"].(float64))
			depth := int64(c["depth"].(float64))
			log.Printf("Toipc: %s Channel: %s Depth: %d Deferred: %d", topicName, channelName, depth, deferredCount)
			if deferredCount+depth == 0 {
				return true
			}
		}
	}
	return false
}
