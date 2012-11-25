package main

import (
	"../nsq"
	"../util"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"
)

func (jt *JobTracker) httpServer(listener net.Listener) {
	log.Printf("HTTP: listening on %s", listener.Addr().String())

	handler := http.NewServeMux()
	handler.HandleFunc("/ping", pingHandler)
	handler.Handle("/job/", jt)

	server := &http.Server{
		Handler: handler,
	}
	err := server.Serve(listener)
	// theres no direct way to detect this error because it is not exposed
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		log.Printf("ERROR: http.Serve() - %s", err.Error())
	}

	log.Printf("HTTP: closing %s", listener.Addr().String())
}

func pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func (jt *JobTracker) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var urlRegex = regexp.MustCompile("^/job/(.*)$")
	matches := urlRegex.FindStringSubmatch(req.URL.Path)
	if len(matches) == 0 {
		util.ApiResponse(w, 500, "INVALID_JOB_ID", nil)
		return
	}
	parts := strings.Split(matches[1], "/")
	jobId := parts[0]

	reqParams, err := util.NewReqParams(req)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	if jobId == "new" {
		jt.newJob(w, req, reqParams)
		return
	}

	// lookup the job
	job, ok := jt.Jobs[jobId]
	if !ok {
		util.ApiResponse(w, 404, "JOB_NOT_FOUND", nil)
		return
	}
	data := make(map[string]interface{})
	data["job"] = job
	// these make it easy for workers to bootstrap their information
	data["lookupd_http_addresses"] = lookupdHTTPAddrs
	data["nsqd_tcp_addresses"] = nsqdTCPAddrs
	data["nsqd_http_addresses"] = nsqdHTTPAddrs
	util.ApiResponse(w, 200, "OK", data)
}

func (jt *JobTracker) newJob(w http.ResponseWriter, req *http.Request, args *util.ReqParams) {
	var err error
	var topics []string
	var name string
	var jobId string
	var job *Job
	var workers []interface{}
	var timeframe string
	var timeframePattern = regexp.MustCompile("^[0-9]{8}-([0-9]{8}|now)$")
	data := make(map[string]interface{})

	workerCount, err := args.GetInt("workers")
	if err != nil {
		goto errorResponse
	}
	topics, err = args.GetAll("topic")
	if err != nil {
		goto errorResponse
	}
	for _, t := range topics {
		if !nsq.IsValidTopicName(t) {
			err = errors.New("Invalid Topic")
			goto errorResponse
		}
	}
	timeframe, err = args.Get("timeframe")
	if err != nil || !timeframePattern.MatchString(timeframe) {
		err = errors.New("INVALID_ARG_TIMEFRAME")
		goto errorResponse
	}

	// max name size = 48 - prefix(8) - date(18)
	name, err = args.Get("name")
	if err != nil || !nsq.IsValidChannelName(name) || len(name) >= 22 {
		err = errors.New("INVALID_ARG_NAME")
		goto errorResponse
	}

	jobId = jt.nextJobID()
	job = &Job{
		ID:                jobId,
		Started:           time.Now().Unix(),
		WorkerCount:       workerCount,
		Topics:            topics,
		Timeframe:         timeframe,
		Name:              name,
		NSQPrefix:         fmt.Sprintf("job-%s-%s", jobId, name),
		NsqdHTTPAddresses: jt.nsqdHTTPAddresses, // todo lookup from lookupd first based on topic
	}
	jt.Jobs[jobId] = job
	job.Start()

	for i := 0; i < job.WorkerCount; i += 1 {
		w := make(map[string]interface{})
		w["id"] = i
		w["url"] = fmt.Sprintf("http://%s/job/%s/worker/%d", req.Host, jobId, i)
		workers = append(workers, w)
	}

	data["job"] = job
	data["workers"] = workers
	util.ApiResponse(w, 200, "OK", data)
	return

errorResponse:
	if err == nil {
		err = errors.New("INTERNAL_ERROR")
	}
	log.Printf("error %s", err.Error())
	util.ApiResponse(w, 500, err.Error(), nil)
}
