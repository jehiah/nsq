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
	"net/url"
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

	util.ApiResponse(w, 200, "OK", job)
}

func (jt *JobTracker) newJob(w http.ResponseWriter, req *http.Request, args *util.ReqParams) {
	var err error
	var topics []string
	var name string
	var jobId string
	var job *Job
	var timeframePattern = regexp.MustCompile("^[0-9]{6}-[0-9]{6}$")
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

	name, err = args.Get("name")
	// max name size = 48 - prefix(8) + date(18)
	if err != nil || !nsq.IsValidChannelName(name) || len(name) >= 22 {
		err = errors.New("INVALID_ARG_NAME")
		goto errorResponse
	}

	jobId = jt.nextJobID()
	job = &Job{
		ID:          jobId,
		Started:     time.Now().Unix(),
		WorkerCount: workerCount,
		Topics:      topics,
		Timeframe:   timeframe,
		Name:        name,
		NSQPrefix:   fmt.Sprintf("job-%s-%s", jobId, name),
	}
	jt.Jobs[jobId] = job
	log.Printf("New Job %s", job)

	// TODO subscribe the job to each topic
	// TODO: use lookupd when present
	// TODO: check nsqd's first for topic before creating
	// keep track of the nsqd's to check
	for _, t := range job.Topics {
		channel := fmt.Sprintf("%s-%s", job.NSQPrefix, job.Timeframe)
		for _, addr := range jt.nsqdHTTPAddresses {
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

	util.ApiResponse(w, 200, "OK", job)
	return

errorResponse:
	if err == nil {
		err = errors.New("INTERNAL_ERROR")
	}
	log.Printf("error %s", err.Error())
	util.ApiResponse(w, 500, err.Error(), nil)
}
