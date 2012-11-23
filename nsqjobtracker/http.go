package main

import (
	"../util"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
)

func httpServer(listener net.Listener) {
	log.Printf("HTTP: listening on %s", listener.Addr().String())

	handler := http.NewServeMux()
	handler.HandleFunc("/ping", pingHandler)
	handler.HandleFunc("/new_job", newJobHandler)
	handler.HandleFunc("/stop_job", stopJobHandler)
	handler.HandleFunc("/jobs", jobsHandler)

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

func jobsHandler(w http.ResponseWriter, req *http.Request) {
	data := make(map[string]interface{})
	data["jobs"] = nil
	util.ApiResponse(w, 200, "OK", data)
}

func newJobHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	_, err = reqParams.Get("workers")
	if err != nil {
		util.ApiResponse(w, 500, "MISSING_ARG_WORKERS", nil)
		return
	}

	data := make(map[string]interface{})
	data["job"] = nil
	util.ApiResponse(w, 200, "OK", data)
}

func stopJobHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	_, err = reqParams.Get("id")
	if err != nil {
		util.ApiResponse(w, 500, "MISSING_ARG_ID", nil)
		return
	}

	data := make(map[string]interface{})
	util.ApiResponse(w, 200, "OK", data)
}
