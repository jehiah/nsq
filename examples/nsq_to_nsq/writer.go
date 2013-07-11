package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"github.com/bitly/nsq/nsq"
	"time"
)

type Writer struct {
	net.Conn
	WriteTimeout      time.Duration
	stopFlag          int32
	ShortIdentifier   string
	LongIdentifier    string
	exitChan          chan int
	writeChan		  chan *publishMessage
	transactions      []*publishMessage
}

type publishMessage struct {
	topic 	string
	msg *nsq.Message
	fin chan *nsq.FinishedMessage
}

func NewWriter(heartbeatInterval int) *Writer {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err.Error())
	}
	w := &Writer{
		transactionChan:   make(chan *writerTransaction),
		exitChan:          make(chan int),
		WriteTimeout:      time.Second,
		ShortIdentifier:   strings.Split(hostname, ".")[0],
		LongIdentifier:    hostname,
			writeChan: make(chan *publishMessage),
				transactions: []*publishMessage,
	}
	return w
}

func (this *Writer) Stop() {
	if !atomic.CompareAndSwapInt32(&this.stopFlag, 0, 1) {
		return
	}
	this.close()
}

func (w *Writer) Publish(topic string, msg *nsq.Message, fin chan *nsq.FinishedMessage) {
	select {
	case w.writeChan <- &publishMsg{topic, msg, fin}:
		return
	case <- w.exitChan:
		fin <- &FinishedMessage{msg.Id, 90, false}
	}
}

func (w *Writer) ConnectToNSQ(addr string) error {
	var err error
	w.Conn, err = net.DialTimeout("tcp", addr, 5 * time.Second)
	if err != nil {
		return errors.New("failed to connect nsq")
	}
	w.SetWriteDeadline(time.Now().Add(this.WriteTimeout))
	if _, err := w.Conn.Write(nsq.MagicV2); err != nil {
		this.close()
		return err
	}
	
	ci := make(map[string]interface{})
	ci["short_id"] = w.ShortIdentifier
	ci["long_id"] = w.LongIdentifier
	// ci["feature_negotiation"] = true
	cmd, err := nsq.Identify(ci)
	if err != nil {
		w.close()
		return fmt.Errorf("[%s] failed to create identify command - %s", w.RemoteAddr(), err.Error())
	}
	
	
	// send Identify
	if err = cmd.Write(w.Conn); err != nil {
		w.close()
		return err
	}

	resp, err := nsq.ReadResponse(w.Conn)
	if err != nil {
		w.close()
		return err
	}
	_, data, _ := nsq.UnpackResponse(resp)
	if !bytes.Equal(data, []byte("OK")) {
		w.close()
		return errors.New("unable to establish connection")
	}	

	go w.readLoop()
	go w.messageRouter()
	return nil
}

func (this *Writer) close() {
	if atomic.CompareAndSwapInt32(&this.state, StateConnected, StateDisconnected) {
		close(this.exitChan)
		this.Conn.Close()
	}
}

func (w *Writer) messageRouter() {
	var err error
	var t *publishMessage
	defer w.transactionCleanup()
	for {
		select {
		case t := <-w.writeChan:
			w.transactions = append(w.transactions, t)
			w.SetWriteDeadline(time.Now().Add(this.WriteTimeout))
			cmd := nsq.Publish(t.topic, t.msg.Body)
			if err = cmd.Write(w.Conn); err != nil {
				log.Printf("[%s] error writing %s", this.RemoteAddr(), err.Error())
				w.close()
				return
			}
		case buf := <-w.dataChan:
			frameType, data, err := nsq.UnpackResponse(buf)
			if err != nil {
				log.Printf("[%s] error (%s) unpacking response %d %s", w.RemoteAddr(), err.Error(), frameType, data)
				return
			}
			if frameType == nsq.FrameTypeResponse &&
				bytes.Equal(data, []byte("_heartbeat_")) {
				log.Printf("[%s] received heartbeat", w.RemoteAddr())
				if err := w.heartbeat(); err != nil {
					log.Printf("[%s] error sending heartbeat - %s", w.RemoteAddr(), err.Error())
					w.close()
					return
				}
			} else {
				t, w.transactions = w.transactions[0], w.transactions[1:]
				t.fin <- &nsq.FinishedMessage{t.msg.Id, 0, true}
			}
		case <-this.exitChan:
			return
		}
	}
}

//send heartbeat
func (this *Writer) heartbeat() error {
	this.SetWriteDeadline(time.Now().Add(this.WriteTimeout))
	if err := Nop().Write(this.Conn); err != nil {
		return err
	}
	return nil
}

// cleanup transactions
func (w *Writer) transactionCleanup() {
	for _, t := range w.transactions {
		t.fin <- &nsq.FinishedMessage{t.msg.Id, 90, false}
	}
	w.transactions = w.transactions[:0]
}

func (w *Writer) readLoop() {
	rbuf := bufio.NewReader(w.Conn)
	for {
		resp, err := nsq.ReadResponse(rbuf)
		if err != nil {
			log.Printf("[%s] error reading response %s", w.RemoteAddr(), err.Error())
			if !strings.Contains(err.Error(), "use of closed network connection") {
				w.close()
			}
			break
		}
		select {
		case w.dataChan <- resp:
		case <-w.exitChan:
			return
		}
	}
}
