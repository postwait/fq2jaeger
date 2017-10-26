package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/postwait/gofq"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"time"
)

var pprofport = flag.Int("pprof", 0, "pprof port")
var fqhost = flag.String("fq-host", "localhost", "Fq Host")
var fqport = flag.Int("fq-port", 8765, "Fq Port")
var fquser = flag.String("fq-user", "guest", "Fq User")
var fqpass = flag.String("fq-pass", "guest", "Fq Password")
var fqexchange = flag.String("fq-exchange", "logging", "Fq Exchange")
var fqprogram = flag.String("fq-program", "prefix:\"zipkin.thrift.\"", "Fq Program")
var ziphost = flag.String("jaeger-zipkin", "http://localhost:9411", "Jaeger Zipkin Collector URI Base")

var be = binary.BigEndian

type jpayload struct {
	count   uint32
	max     uint32
	payload []byte
}

func NewPayload(max uint32) jpayload {
	return jpayload{count: 0, max: max, payload: []byte{12, 0, 0, 0, 0}}
}
func (p *jpayload) append(buf []byte) {
	if p.count > p.max {
		p.send()
	}
	p.count++
	be.PutUint32(p.payload[1:], p.count)
	p.payload = append(p.payload[:], buf[:]...)
}
func (p *jpayload) reset() {
	p.count = 0
	p.payload = p.payload[0:5]
	be.PutUint32(p.payload[1:], p.count)
}
func (p *jpayload) send() {
	if p.count == 0 {
		return
	}
	start := time.Now()
	resp, err := http.Post(*ziphost+"/api/v1/spans", "application/x-thrift", bytes.NewReader(p.payload))
	if err != nil {
		fmt.Fprintf(os.Stderr, "post error: %v\n", err)
	} else if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, err2 := ioutil.ReadAll(resp.Body)
		if err2 != nil {
			fmt.Fprintf(os.Stderr, "post error[%v] %v\n", resp.StatusCode, err2)
		} else {
			fmt.Fprintf(os.Stderr, "post error[%v] %s\n", resp.StatusCode, string(bodyBytes))
		}
	} else {
		fmt.Fprintf(os.Stderr, "Submitting %d traces [%v]\n", p.count, time.Now().Sub(start))
	}
	p.reset()
}

func consume(c chan []byte) {
	timer := time.NewTicker(time.Millisecond * 250)
	buf := NewPayload(200)
	for {
		select {
		case trace := <-c:
			buf.append(trace)
		case <-timer.C:
			buf.send()
		}
	}
}
func main() {
	flag.Parse()

	if *pprofport > 0 {
		go func() {
			log.Println(http.ListenAndServe("localhost:"+strconv.Itoa(*pprofport), nil))
		}()
	}
	fmt.Printf("fq2jaeger\n")
	hooks := fq.NewTSHooks()
	hooks.AddBinding(*fqexchange, *fqprogram)
	fqc := fq.NewClient()
	fqc.SetHooks(&hooks)
	fqc.Creds(*fqhost, uint16(*fqport), *fquser, *fqpass)
	fqc.Connect()
	traces := make(chan []byte, 10000)
	go consume(traces)
	for {
		select {
		case msg := <-hooks.MsgsC:
			traces <- msg.Payload
		case err := <-hooks.ErrorsC:
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		}
	}
}
