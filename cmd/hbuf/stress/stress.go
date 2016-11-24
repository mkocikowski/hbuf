package stress

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
)

var (
	INFO   = log.New(os.Stderr, "[INFO] ", 0)
	DEBUG  = log.New(os.Stderr, "[DEBUG] ", log.Lshortfile)
	client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 256,
		},
		Timeout: 5 * time.Second,
	}
	wg   = sync.WaitGroup{}
	done = make(chan bool)
	//
	defaultProducerConf = &producerT{
		URL:          "http://localhost:8080/topics/foo",
		MsgSizeB:     1024,
		WriteSleepMs: 10,
	}
	defaultConsumerConf = &consumerT{
		URL:         "http://localhost:8080/topics/foo/next?id=test",
		ReadSleepMs: 10,
	}
	DefaultConf = &confT{
		Producers: []*producerT{defaultProducerConf},
		Consumers: []*consumerT{defaultConsumerConf},
	}
)

type confT struct {
	Producers []*producerT
	Consumers []*consumerT
}

type producerT struct {
	URL          string
	MsgSizeB     int
	WriteSleepMs int
}

type consumerT struct {
	URL         string
	ReadSleepMs int
}

type messageT struct {
	url   string
	body  []byte
	sleep time.Duration
}

func startProducer(p *producerT) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			s := strings.Repeat("x", p.MsgSizeB)
			resp, err := http.Post(p.URL, "text/plain", bytes.NewBufferString(s))
			if err != nil {
				DEBUG.Fatalln(err)
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				DEBUG.Fatalln(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				DEBUG.Fatalln(resp.StatusCode, string(body))
			}
			time.Sleep(time.Duration(p.WriteSleepMs) * time.Millisecond)
		}
	}()
}

func startConsumer(c *consumerT) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			resp, err := client.Get(c.URL)
			if err != nil {
				DEBUG.Fatalln(err)
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				DEBUG.Fatalln(err)
			}
			resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusOK:
			case http.StatusNoContent:
			default:
				DEBUG.Fatalln(resp.StatusCode, string(body))
			}
			time.Sleep(time.Duration(c.ReadSleepMs) * time.Millisecond)
		}
	}()
}

func configure(filename string) (*confT, error) {
	if filename == "" {
		return DefaultConf, nil
	}
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %v", err)
	}
	c := new(confT)
	if err = json.Unmarshal(b, c); err != nil {
		return nil, fmt.Errorf("error parsing hbuf stress config file: %v", err)
	}
	return c, nil
}

func Run(path string) {
	conf, err := configure(path)
	if err != nil {
		DEBUG.Fatalf("error loading config: %v", err)
	}
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		// Block until a signal is received.
		<-c
		close(done)
	}()
	for _, p := range conf.Producers {
		startProducer(p)
	}
	for _, c := range conf.Consumers {
		startConsumer(c)
	}
	INFO.Println("running.")
	wg.Wait()
}