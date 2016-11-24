package produce

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
)

var (
	INFO   = log.New(os.Stderr, "[INFO] ", 0)
	ERROR  = log.New(os.Stderr, "[ERROR] ", log.Lshortfile)
	client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 256,
		},
		Timeout: 5 * time.Second,
	}
	wg   = sync.WaitGroup{}
	done = make(chan bool)
	data = make(chan []byte)
)

func startProducer(url, contentType string) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for b := range data {
			resp, err := client.Post(url, contentType, bytes.NewBuffer(b))
			if err != nil {
				ERROR.Fatalln(err)
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				ERROR.Fatalln(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				ERROR.Fatalln(resp.StatusCode, string(body))
			}
		}
		INFO.Println("exiting...")
	}()
}

func Run(url, contentType string) {
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		// Block until a signal is received.
		<-c
		close(done)
	}()
	startProducer(url, contentType)
	go func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			b, err := reader.ReadBytes('\n')
			if err == io.EOF {
				//INFO.Println("EOF")
				close(data)
				return
			}
			if err != nil {
				ERROR.Printf("error reading input: %v", err)
				close(data)
				return
			}
			select {
			case data <- b[:len(b)-1]: // strip trailing newline
			case <-done:
				close(data)
				return
			}
		}
	}()
	INFO.Println("running.")
	wg.Wait()
	INFO.Println("exit.")
}
