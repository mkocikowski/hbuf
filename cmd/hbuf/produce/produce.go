package produce

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/mkocikowski/hbuf/log"
)

var (
	client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 256,
		},
		Timeout: 5 * time.Second,
	}
	wg = sync.WaitGroup{}
	//done = make(chan bool)
	data = make(chan []byte)
)

func startProducer(url, contentType string) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for b := range data {
			resp, err := client.Post(url, contentType, bytes.NewBuffer(b))
			if err != nil {
				log.ERROR.Fatalln(url, err)
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.ERROR.Fatalln(url, err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.ERROR.Fatalln(url, string(body))
			}
		}
		log.INFO.Println("exiting...")
	}()
}

func Run(url, contentType string) {
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		// Block until a signal is received.
		<-c
		log.INFO.Println("CTRL-C")
		close(data)
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
				log.ERROR.Printf("error reading input: %v", err)
				close(data)
				return
			}
			data <- b[:len(b)-1] // strip trailing newline
		}
	}()
	log.INFO.Println("running.")
	wg.Wait()
	log.INFO.Println("exit.")
}
