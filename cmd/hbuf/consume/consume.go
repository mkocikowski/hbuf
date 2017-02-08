package consume

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
)

var (
	client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 256,
		},
		Timeout: 5 * time.Second,
	}
	wg   = sync.WaitGroup{}
	done = make(chan bool)
)

func startConsumer(url string) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			resp, err := client.Get(url)
			if err != nil {
				log.Fatalln(err)
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Fatalln(err)
			}
			resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusOK:
				fmt.Println(string(body))
			case http.StatusNoContent:
				time.Sleep(100 * time.Millisecond)
			default:
				log.Fatalln(resp.StatusCode, string(body))
			}
		}
	}()
}

func Run(url string) {
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		// Block until a signal is received.
		<-c
		close(done)
	}()
	startConsumer(url)
	log.Println("running.")
	wg.Wait()
	log.Println("exit.")
}
