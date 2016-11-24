package node

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/mkocikowski/hbuf/node"
)

var (
	INFO = log.New(os.Stderr, "[INFO] ", 0)
)

func Run() {
	INFO.Println("starting...")
	n := &node.Node{
		URL: "http://localhost:8080",
		Dir: "./data",
	}
	n.Init()
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		// Block until a signal is received.
		<-c
		//log.Println("got signal:", <-c)
		n.Stop()
		os.Exit(0)
	}()
	go func() {
		n.Load()
		//n.AddTenant("-", *join)
		n.AddTenant("-", n.URL)
	}()
	//log.Fatal(http.ListenAndServe(fmt.Sprintf("localhost:%d", *port), n))
	// https://blog.cloudflare.com/the-complete-guide-to-golang-net-http-timeouts/
	srv := &http.Server{
		Addr:           "localhost:8080",
		Handler:        n,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		MaxHeaderBytes: 1 << 12, // 4KB
	}
	log.Fatal(srv.ListenAndServe())
}
