package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/mkocikowski/hbuf/cmd/hbuf/consume"
	"github.com/mkocikowski/hbuf/cmd/hbuf/node"
	"github.com/mkocikowski/hbuf/cmd/hbuf/produce"
	"github.com/mkocikowski/hbuf/cmd/hbuf/stress"
)

var (
	BuildHash string
	BuildDate string
	Version   string

	info = `Commands:
	node		start hbuf server node
	produce		read from stdin, write to specified topic
	consume		consume from specified topic[s], write to stdout
	stress		run "fake" load against specified cluster

When called with no arguments, starts an hbuf node on localhost:8080; this is
for dev convenience, to run a "real" server see the "node" command.`
)

func main() {

	log.Printf("version: %s build: %s %s", Version, BuildHash, BuildDate)

	if len(os.Args) == 1 {
		node.Run()
		os.Exit(0)
	}

	switch os.Args[1] {
	case "node":
		fs := flag.NewFlagSet("node", flag.ExitOnError)
		fs.Parse(os.Args[2:])
		node.Run()
		os.Exit(0)
	case "produce":
		fs := flag.NewFlagSet("produce", flag.ExitOnError)
		url := fs.String("url", "http://localhost:8080/topics/test", "url of the topic")
		fs.Usage = func() {
			fmt.Fprintln(os.Stderr, "Read messages from stdin, one message per line, write to topic.")
			fs.PrintDefaults()
		}
		ct := fs.String("content-type", "text/plain", "'Content-Type:' of the data")
		fs.Parse(os.Args[2:])
		produce.Run(*url, *ct)
		os.Exit(0)
	case "consume":
		fs := flag.NewFlagSet("consume", flag.ExitOnError)
		url := fs.String("url", "http://localhost:8080/topics/test/next", "url to consume from")
		fs.Usage = func() {
			fmt.Fprintln(os.Stderr, "Consume messages from topic[s], write to stdout.")
			fs.PrintDefaults()
		}
		fs.Parse(os.Args[2:])
		consume.Run(*url)
		os.Exit(0)
	case "stress":
		fs := flag.NewFlagSet("stress", flag.ExitOnError)
		config := fs.String("config", "", "path to the config file; uses default config when config=''")
		example := fs.Bool("example", false, "when set, print default config to stdout and exit")
		duration := fs.Duration("duration", 10*time.Second, "run for this long then exit")
		fs.Usage = func() {
			fmt.Fprintln(os.Stderr, "Run load against configured endpoints.")
			fs.PrintDefaults()
		}
		fs.Parse(os.Args[2:])
		if *example {
			j, _ := json.MarshalIndent(stress.DefaultConf, "", "  ")
			fmt.Fprintf(os.Stdout, "%s\n", string(j))
			os.Exit(1)
		}
		stress.Run(*config, *duration)
	default:
		fmt.Println(info)
		os.Exit(2)
	}

}
