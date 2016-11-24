package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/mkocikowski/hbuf/cmd/hbuf/consume"
	"github.com/mkocikowski/hbuf/cmd/hbuf/node"
	"github.com/mkocikowski/hbuf/cmd/hbuf/produce"
	"github.com/mkocikowski/hbuf/cmd/hbuf/stress"
)

var (
	BuildHash string
	BuildDate string
	Version   = "0.0.1"
	INFO      = log.New(os.Stderr, "[INFO] ", 0)

	info = `Commands:
	node		start hbuf server node
	produce		read from stdin, write to specified topic
	consume		consume from specified topic[s], write to stdout
	stress		run "fake" load against specified cluster

When called with no arguments, starts an hbuf node on localhost:8080; this is
for dev convenience, to run a "real" server see the "node" command.`
)

func main() {

	INFO.Printf("version: %s build: %s %s", Version, BuildHash, BuildDate)

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
		url := fs.String("url", "http://localhost:8080/topics/test/next?id=test", "url of the consumer")
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
		showExample := fs.Bool("example", false, "when set, print default config to stdout and exit")
		fs.Usage = func() {
			fmt.Fprintln(os.Stderr, "Run load against configured endpoints.")
			fs.PrintDefaults()
		}
		fs.Parse(os.Args[2:])
		if *showExample {
			j, _ := json.MarshalIndent(stress.DefaultConf, "", "  ")
			fmt.Fprintf(os.Stdout, "%s\n", string(j))
			os.Exit(1)
		}
		stress.Run(*config)
	default:
		fmt.Println(info)
		os.Exit(2)
	}

}
