package stats

import (
	"encoding/json"
	"log"
	"os"
	"sync"
)

type StatKind int

const (
	Counter StatKind = iota
	Gauge
)

type Stat struct {
	Name   string
	Kind   StatKind
	IntVal int
}

var (
	stats = make(map[string]*Stat)
	Stats = make(chan *Stat, 1<<12)
	lock  = new(sync.Mutex)
	DEBUG = log.New(os.Stderr, "[DEBUG] ", log.Lshortfile)
)

func init() {
	go listen()
	//DEBUG.Println("started stats listener")
}

func listen() {
	for s := range Stats {
		lock.Lock()
		if _, ok := stats[s.Name]; !ok {
			stats[s.Name] = s
		} else {
			switch s.Kind {
			case Counter:
				stats[s.Name].IntVal += s.IntVal
			case Gauge:
				stats[s.Name] = s
			}
		}
		lock.Unlock()
	}
}

func Json() []byte {
	lock.Lock()
	j, _ := json.Marshal(stats)
	lock.Unlock()
	return j
}
