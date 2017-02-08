package stats

import (
	"encoding/json"
	"sync"
)

const (
	Counter byte = 'c'
	Gauge        = 'g'
)

type Stat struct {
	Name   string `json:"name"`
	Kind   byte   `json:"-"`
	IntVal int    `json:"val"`
}

var (
	stats = make(map[string]*Stat)
	Stats = make(chan *Stat, 1<<12)
	lock  = new(sync.Mutex)
)

func init() {
	go listen()
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

func Reset() {
	lock.Lock()
	stats = make(map[string]*Stat)
	lock.Unlock()
}
