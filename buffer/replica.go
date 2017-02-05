package buffer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mkocikowski/hbuf/curl"
	"github.com/mkocikowski/hbuf/log"
	"github.com/mkocikowski/hbuf/message"
)

type Replica struct {
	ID         string `json:"id"`
	URL        string `json:"url"`
	Controller string `json:"-"`
	Len        int    `json:"len"`
	lock       *sync.Mutex
	data       chan *message.Message
	done       chan bool
}

func (r *Replica) Init() error {
	//
	r.lock = new(sync.Mutex)
	r.data = make(chan *message.Message)
	r.done = make(chan bool)
	// TODO: add "health check": make sure the replica is up to date, catch up if needed
	go r.update()
	go r.writer()
	return nil
}

func (r *Replica) Stop() {
	close(r.data)
	close(r.done)
}

func (r *Replica) update() {
	//
	log.DEBUG.Printf("starting updater for replica %q", r.ID)
	for {
		select {
		case <-r.done:
			return
		default:
		}
		if err := r.url(); err != nil {
			log.WARN.Println(err)
			time.Sleep(1 * time.Second)
		} else {
			time.Sleep(5 * time.Second)
		}
	}
}

func (r *Replica) url() error {
	//
	r.lock.Lock()
	c := r.Controller
	r.lock.Unlock()
	b, err := curl.Get(c + "/buffers/" + r.ID)
	if err != nil {
		return fmt.Errorf("error getting replica URL from %q: %v", c, err)
	}
	u := Replica{}
	if err := json.Unmarshal(b, &u); err != nil {
		return fmt.Errorf("error parsing replica URL: %v", err)
	}
	r.lock.Lock()
	if r.URL != u.URL {
		r.URL = u.URL
		log.DEBUG.Printf("set url for replica %q: %v", r.ID, r.URL)
	}
	r.lock.Unlock()
	return nil
}

func (r *Replica) writer() {
	log.DEBUG.Printf("starting writer for replica %q", r.ID)
	for m := range r.data {
		for {
			if m.Error == nil {
				m.Error = make(chan error, 1)
			}
			_, err := curl.Post(r.URL, m.Type, bytes.NewBuffer(m.Body))
			if err == nil {
				r.Len += 1
				close(m.Error)
				break
			}
			log.WARN.Printf("error replicating message to %q / %q : %v", r.ID, r.URL, err)
			time.Sleep(1 * time.Second)
		}
	}
}
