package buffer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/mkocikowski/hbuf/curl"
	"github.com/mkocikowski/hbuf/message"
)

type Replica struct {
	ID         string `json:"id"`
	URL        string `json:"url"`
	Controller string `json:"-"`
	Len        int    `json:"len"`
	lock       *sync.Mutex
	wg         sync.WaitGroup
	data       chan *message.Message
	done       chan bool
}

func (r *Replica) Init() error {
	//
	r.lock = new(sync.Mutex)
	r.data = make(chan *message.Message)
	r.done = make(chan bool)
	// TODO: add "health check": make sure the replica is up to date, catch up if needed
	r.wg.Add(1)
	go r.updater()
	r.wg.Add(1)
	go r.writer()
	return nil
}

func (r *Replica) Stop() {
	close(r.data)
	close(r.done)
	// TODO: use wait group to make sure all goroutines exited?
	r.wg.Wait()
	log.Printf("replica %q stopped", r.ID)
}

func (r *Replica) updater() {
	//
	defer r.wg.Done()
	log.Printf("starting updater for replica %q", r.ID)
	for {
		if err := r.update(); err != nil {
			log.Println(err)
		}
		select {
		case <-time.After(1 * time.Second):
		case <-r.done:
			log.Printf("stopped replica %q updater", r.ID)
			return
		}
	}
}

func (r *Replica) update() error {
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
		log.Printf("set url for replica %q: %v", r.ID, r.URL)
	}
	r.lock.Unlock()
	log.Printf("updated url for replica %q", r.ID)
	return nil
}

func (r *Replica) writer() {
	//
	defer r.wg.Done()
	log.Printf("starting writer for replica %q", r.ID)
	// TODO: this logic needs to be changed
	for m := range r.data {
		r.lock.Lock()
		url := r.URL
		r.lock.Unlock()
		_, err := curl.Post(url, m.Type, bytes.NewBuffer(m.Body))
		if err != nil {
			m.Error <- err
			continue
		}
		r.Len += 1
		close(m.Error)
	}
	log.Printf("stopped replica %q writer", r.ID)
}
