package buffer

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/mkocikowski/hbuf/curl"
	"github.com/mkocikowski/hbuf/segment"
)

type replica struct {
	ID      string `json:"id"`
	URL     string `json:"url"`
	manager string
	length  int
	buffer  *Buffer
	lock    *sync.Mutex
	wg      sync.WaitGroup
	data    chan bool
	sync    chan bool
	done    chan bool
	isUp    chan bool
}

func (r *replica) Init() error {

	r.data = make(chan bool, 1)
	r.sync = make(chan bool, 1)
	r.done = make(chan bool)
	r.isUp = make(chan bool)
	r.lock = new(sync.Mutex)

	r.wg.Add(2)
	go r.update()
	go r.writer()

	return nil
}

func (r *replica) Stop() {
	close(r.done)
	//r.wg.Wait()
	log.Printf("replica %q stopped", r.ID)
}

func (r *replica) Len() int {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.length
}

func (r *replica) update() {

	defer r.wg.Done()
	// get buffer URL from manager
	for {
		select {
		case <-r.done:
			return
		default:
		}
		u := r.manager + "/buffers/" + r.ID
		b, err := curl.Get(u)
		if err != nil {
			log.Printf("error getting replica URL from manager %q: %v", u, err)
			time.Sleep(1 * time.Second)
			continue
		}
		buffer := Buffer{}
		if err := json.Unmarshal(b, &buffer); err != nil {
			log.Printf("error parsing replica metadata: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		r.URL = buffer.URL
		break
	}
	// get length of the remote buffer (where the replica data is written to)
	for {
		select {
		case <-r.done:
			return
		default:
		}
		b, err := curl.Get(r.URL)
		if err != nil {
			log.Printf("error getting replica length from buffer %q: %v", r.URL, err)
			time.Sleep(1 * time.Second)
			continue
		}
		buffer := Buffer{}
		if err := json.Unmarshal(b, &buffer); err != nil {
			log.Printf("error parsing replica metadata: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		r.length = buffer.Len
		log.Printf("replica %q length: %d", r.ID, r.length)
		break
	}
	close(r.isUp)
	r.data <- true // signal for writer to start polling the buffer
}

func (r *replica) writer() {

	defer r.wg.Done()
	<-r.isUp
	log.Printf("starting writer for replica %q", r.ID)

	for {
		select {
		case <-r.data:
		case <-r.done:
			return
		}
		r.lock.Lock()
		l := r.length
		u := r.URL
		r.lock.Unlock()
		for {
			m, err := r.buffer.Read(l)
			if err == segment.ErrorOutOfBounds {
				break
			}
			if err != nil {
				log.Println(err)
				break
			}
			req, _ := http.NewRequest("POST", u, bytes.NewBuffer(m.Body))
			req.Header.Add("Content-Type", m.Type)
			req.Header.Add("Hbuf-Ts", m.TS.Format(time.RFC3339Nano))
			req.Header.Add("Hbuf-Id", strconv.Itoa(m.ID))
			b, err := curl.Do(req)
			if err != nil {
				log.Println(err)
				break
			}

			// TODO: read remote buffer length, compare to expected
			x := struct {
				ID int `json:"id"`
			}{}
			if err = json.Unmarshal(b, &x); err != nil {
				log.Printf("error parsing write response from remote: %v", err)
				break
			}
			if x.ID != l {
				log.Println("error replicating: remote message id doesn't match expected")
				break
			}
			log.Printf("replicated: %v", string(b))

			l += 1
			r.lock.Lock()
			r.length = l
			r.lock.Unlock()
			//log.Println("replicated")
			select {
			case r.sync <- true: // signal that length has changed
			default:
			}
		}
	}

	log.Printf("stopped replica %q writer", r.ID)
}
