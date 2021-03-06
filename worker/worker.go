package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/mkocikowski/hbuf/buffer"
	"github.com/mkocikowski/hbuf/message"
	"github.com/mkocikowski/hbuf/router"
	"github.com/mkocikowski/hbuf/segment"
	"github.com/mkocikowski/hbuf/util"
)

var (
	client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 1000,
		},
		Timeout: 5 * time.Second,
	}
)

type Worker struct {
	ID         string `json:"id"`
	URL        string `json:"url"`
	Tenant     string `json:"-"`
	Controller string `json:"-"`
	Path       string `json:"-"`
	routes     []*router.Route
	buffers    map[string]*buffer.Buffer
	running    bool
	lock       *sync.Mutex
}

func (w *Worker) Init() error {
	//
	w.buffers = make(map[string]*buffer.Buffer)
	w.lock = new(sync.Mutex)
	w.lock.Lock()
	defer w.lock.Unlock()
	w.routes = []*router.Route{
		{"", []string{"GET"}, w.handleGetInfo, ""},
		{"/buffers", []string{"POST"}, w.handleCreateBuffer, ""},
		{"/buffers", []string{"GET"}, w.handleGetBuffers, ""},
		{"/buffers/{buffer:[a-f0-9]{16}}", []string{"GET"}, w.handleGetBuffer, ""},
		{"/buffers/{buffer:[a-f0-9]{16}}", []string{"POST"}, w.handleWriteToBuffer, ""},
		{"/buffers/{buffer:[a-f0-9]{16}}", []string{"DELETE"}, w.handleDeleteBuffer, ""},
		{"/buffers/{buffer:[a-f0-9]{16}}/replicas", []string{"POST"}, w.handleSetReplicas, ""},
		{"/buffers/{buffer:[a-f0-9]{16}}/consumers", []string{"GET"}, w.handleGetOffsets, ""},
		{
			`/buffers/{buffer:[a-f0-9]{16}}/consumers/{consumer:[a-zA-Z0-9_\-]{1,256}}/_next`,
			[]string{"POST"}, w.handleConsumeFromBuffer, "",
		},
	}
	if err := w.loadBuffers(); err != nil {
		return fmt.Errorf("error loading buffers: %v", err)
	}
	if err := w.registerWithController(); err != nil {
		return fmt.Errorf("error registering worker with controller: %v", err)
	}
	return nil
}

func (w *Worker) Routes() []*router.Route {
	return w.routes
}

func (w *Worker) loadBuffers() error {
	//
	files, err := ioutil.ReadDir(filepath.Join(w.Path, "buffers"))
	if err != nil && !os.IsNotExist(err) {
		log.Printf("can't access worker's data directory: %v", err)
		return nil
	}
	for _, f := range files {
		uid := f.Name()
		b := &buffer.Buffer{
			ID:         uid,
			URL:        w.URL + "/buffers/" + uid,
			Controller: w.Controller,
			Tenant:     w.Tenant,
			Path:       filepath.Join(w.Path, "buffers", uid),
		}
		if err := b.Init(); err != nil {
			log.Printf("error initializing buffer from disk: %v", err)
			continue
		}
		w.buffers[b.ID] = b
		//j, _ := json.Marshal(b)
		log.Printf("loaded buffer: %v", b.Path)
	}
	return nil
}

func (w *Worker) registerWithController() error {
	//
	j, _ := json.Marshal(w)
	resp, err := client.Post(w.Controller+"/workers", "application/json", bytes.NewBuffer(j))
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("error registering worker: (%d) %v", resp.StatusCode, string(body))
	}
	//
	for _, b := range w.buffers {
		c := map[string]string{"id": b.ID, "url": b.URL}
		j, _ := json.Marshal(c)
		resp, err := client.Post(w.Controller+"/buffers", "application/json", bytes.NewBuffer(j))
		if err != nil {
			return err
		}
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusNoContent {
			return fmt.Errorf("error registering buffer: (%d) %v", resp.StatusCode, string(body))
		}
		log.Printf("registered buffer %q with controller", b.ID)
	}
	return nil
}

func (w *Worker) Stop() {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.running = false
	for _, b := range w.buffers {
		b.Stop()
	}
	log.Printf("worker %q stopped", w.ID)
}

func (w *Worker) handleGetInfo(req *http.Request) *router.Response {
	w.lock.Lock()
	defer w.lock.Unlock()
	j, _ := json.Marshal(w)
	log.Println(req.Proto)
	return &router.Response{Body: j}
}

func (w *Worker) handleCreateBuffer(req *http.Request) *router.Response {
	//
	uid := util.Uid()
	b := &buffer.Buffer{
		ID:         uid,
		URL:        w.URL + "/buffers/" + uid,
		Controller: w.Controller,
		Tenant:     w.Tenant,
		Path:       filepath.Join(w.Path, "buffers", uid),
	}
	if err := b.Init(); err != nil {
		return &router.Response{Error: fmt.Errorf("error creating buffer: %v", err)}
	}
	w.lock.Lock()
	w.buffers[b.ID] = b
	w.lock.Unlock()
	j, _ := json.Marshal(b)
	log.Printf("created buffer %q", b.ID)
	return &router.Response{Body: j, StatusCode: http.StatusCreated}
}

func (w *Worker) handleDeleteBuffer(req *http.Request) *router.Response {
	id := mux.Vars(req)["buffer"]
	w.lock.Lock()
	defer w.lock.Unlock()
	b, ok := w.buffers[id]
	if !ok {
		return &router.Response{StatusCode: http.StatusNotFound}
	}
	if err := b.Delete(); err != nil {
		return &router.Response{Error: fmt.Errorf("error deleting buffer: %v", err)}
	}
	delete(w.buffers, id)
	return &router.Response{StatusCode: http.StatusOK}
}

func (w *Worker) handleSetReplicas(req *http.Request) *router.Response {
	//
	w.lock.Lock()
	b, ok := w.buffers[mux.Vars(req)["buffer"]]
	w.lock.Unlock()
	if !ok {
		return &router.Response{StatusCode: http.StatusNotFound}
	}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Printf("error reading set replica body: %v", err)
		return &router.Response{Error: fmt.Errorf("error reading set replica body: %v", err)}
	}
	var replicas []string
	if err := json.Unmarshal(body, &replicas); err != nil {
		return &router.Response{Error: fmt.Errorf("error parsing set replica body: %v", err)}
	}
	b.SetReplicas(replicas)
	log.Printf("set replicas %q for buffer %q", replicas, b.ID)
	// TODO: should this call the replicas to see what's up?
	return &router.Response{StatusCode: http.StatusOK}
}

func (w *Worker) handleGetBuffers(req *http.Request) *router.Response {
	//
	w.lock.Lock()
	j, _ := json.Marshal(w.buffers)
	w.lock.Unlock()
	return &router.Response{Body: j}
}

func (w *Worker) handleGetBuffer(req *http.Request) *router.Response {
	//
	buffer := mux.Vars(req)["buffer"]
	w.lock.Lock()
	defer w.lock.Unlock()
	b, ok := w.buffers[buffer]
	if !ok {
		return &router.Response{
			Error:      fmt.Errorf("buffer %q not found", buffer),
			StatusCode: http.StatusNotFound,
		}
	}
	j, _ := json.Marshal(b)
	//log.Println(string(j))
	return &router.Response{Body: j}
}

func (w *Worker) handleWriteToBuffer(req *http.Request) *router.Response {
	//
	w.lock.Lock()
	b, ok := w.buffers[mux.Vars(req)["buffer"]]
	w.lock.Unlock()
	if !ok {
		return &router.Response{StatusCode: http.StatusNotFound}
	}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return &router.Response{Error: fmt.Errorf("error reading message body: %v", err)}
	}
	id := 0
	if h := req.Header.Get("Hbuf-Id"); h != "" {
		id, err = strconv.Atoi(h)
		if err != nil {
			return &router.Response{
				Error:      fmt.Errorf("error parsing Hbuf-Id header: %v", err),
				StatusCode: http.StatusBadRequest,
			}
		}
	}
	ts := time.Now().UTC()
	if h := req.Header.Get("Hbuf-Ts"); h != "" {
		ts, err = time.Parse(time.RFC3339Nano, h)
		if err != nil {
			return &router.Response{
				Error:      fmt.Errorf("error parsing Hbuf-Ts header: %v", err),
				StatusCode: http.StatusBadRequest,
			}
		}
	}
	m := &message.Message{
		ID:   id,
		TS:   ts,
		Type: req.Header.Get("Content-Type"),
		Body: body,
	}
	if err := b.Write(m); err != nil {
		// theoretically the buffer may have been destroyed in the mean time
		log.Printf("error writing message body to disk: %v", err)
		return &router.Response{Error: fmt.Errorf("error writing message body: %v", err)}
	}
	j, _ := json.Marshal(m)
	return &router.Response{Body: j}
}

func (w *Worker) handleReadFromBuffer(req *http.Request) *router.Response {
	offset := req.URL.Query().Get("offset")
	i, err := strconv.Atoi(offset)
	if err != nil {
		return &router.Response{
			Error:      fmt.Errorf("bad offset %q (expected aw integer): %v", offset, err),
			StatusCode: http.StatusBadRequest,
		}
	}
	buffer := mux.Vars(req)["buffer"]
	w.lock.Lock()
	b, ok := w.buffers[buffer]
	w.lock.Unlock()
	if !ok {
		return &router.Response{StatusCode: http.StatusNotFound}
	}
	m, err := b.Read(i)
	if err != nil {
		return &router.Response{Error: fmt.Errorf("error reading from buffer %q: %v", buffer, err)}
	}
	return &router.Response{Body: m.Body, ContentType: m.Type}
}

func (w *Worker) handleConsumeFromBuffer(req *http.Request) *router.Response {
	//
	buffer := mux.Vars(req)["buffer"]
	w.lock.Lock()
	b, ok := w.buffers[buffer]
	w.lock.Unlock()
	if !ok {
		return &router.Response{StatusCode: http.StatusNotFound}
	}
	consumer := mux.Vars(req)["consumer"]
	if consumer == "" {
		consumer = "-"
	}
	m, err := b.Consume(consumer)
	if err == segment.ErrorOutOfBounds {
		return &router.Response{StatusCode: http.StatusNoContent}
	}
	if err == io.EOF {
		return &router.Response{StatusCode: http.StatusNoContent}
	}
	if err != nil {
		log.Printf("error consuming from buffer %q, consumer id %q: %v", buffer, consumer, err)
		return &router.Response{Error: fmt.Errorf("error consuming from buffer: %v", err)}
	}
	return &router.Response{Body: m.Body, ContentType: m.Type}
}

func (w *Worker) handleGetOffsets(req *http.Request) *router.Response {
	//
	w.lock.Lock()
	b, ok := w.buffers[mux.Vars(req)["buffer"]]
	w.lock.Unlock()
	if !ok {
		return &router.Response{StatusCode: http.StatusNotFound}
	}
	return &router.Response{Body: b.Consumers()}
}
