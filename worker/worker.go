package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/mkocikowski/hbuf/buffer"
	"github.com/mkocikowski/hbuf/controller"
	"github.com/mkocikowski/hbuf/log"
	"github.com/mkocikowski/hbuf/message"
	"github.com/mkocikowski/hbuf/router"
	"github.com/mkocikowski/hbuf/util"
)

var (
	client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 256,
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

func (w *Worker) Init() (*Worker, error) {
	//
	w.buffers = make(map[string]*buffer.Buffer)
	w.lock = new(sync.Mutex)
	w.lock.Lock()
	defer w.lock.Unlock()
	w.routes = []*router.Route{
		{"", []string{"GET"}, w.handleGetInfo, "show information about the node"},
		{"/buffers", []string{"POST"}, w.handleCreateBuffer, "create a buffer; send empty body; returns new buffer info"},
		{"/buffers", []string{"GET"}, w.handleGetBuffers, "show available buffers"},
		{"/buffers/{buffer:[a-f0-9]{16}}", []string{"GET"}, w.handleGetBuffer, "show buffer info"},
		{"/buffers/{buffer:[a-f0-9]{16}}", []string{"POST"}, w.handleWriteToBuffer, "write message to buffer"},
		{"/buffers/{buffer:[a-f0-9]{16}}", []string{"DELETE"}, w.handleDeleteBuffer, "delete buffer and all its data"},
		{"/buffers/{buffer:[a-f0-9]{16}}/replicas", []string{"POST"}, w.handleSetReplicas, "set buffer's replicas"},
		{"/buffers/{buffer:[a-f0-9]{16}}/consumers", []string{"GET"}, w.handleGetOffsets, "get consumers and their offsets"},
		{`/buffers/{buffer:[a-f0-9]{16}}/consumers/{consumer:[a-zA-Z0-9_\-]{1,256}}/_next`, []string{"POST"}, w.handleConsumeFromBuffer, "consume message from buffer"},
	}
	//
	if err := w.loadBuffers(); err != nil {
		return nil, fmt.Errorf("error loading buffers: %v", err)
	}
	if err := w.registerWithController(); err != nil {
		return nil, fmt.Errorf("error registering worker with controller: %v", err)
	}
	return w, nil
}

func (w *Worker) Routes() []*router.Route {
	return w.routes
}

func (w *Worker) loadBuffers() error {
	//
	files, err := ioutil.ReadDir(filepath.Join(w.Path, "buffers"))
	if err != nil && !os.IsNotExist(err) {
		log.WARN.Printf("can't access worker's data directory: %v", err)
		return nil
	}
	for _, f := range files {
		uid := f.Name()
		b := &buffer.Buffer{
			ID:     uid,
			URL:    w.URL + "/buffers/" + uid,
			Tenant: w.Tenant,
			Path:   filepath.Join(w.Path, "buffers", uid),
		}
		if err := b.Init(); err != nil {
			log.WARN.Printf("error initializing buffer from disk: %v", err)
			continue
		}
		w.buffers[b.ID] = b
		//j, _ := json.Marshal(b)
		log.DEBUG.Printf("loaded buffer: %v", b.Path)
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
		return fmt.Errorf("error registering worker with controller: (%d) %v", resp.StatusCode, string(body))
	}
	//
	for _, b := range w.buffers {
		c := controller.Buffer{
			ID:       b.ID,
			URL:      b.URL,
			Replicas: make([]string, 0),
		}
		for _, r := range b.Replicas {
			c.Replicas = append(c.Replicas, r.ID)
		}
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
			return fmt.Errorf("error registering buffer with controller: (%d) %v", resp.StatusCode, string(body))
		}
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
	log.DEBUG.Printf("worker %q stopped", w.ID)
}

func (w *Worker) handleGetInfo(req *http.Request) *router.Response {
	w.lock.Lock()
	defer w.lock.Unlock()
	j, _ := json.Marshal(w)
	log.DEBUG.Println(req.Proto)
	return &router.Response{Body: j}
}

func (w *Worker) handleCreateBuffer(req *http.Request) *router.Response {
	uid := util.Uid()
	b := &buffer.Buffer{
		ID:     uid,
		URL:    w.URL + "/buffers/" + uid,
		Tenant: w.Tenant,
		Path:   filepath.Join(w.Path, "buffers", uid),
	}
	if err := b.Init(); err != nil {
		return &router.Response{Error: fmt.Errorf("error creating buffer: %v", err), StatusCode: http.StatusInternalServerError}
	}
	w.lock.Lock()
	w.buffers[b.ID] = b
	w.lock.Unlock()
	j, _ := json.Marshal(b)
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
		log.WARN.Printf("error reading set replica body: %v", err)
		return &router.Response{Error: fmt.Errorf("error reading set replica body: %v", err)}
	}
	var replicas []string
	if err := json.Unmarshal(body, &replicas); err != nil {
		return &router.Response{Error: fmt.Errorf("error parsing set replica body: %v", err)}
	}
	b.Replicas = make(map[string]*buffer.Replica)
	for _, r := range replicas {
		b.Replicas[r] = &buffer.Replica{ID: r}
	}
	log.DEBUG.Printf("set replicas %q for buffer %q", replicas, b.ID)
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
	return &router.Response{Body: j}
}

func (w *Worker) handleWriteToBuffer(req *http.Request) *router.Response {
	//dump, _ := httputil.DumpRequest(req, true)
	//log.Println(string(dump))
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.DEBUG.Println(err)
		return &router.Response{Error: fmt.Errorf("error reading message body: %v", err)}
	}
	buffer := mux.Vars(req)["buffer"]
	w.lock.Lock()
	b, ok := w.buffers[buffer]
	w.lock.Unlock()
	if !ok {
		log.DEBUG.Println("couldn't find buffer:", buffer)
		return &router.Response{Error: fmt.Errorf("buffer %q not found", buffer), StatusCode: http.StatusNotFound}
	}
	m := &message.Message{
		Type: req.Header.Get("Content-Type"),
		Body: body,
	}
	if err := b.Write(m); err != nil {
		// theoretically the buffer may have been destroyed in the mean time
		log.DEBUG.Println("error writing message body to disk: %v", err)
		return &router.Response{Error: fmt.Errorf("error writing message body: %v", err), StatusCode: http.StatusInternalServerError}
	}
	return &router.Response{StatusCode: http.StatusOK}
}

func (w *Worker) handleReadFromBuffer(req *http.Request) *router.Response {
	offset := req.URL.Query().Get("offset")
	i, err := strconv.Atoi(offset)
	if err != nil {
		return &router.Response{Error: fmt.Errorf("bad offset %q (expected aw integer): %v", offset, err), StatusCode: http.StatusBadRequest}
	}
	buffer := mux.Vars(req)["buffer"]
	w.lock.Lock()
	b, ok := w.buffers[buffer]
	w.lock.Unlock()
	if !ok {
		return &router.Response{Error: fmt.Errorf("buffer %q not found", buffer), StatusCode: http.StatusNotFound}
	}
	m, err := b.Read(i)
	if err != nil {
		return &router.Response{Error: fmt.Errorf("error reading from buffer %q: %v", buffer, err), StatusCode: http.StatusInternalServerError}
	}
	return &router.Response{Body: m.Body, ContentType: m.Type}
}

func (w *Worker) handleConsumeFromBuffer(req *http.Request) *router.Response {
	buffer := mux.Vars(req)["buffer"]
	w.lock.Lock()
	b, ok := w.buffers[buffer]
	w.lock.Unlock()
	if !ok {
		return &router.Response{Error: fmt.Errorf("buffer %q not found", buffer), StatusCode: http.StatusNotFound}
	}
	consumer := mux.Vars(req)["consumer"]
	if consumer == "" {
		consumer = "-"
	}
	m, err := b.Consume(consumer)
	if err != nil {
		return &router.Response{Error: fmt.Errorf("error consuming from buffer %q, consumer id %q: %v", buffer, consumer, err), StatusCode: http.StatusInternalServerError}
	}
	return &router.Response{Body: m.Body, ContentType: m.Type}
}

func (w *Worker) handleGetOffsets(req *http.Request) *router.Response {
	buffer := mux.Vars(req)["buffer"]
	w.lock.Lock()
	b, ok := w.buffers[buffer]
	w.lock.Unlock()
	if !ok {
		return &router.Response{Error: fmt.Errorf("buffer %q not found", buffer), StatusCode: http.StatusNotFound}
	}
	return &router.Response{Body: b.GetConsumers()}
}
