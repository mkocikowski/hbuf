package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/mkocikowski/hbuf/buffer"
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
	DEBUG = log.New(os.Stderr, "[DEBUG] ", log.Lshortfile)
)

type Node struct {
	Id            string `json:"id"`
	Tenant        string `json:"tenant"`
	URL           string `json:"url"`
	ControllerURL string `json:"-"`
	Dir           string `json:"-"`
}

type Worker struct {
	Node
	routes  []*router.Route
	buffers map[string]*buffer.Buffer
	running bool
	lock    *sync.Mutex
}

func NewWorker(n *Node, r *mux.Router) (*Worker, error) {
	w := &Worker{
		Node:    *n,
		buffers: make(map[string]*buffer.Buffer),
		lock:    new(sync.Mutex),
	}
	w.lock.Lock()
	defer w.lock.Unlock()
	w.routes = []*router.Route{
		{"/", []string{"GET"}, w.handleGetInfo, "show information about the node"},
		{"/buffers", []string{"POST"}, w.handleCreateBuffer, "create a buffer; send empty body; returns new buffer info"},
		{"/buffers", []string{"GET"}, w.handleGetBuffers, "show available buffers"},
		{"/buffers/{buffer:[a-f0-9]{16}}", []string{"GET"}, w.handleGetBuffer, "show buffer info"},
		{"/buffers/{buffer:[a-f0-9]{16}}", []string{"POST"}, w.handleWriteToBuffer, "write message to buffer"},
		{"/buffers/{buffer:[a-f0-9]{16}}", []string{"DELETE"}, w.handleDeleteBuffer, "delete buffer and all its data"},
		{"/buffers/{buffer:[a-f0-9]{16}}/consumers", []string{"GET"}, w.handleGetOffsets, "get consumers and their offsets"},
		{`/buffers/{buffer:[a-f0-9]{16}}/consumers/{consumer:[a-zA-Z0-9_\-]{1,256}}/_next`, []string{"POST"}, w.handleConsumeFromBuffer, "consume message from buffer"},
		//{"/buffers/{buffer}/_read", []string{"GET"}, w.handleReadFromBuffer, "read message specified by ?offset="},
		//{"/buffers/{buffer}/_consume", []string{"POST"}, w.handleConsumeFromBuffer, "consume from buffer; optional ?id= specifies consumer"},
	}
	u, _ := url.Parse(w.URL)
	router.RegisterRoutes(r, u.Path, w.routes)
	//
	path := filepath.Join(w.Dir, "buffers")
	files, err := ioutil.ReadDir(path)
	if err != nil && !os.IsNotExist(err) {
		DEBUG.Printf("error creating worker: %v", err)
	}
	for _, f := range files {
		uid := f.Name()
		b := &buffer.Buffer{
			Id:     uid,
			Tenant: w.Tenant,
			URL:    w.URL + "/buffers/" + uid,
			Dir:    filepath.Join(w.Dir, "buffers", uid),
		}
		if err := b.Init(); err != nil {
			DEBUG.Printf("error creating worker: %v", err)
			continue
		}
		w.buffers[b.Id] = b
		//j, _ := json.Marshal(b)
		//log.Printf("loaded buffer: %v", b.Dir)
	}
	err = w.registerWithController()
	if err != nil {
		return nil, fmt.Errorf("error creating worker: %v", err)
	}
	return w, nil
}

func (w *Worker) registerWithController() error {
	j, _ := json.Marshal(w)
	resp, err := client.Post(w.ControllerURL+"/workers", "application/json", bytes.NewBuffer(j))
	if err != nil {
		return err
	}
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("error registering worker with controller: (%d) %v", resp.StatusCode, string(body))
	}
	for _, b := range w.buffers {
		j, _ = json.Marshal(b)
		resp, err := client.Post(w.ControllerURL+"/buffers", "application/json", bytes.NewBuffer(j))
		if err != nil {
			return err
		}
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
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
}

func (w *Worker) handleGetInfo(req *http.Request) *router.Response {
	w.lock.Lock()
	defer w.lock.Unlock()
	j, _ := json.Marshal(w)
	return &router.Response{Body: j}
}

func (w *Worker) handleCreateBuffer(req *http.Request) *router.Response {
	uid := util.Uid()
	b := &buffer.Buffer{
		Id:     uid,
		Tenant: w.Tenant,
		URL:    w.URL + "/buffers/" + uid,
		Dir:    filepath.Join(w.Dir, "buffers", uid),
	}
	if err := b.Init(); err != nil {
		return &router.Response{Error: fmt.Errorf("error creating buffer: %v", err), StatusCode: http.StatusInternalServerError}
	}
	w.lock.Lock()
	w.buffers[b.Id] = b
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
		return &router.Response{Error: fmt.Errorf("error deleting buffer: %v", err), StatusCode: http.StatusInternalServerError}
	}
	delete(w.buffers, id)
	return &router.Response{StatusCode: http.StatusOK}
}

func (w *Worker) handleGetBuffers(req *http.Request) *router.Response {
	w.lock.Lock()
	j, _ := json.Marshal(w.buffers)
	w.lock.Unlock()
	return &router.Response{Body: j}
}

func (w *Worker) handleGetBuffer(req *http.Request) *router.Response {
	bufId := mux.Vars(req)["buffer"]
	w.lock.Lock()
	defer w.lock.Unlock()
	b, ok := w.buffers[bufId]
	if !ok {
		return &router.Response{Error: fmt.Errorf("buffer %q not found", bufId), StatusCode: http.StatusNotFound}
	}
	j, _ := json.Marshal(b)
	return &router.Response{Body: j}
}

func (w *Worker) handleWriteToBuffer(req *http.Request) *router.Response {
	//dump, _ := httputil.DumpRequest(req, true)
	//log.Println(string(dump))
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		DEBUG.Println(err)
		return &router.Response{Error: fmt.Errorf("error reading message body: %v", err), StatusCode: http.StatusInternalServerError}
	}
	bufId := mux.Vars(req)["buffer"]
	w.lock.Lock()
	b, ok := w.buffers[bufId]
	w.lock.Unlock()
	if !ok {
		DEBUG.Println("couldn't find buffer:", bufId)
		return &router.Response{Error: fmt.Errorf("buffer %q not found", bufId), StatusCode: http.StatusNotFound}
	}
	m := &message.Message{
		Type: req.Header.Get("Content-Type"),
		Body: body,
	}
	if err := b.Write(m); err != nil {
		// theoretically the buffer may have been destroyed in the mean time
		DEBUG.Println("error writing message body to disk: %v", err)
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
	bufId := mux.Vars(req)["buffer"]
	w.lock.Lock()
	b, ok := w.buffers[bufId]
	w.lock.Unlock()
	if !ok {
		return &router.Response{Error: fmt.Errorf("buffer %q not found", bufId), StatusCode: http.StatusNotFound}
	}
	m, err := b.Read(i)
	if err != nil {
		return &router.Response{Error: fmt.Errorf("error reading from buffer %q: %v", bufId, err), StatusCode: http.StatusInternalServerError}
	}
	return &router.Response{Body: m.Body, ContentType: m.Type}
}

func (w *Worker) handleConsumeFromBuffer(req *http.Request) *router.Response {
	bufId := mux.Vars(req)["buffer"]
	w.lock.Lock()
	b, ok := w.buffers[bufId]
	w.lock.Unlock()
	if !ok {
		return &router.Response{Error: fmt.Errorf("buffer %q not found", bufId), StatusCode: http.StatusNotFound}
	}
	consId := mux.Vars(req)["consumer"]
	if consId == "" {
		consId = "-"
	}
	m, err := b.Consume(consId)
	if err != nil {
		return &router.Response{Error: fmt.Errorf("error consuming from buffer %q, consumer id %q: %v", bufId, consId, err), StatusCode: http.StatusInternalServerError}
	}
	return &router.Response{Body: m.Body, ContentType: m.Type}
}

func (w *Worker) handleGetOffsets(req *http.Request) *router.Response {
	bufId := mux.Vars(req)["buffer"]
	w.lock.Lock()
	b, ok := w.buffers[bufId]
	w.lock.Unlock()
	if !ok {
		return &router.Response{Error: fmt.Errorf("buffer %q not found", bufId), StatusCode: http.StatusNotFound}
	}
	return &router.Response{Body: b.GetConsumers()}
}
