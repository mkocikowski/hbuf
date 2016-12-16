package controller

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gorilla/mux"
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
	WARN  = log.New(os.Stderr, "[WARN] ", log.Lshortfile)
)

type Buffer struct {
	Id  string `json:"id"`
	URL string `json:"url"`
}

type Topic struct {
	Id      string   `json:"id"`
	Buffers []string `json:"buffers"`
}

type Worker struct {
	Id            string `json:"id"`
	URL           string `json:"url"`
	ControllerURL string `json:"-"`
}

type Node struct {
	Id            string `json:"id"`
	Tenant        string `json:"tenant"`
	URL           string `json:"url"`
	ControllerURL string `json:"-"`
	Dir           string `json:"-"`
}

type Controller struct {
	Node
	workers map[string]*Worker
	topics  map[string]*Topic
	buffers map[string]*Buffer
	running bool
	master  bool
	n       int
	routes  []*router.Route
	lock    *sync.Mutex
}

func NewController(n *Node, r *mux.Router) (*Controller, error) {
	c := &Controller{
		Node:    *n,
		workers: make(map[string]*Worker),
		topics:  make(map[string]*Topic),
		buffers: make(map[string]*Buffer),
		lock:    new(sync.Mutex),
	}
	c.routes = []*router.Route{
		{"", []string{"GET"}, c.handleGetInfo, ""},
		{"/workers", []string{"POST"}, c.handleRegisterWorker, ""},
		{"/workers", []string{"GET"}, c.handleGetWorkers, ""},
		{"/topics", []string{"GET"}, c.handleGetTopics, ""},
		{`/topics/{topic:[a-zA-Z0-9_\-]{1,256}}`, []string{"POST"}, c.handleCreateTopic, ""},
		{`/topics/{topic:[a-zA-Z0-9_\-]{1,256}}`, []string{"DELETE"}, c.handleDeleteTopic, ""},
		{`/topics/{topic:[a-zA-Z0-9_\-]{1,256}}`, []string{"GET"}, c.handleGetTopic, ""},
		{"/buffers", []string{"POST"}, c.handleRegisterBuffer, ""},
		{"/buffers", []string{"GET"}, c.handleGetBuffers, ""},
	}
	u, _ := url.Parse(c.URL)
	router.RegisterRoutes(r, u.Path, c.routes)
	//DEBUG.Printf("c: %v", c.URL)
	c.lock.Lock()
	defer c.lock.Unlock()
	//
	if _, err := os.Stat(c.Dir); os.IsNotExist(err) {
		// directory doesn't exist, assume "fresh" node
		return c, nil
	}
	b, err := ioutil.ReadFile(filepath.Join(c.Dir, "topics"))
	if err != nil {
		log.Printf("error reading topics data: %v", err)
		return c, nil
	}
	t := make(map[string]*Topic)
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, fmt.Errorf("error parsing topics data: %v", err)
	}
	//log.Printf("%#v\n", t)
	//log.Println("loaded topics")
	c.topics = t
	return c, nil
}

func (c *Controller) Stop() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.running = false
	os.MkdirAll(c.Dir, 0755)
	j, _ := json.Marshal(c.topics)
	ioutil.WriteFile(filepath.Join(c.Dir, "topics"), j, 0644)
}

func (c *Controller) handleGetInfo(req *http.Request) *router.Response {
	c.lock.Lock()
	j, _ := json.Marshal(c)
	for _, t := range c.topics {
		for _, b := range t.Buffers {
			if _, ok := c.buffers[b]; !ok {
				WARN.Printf("buffer %q not registered for topic %q", b, t.Id)
			}
		}
	}
	c.lock.Unlock()
	return &router.Response{Body: j}
}

func (c *Controller) handleRegisterWorker(req *http.Request) *router.Response {
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return &router.Response{Error: fmt.Errorf("error reading register worker request body: %v", err)}
	}
	w := &Worker{}
	if err := json.Unmarshal(b, w); err != nil {
		return &router.Response{Error: fmt.Errorf("error parsing register worker request body: %v", err), StatusCode: http.StatusBadRequest}
	}
	c.lock.Lock()
	c.workers[w.Id] = w
	c.lock.Unlock()
	//DEBUG.Printf("w: %v", w.URL)
	return &router.Response{StatusCode: http.StatusNoContent}
}

func (c *Controller) handleGetWorkers(req *http.Request) *router.Response {
	c.lock.Lock()
	j, _ := json.Marshal(c.workers)
	c.lock.Unlock()
	return &router.Response{Body: j}
}

func (c *Controller) handleRegisterBuffer(req *http.Request) *router.Response {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return &router.Response{Error: fmt.Errorf("error reading register buffer request body: %v", err)}
	}
	buff := &Buffer{}
	if err := json.Unmarshal(body, buff); err != nil {
		return &router.Response{Error: fmt.Errorf("error parsing register buffer request body: %v", err), StatusCode: http.StatusBadRequest}
	}
	c.lock.Lock()
	c.buffers[buff.Id] = buff
	c.lock.Unlock()
	//DEBUG.Printf("b: %v", buff.URL)
	return &router.Response{StatusCode: http.StatusNoContent}
}

func (c *Controller) handleGetBuffers(req *http.Request) *router.Response {
	c.lock.Lock()
	j, _ := json.Marshal(c.buffers)
	c.lock.Unlock()
	return &router.Response{Body: j}
}

func (c *Controller) pickWorker() (*Worker, error) {
	if len(c.workers) == 0 {
		return nil, fmt.Errorf("no workers registered")
	}
	n := c.n % len(c.workers)
	w := &Worker{}
	for _, w = range c.workers {
		if n == 0 {
			break
		}
		n -= 1
	}
	c.n += 1
	return w, nil
}

func (c *Controller) createBuffer() (*Buffer, error) {
	w, err := c.pickWorker()
	if err != nil {
		return nil, fmt.Errorf("error picking worker for new buffer: %v", err)
	}
	resp, err := client.Post(w.URL+"/buffers", "", nil)
	if err != nil {
		return nil, fmt.Errorf("error making create buffer request, status code: %d", resp.StatusCode)
	}
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("error making create buffer request, status code %d: %v", resp.StatusCode, string(body))
	}
	if err != nil {
		return nil, fmt.Errorf("error reading response body for create buffer request: %v", string(body))
	}
	buff := &Buffer{}
	if err := json.Unmarshal(body, buff); err != nil {
		return nil, fmt.Errorf("error parsing response body for create buffer request: %v", err)
	}
	return buff, nil
}

func (c *Controller) createTopic(id string) (*Topic, error) {
	t := &Topic{
		Id:      id,
		Buffers: make([]string, 0, 3),
	}
	for i := 0; i < 3; i++ {
		b, err := c.createBuffer()
		if err != nil {
			// TODO: cleanup buffers that have already been created?
			return nil, err
		}
		c.buffers[b.Id] = b
		t.Buffers = append(t.Buffers, b.Id)
	}
	c.topics[id] = t
	return t, nil
}

func (c *Controller) handleCreateTopic(req *http.Request) *router.Response {
	id := mux.Vars(req)["topic"]
	if !util.TopicNameRE.MatchString(id) {
		return &router.Response{Error: fmt.Errorf("invalid topic name"), StatusCode: http.StatusBadRequest}
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, ok := c.topics[id]; ok {
		return &router.Response{Error: fmt.Errorf("topic %q already exists", id), StatusCode: http.StatusConflict}
	}
	//DEBUG.Printf("creating topic %q, current topics: %v", id, c.topics)
	t, err := c.createTopic(id)
	if err != nil {
		return &router.Response{Error: fmt.Errorf("error creating topic: %v", err), StatusCode: http.StatusInternalServerError}
	}
	j, _ := json.Marshal(t)
	return &router.Response{Body: j, StatusCode: http.StatusCreated}
}

func (c *Controller) handleGetTopics(req *http.Request) *router.Response {
	c.lock.Lock()
	j, _ := json.Marshal(c.topics)
	c.lock.Unlock()
	return &router.Response{Body: j}
}

func (c *Controller) handleGetTopic(req *http.Request) *router.Response {
	id := mux.Vars(req)["topic"]
	if !util.TopicNameRE.MatchString(id) {
		return &router.Response{Error: fmt.Errorf("invalid topic name"), StatusCode: http.StatusBadRequest}
	}
	c.lock.Lock()
	t, ok := c.topics[id]
	c.lock.Unlock()
	if !ok {
		return &router.Response{Error: fmt.Errorf("topic not found"), StatusCode: http.StatusNotFound}
	}
	j, _ := json.Marshal(t)
	return &router.Response{Body: j}
}

func (c *Controller) deleteBuffer(id string) error {
	b, ok := c.buffers[id]
	if !ok {
		return fmt.Errorf("error deleting buffer %q: buffer doesn't exist", id)
	}
	r, _ := http.NewRequest("DELETE", b.URL, nil)
	resp, err := client.Do(r)
	if err != nil {
		fmt.Errorf("error making request to worker: %v", err)
	}
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		delete(c.buffers, id)
		return nil
	}
	return fmt.Errorf("error response %d from controller: %v", resp.StatusCode, string(body))
}

func (c *Controller) deleteTopic(id string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	t, ok := c.topics[id]
	if !ok {
		return nil
	}
	delete(c.topics, id)
	for _, b := range t.Buffers {
		if err := c.deleteBuffer(b); err != nil {
			log.Printf("error deleting buffer for topic %q; this buffer is now orphaned: %v", id, err)
		}
	}
	return nil
}

func (c *Controller) handleDeleteTopic(req *http.Request) *router.Response {
	id := mux.Vars(req)["topic"]
	if !util.TopicNameRE.MatchString(id) {
		return &router.Response{Error: fmt.Errorf("invalid topic name"), StatusCode: http.StatusBadRequest}
	}
	err := c.deleteTopic(id)
	if err != nil {
		return &router.Response{Error: fmt.Errorf("error deleting topic: %v", err), StatusCode: http.StatusInternalServerError}
	}
	return &router.Response{StatusCode: http.StatusOK}
}
