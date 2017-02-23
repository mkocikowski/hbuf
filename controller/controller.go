package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/mkocikowski/hbuf/router"
)

var (
	client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 5000,
		},
		Timeout: 5 * time.Second,
	}
)

type Buffer struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type Topic struct {
	ID      string   `json:"id"`
	Buffers []string `json:"buffers"`
}

type Worker struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type State struct {
	Workers map[string]*Worker `json:"workers"`
	Topics  map[string]*Topic  `json:"topics"`
	Buffers map[string]*Buffer `json:"buffers"`
}

type Controller struct {
	ID       string `json:"id"`
	URL      string `json:"url"`
	Tenant   string `json:"-"`
	Path     string `json:"-"`
	routes   []*router.Route
	workers  map[string]*Worker
	topics   map[string]*Topic
	buffers  map[string]*Buffer
	replicas map[string][]string
	running  bool
	n        int
	lock     *sync.Mutex
}

func (c *Controller) Init() (*Controller, error) {
	//
	c.workers = make(map[string]*Worker)
	c.topics = make(map[string]*Topic)
	c.buffers = make(map[string]*Buffer)
	c.replicas = make(map[string][]string)
	c.lock = new(sync.Mutex)
	c.lock.Lock()
	defer c.lock.Unlock()
	//
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
		{"/buffers/{buffer:[a-f0-9]{16}}", []string{"GET"}, c.handleGetBuffer, ""},
	}
	//
	if _, err := os.Stat(c.Path); os.IsNotExist(err) {
		// directory doesn't exist, assume "fresh" node
		return c, nil
	}
	t, err := ioutil.ReadFile(filepath.Join(c.Path, "topics"))
	if err != nil {
		log.Printf("error reading topics data: %v", err)
		return c, nil
	}
	if err := json.Unmarshal(t, &c.topics); err != nil {
		return nil, fmt.Errorf("error parsing topics data: %v", err)
	}
	//
	r, err := ioutil.ReadFile(filepath.Join(c.Path, "replicas"))
	if err != nil {
		log.Printf("error reading replicas data: %v", err)
		return c, nil
	}
	if err := json.Unmarshal(r, &c.replicas); err != nil {
		return nil, fmt.Errorf("error parsing replicas data: %v", err)
	}
	for p, r := range c.replicas {
		go c.setReplicas(p, r)
	}
	//
	return c, nil
}

func (c *Controller) Routes() []*router.Route {
	return c.routes
}

func (c *Controller) Stop() {
	//
	c.lock.Lock()
	defer c.lock.Unlock()
	c.running = false
	os.MkdirAll(c.Path, 0755)
	t, _ := json.Marshal(c.topics)
	ioutil.WriteFile(filepath.Join(c.Path, "topics"), t, 0644)
	r, _ := json.Marshal(c.replicas)
	ioutil.WriteFile(filepath.Join(c.Path, "replicas"), r, 0644)
	log.Printf("controller %q stopped", c.ID)
}

func (c *Controller) handleGetInfo(req *http.Request) *router.Response {
	//
	c.lock.Lock()
	state := State{
		Topics:  c.topics,
		Buffers: c.buffers,
		Workers: c.workers,
	}
	j, _ := json.Marshal(state)
	c.lock.Unlock()
	return &router.Response{Body: j}
}

func (c *Controller) handleRegisterWorker(req *http.Request) *router.Response {
	//
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return &router.Response{Error: fmt.Errorf("error reading register worker request body: %v", err)}
	}
	w := &Worker{}
	if err := json.Unmarshal(b, w); err != nil {
		return &router.Response{
			Error:      fmt.Errorf("error parsing register worker request body: %v", err),
			StatusCode: http.StatusBadRequest,
		}
	}
	c.lock.Lock()
	c.workers[w.ID] = w
	c.lock.Unlock()
	log.Printf("registered worker: %v", w.URL)
	return &router.Response{StatusCode: http.StatusNoContent}
}

func (c *Controller) handleGetWorkers(req *http.Request) *router.Response {
	//
	c.lock.Lock()
	j, _ := json.Marshal(c.workers)
	c.lock.Unlock()
	return &router.Response{Body: j}
}

func (c *Controller) handleRegisterBuffer(req *http.Request) *router.Response {
	//
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return &router.Response{Error: fmt.Errorf("error reading register buffer request body: %v", err)}
	}
	remote := &Buffer{}
	if err := json.Unmarshal(body, remote); err != nil {
		return &router.Response{
			Error:      fmt.Errorf("error parsing register buffer request body: %v", err),
			StatusCode: http.StatusBadRequest,
		}
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	c.buffers[remote.ID] = remote
	return &router.Response{StatusCode: http.StatusNoContent}
}

func (c *Controller) handleGetBuffers(req *http.Request) *router.Response {
	//
	c.lock.Lock()
	j, _ := json.Marshal(c.buffers)
	c.lock.Unlock()
	return &router.Response{Body: j}
}

func (c *Controller) handleGetBuffer(req *http.Request) *router.Response {
	//
	c.lock.Lock()
	b, ok := c.buffers[mux.Vars(req)["buffer"]]
	c.lock.Unlock()
	if !ok {
		return &router.Response{StatusCode: http.StatusNotFound}
	}
	j, _ := json.Marshal(b)
	return &router.Response{Body: j}
}

func (c *Controller) pickWorker() (*Worker, error) {
	//
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
	//
	w, err := c.pickWorker()
	if err != nil {
		return nil, fmt.Errorf("error picking worker for new buffer: %v", err)
	}
	resp, err := client.Post(w.URL+"/buffers", "", nil)
	if err != nil {
		return nil, fmt.Errorf("error making create buffer request: %v", err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("error reading response body for create buffer request: %v", string(body))
	}
	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("error making create buffer request: (%d) %v", resp.StatusCode, string(body))
	}
	b := &Buffer{}
	if err := json.Unmarshal(body, b); err != nil {
		return nil, fmt.Errorf("error parsing response body for create buffer request: %v", err)
	}
	return b, nil
}

func (c *Controller) setReplicas(primary string, replicas []string) {
	//
	for {
		c.lock.Lock()
		b, ok := c.buffers[primary]
		c.lock.Unlock()
		if !ok {
			log.Printf("buffer %q not registered with controller, can't set replicas", primary)
			time.Sleep(1 * time.Second)
			continue
		}
		j, _ := json.Marshal(replicas)
		resp, err := client.Post(b.URL+"/replicas", "application/json", bytes.NewBuffer(j))
		if err != nil {
			log.Printf("error making set replicas request: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Printf("error reading response body for set replicas request: %v", string(body))
			time.Sleep(1 * time.Second)
			continue
		}
		if resp.StatusCode == http.StatusOK {
			log.Printf("replicas %q for buffer %q set successfuly", replicas, b.ID)
			return
		}
		fmt.Printf("error making set replicas request: (%d) %v", resp.StatusCode, string(body))
		time.Sleep(1 * time.Second)
	}
}

func (c *Controller) createTopic(id string) (*Topic, error) {
	//
	t := &Topic{
		ID:      id,
		Buffers: make([]string, 0, 3),
	}
	for i := 0; i < 3; i++ {
		b, err := c.createBuffer()
		if err != nil {
			// TODO: cleanup buffers that have already been created?
			return nil, fmt.Errorf("error creating primary buffer: %v", err)
		}
		c.buffers[b.ID] = b
		t.Buffers = append(t.Buffers, b.ID)
		//
		replicas := make([]string, 0, 2)
		for i := 0; i < 2; i++ {
			r, err := c.createBuffer()
			if err != nil {
				return nil, fmt.Errorf("error creating replica buffer: %v", err)
			}
			c.buffers[r.ID] = r
			replicas = append(replicas, r.ID)
		}
		c.replicas[b.ID] = replicas
		go c.setReplicas(b.ID, replicas)
	}
	return t, nil
}

func (c *Controller) handleCreateTopic(req *http.Request) *router.Response {
	//
	id := mux.Vars(req)["topic"]
	c.lock.Lock()
	defer c.lock.Unlock()
	//
	if _, ok := c.topics[id]; ok {
		return &router.Response{
			Error:      fmt.Errorf("topic %q already exists", id),
			StatusCode: http.StatusConflict,
		}
	}
	t, err := c.createTopic(id)
	if err != nil {
		log.Printf("error creating topic: %v", err)
		return &router.Response{
			Error:      fmt.Errorf("error creating topic: %v", err),
			StatusCode: http.StatusInternalServerError,
		}
	}
	c.topics[id] = t
	j, _ := json.Marshal(t)
	return &router.Response{Body: j, StatusCode: http.StatusCreated}
}

func (c *Controller) handleGetTopics(req *http.Request) *router.Response {
	//
	c.lock.Lock()
	j, _ := json.Marshal(c.topics)
	c.lock.Unlock()
	return &router.Response{Body: j}
}

func (c *Controller) handleGetTopic(req *http.Request) *router.Response {
	//
	id := mux.Vars(req)["topic"]
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
	//
	b, ok := c.buffers[id]
	if !ok {
		return fmt.Errorf("error deleting buffer %q: buffer doesn't exist", id)
	}
	//
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
	//
	c.lock.Lock()
	defer c.lock.Unlock()
	t, ok := c.topics[id]
	if !ok {
		return nil
	}
	//
	delete(c.topics, id)
	for _, b := range t.Buffers {
		if err := c.deleteBuffer(b); err != nil {
			log.Printf("error deleting buffer for topic %q; this buffer is now orphaned: %v", id, err)
		}
	}
	return nil
}

func (c *Controller) handleDeleteTopic(req *http.Request) *router.Response {
	//
	id := mux.Vars(req)["topic"]
	err := c.deleteTopic(id)
	if err != nil {
		return &router.Response{Error: fmt.Errorf("error deleting topic: %v", err)}
	}
	return &router.Response{StatusCode: http.StatusOK}
}
