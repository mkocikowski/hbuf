package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
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
)

type Buffer struct {
	Id  string `json:"id"`
	URL string `json:"url"`
}

type Topic struct {
	Id      string   `json:"id"`
	Buffers []string `json:"buffers"`
}

type Node struct {
	Id            string `json:"id"`
	Tenant        string `json:"tenant"`
	URL           string `json:"url"`
	ControllerURL string `json:"controller"`
}

type Client struct {
	Node
	Routes  []*router.Route `json:"routes"`
	topics  map[string]*Topic
	buffers map[string]*Buffer
	lock    *sync.Mutex
}

func NewClient(n *Node, r *mux.Router) *Client {
	c := &Client{
		Node:    *n,
		topics:  make(map[string]*Topic),
		buffers: make(map[string]*Buffer),
		lock:    new(sync.Mutex),
	}
	c.Routes = []*router.Route{
		{"/", []string{"GET"}, c.handleGetInfo, "show information about the node"},
		{"/topics", []string{"GET"}, c.handleGetTopics, "show topics"},
		{`/topics/{topic:[a-zA-Z0-9_\-]{1,256}}`, []string{"POST"}, c.handleWriteToTopic, "send message to topic, creating topic if necessary"},
		{`/topics/{topic:[a-zA-Z0-9_\-]{1,256}}`, []string{"DELETE"}, c.handleDeleteTopic, "delete topic and all its data"},
		{`/topics/{topic:[a-zA-Z0-9_\-]{1,256}}/next`, []string{"GET", "POST"}, c.handleConsumeFromTopic, "consume from topic; optional ?c= specifies consumer"},
		//{"/buffers", []string{"GET"}, c.handleGetBuffers, "show topic buffers"},
	}
	u, _ := url.Parse(c.URL)
	router.RegisterRoutes(r, u.Path, c.Routes)
	if c.Tenant == "-" {
		router.RegisterRoutes(r, "", c.Routes)
		u.Path = ""
		//DEBUG.Printf("client for default tenant registered at: %s", u)
	}
	return c
}

func (c *Client) handleGetInfo(req *http.Request) *router.Response {
	c.lock.Lock()
	j, _ := json.Marshal(c)
	c.lock.Unlock()
	return &router.Response{Body: j}
}

func (c *Client) getTopics() (map[string]*Topic, error) {
	resp, err := client.Get(c.ControllerURL + "/topics")
	if err != nil {
		return nil, fmt.Errorf("error getting topics: %v", err)
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error getting topics: %v", err)
	}
	topics := make(map[string]*Topic)
	err = json.Unmarshal(b, &topics)
	if err != nil {
		return nil, fmt.Errorf("error getting topics: %v", err)
	}
	return topics, nil
}

func (c *Client) getBuffers() (map[string]*Buffer, error) {
	resp, err := client.Get(c.ControllerURL + "/buffers")
	if err != nil {
		return nil, fmt.Errorf("error getting buffers: %v", err)
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error getting buffers: %v", err)
	}
	buffers := make(map[string]*Buffer)
	err = json.Unmarshal(b, &buffers)
	if err != nil {
		return nil, fmt.Errorf("error getting buffers: %v", err)
	}
	return buffers, nil
}

func (c *Client) updateMetadata() error {
	t, err := c.getTopics()
	if err != nil {
		return fmt.Errorf("error updating metadata: %v", err)
	}
	b, err := c.getBuffers()
	if err != nil {
		return fmt.Errorf("error updating metadata: %v", err)
	}
	c.lock.Lock()
	c.topics = t
	c.buffers = b
	c.lock.Unlock()
	return nil
}

func (c *Client) handleGetTopics(req *http.Request) *router.Response {
	if err := c.updateMetadata(); err != nil {
		return &router.Response{Error: fmt.Errorf("error getting topics: %v", err), StatusCode: http.StatusInternalServerError}
	}
	c.lock.Lock()
	t := make([]string, 0, len(c.topics))
	for k, _ := range c.topics {
		t = append(t, k)
	}
	c.lock.Unlock()
	j, _ := json.Marshal(t)
	//	topics := make(map[string][]string)
	//	for id, t := range c.topics {
	//		topics[id] = t.Buffers
	//	}
	//	j, _ := json.Marshal(topics)
	//j, _ := json.Marshal(c.topics)
	return &router.Response{Body: j}
}

func (c *Client) createTopic(id string) error {
	resp, err := client.Post(c.ControllerURL+"/topics/"+id, "application/json", nil)
	if err != nil {
		return fmt.Errorf("error creating topic: %v", err)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	switch resp.StatusCode {
	case http.StatusCreated, http.StatusConflict:
		return nil
	}
	return fmt.Errorf("error creating topic: (%d) %v", resp.StatusCode, string(body))
}

func (c *Client) handleWriteToTopic(req *http.Request) *router.Response {
	topic := mux.Vars(req)["topic"]
	if !util.TopicNameRE.MatchString(topic) {
		return &router.Response{Error: fmt.Errorf("topic name must match %q", util.TopicNameRE), StatusCode: http.StatusBadRequest}
	}
	c.lock.Lock()
	t, ok := c.topics[topic]
	c.lock.Unlock()
	// create topic if needed
	if !ok {
		if err := c.createTopic(topic); err != nil {
			return &router.Response{Error: fmt.Errorf("error writing to topic: %v", err), StatusCode: http.StatusInternalServerError}
		}
		err := c.updateMetadata()
		if err != nil {
			return &router.Response{Error: fmt.Errorf("error writing to topic: %v", err), StatusCode: http.StatusInternalServerError}
		}
		c.lock.Lock()
		t, ok = c.topics[topic]
		c.lock.Unlock()
		if !ok {
			return &router.Response{Error: fmt.Errorf("error writing to topic: couldn't create topic"), StatusCode: http.StatusInternalServerError}
		}
	}
	// make a local copy of buffers
	c.lock.Lock()
	buffers := make([]*Buffer, 0, len(t.Buffers))
	for _, id := range t.Buffers {
		if b, ok := c.buffers[id]; ok {
			buffers = append(buffers, b)
		}
	}
	c.lock.Unlock()
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		DEBUG.Printf("couldn't read message body: %v", err)
		return &router.Response{Error: fmt.Errorf("error writing to topic: couldn't read message body: %v", err), StatusCode: http.StatusInternalServerError}
	}
	n := rand.Intn(len(buffers))
	for i := n; i < n+len(buffers); i++ {
		b := buffers[i%len(buffers)]
		//dump, _ := httputil.DumpRequest(req, true)
		//INFO.Println(string(dump))
		//resp, err := http.Post(b.URL, req.Header.Get("Content-Type"), req.Body)
		resp, err := client.Post(b.URL, req.Header.Get("Content-Type"), bytes.NewBuffer(data))
		if err != nil {
			DEBUG.Println(err)
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			DEBUG.Printf("couldn't read response from worker for buffer: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return &router.Response{StatusCode: http.StatusOK}
		}
		DEBUG.Printf("error response when writing to buffer %q for topic %q: %v", b.Id, topic, string(body))
	}
	return &router.Response{Error: fmt.Errorf("error writing to topic: couldn't write to any buffer %v", t.Buffers), StatusCode: http.StatusInternalServerError}
}

func (c *Client) handleConsumeFromTopic(req *http.Request) *router.Response {
	topics := strings.Split(mux.Vars(req)["topic"], ",")
	for _, t := range topics {
		if !util.TopicNameRE.MatchString(t) {
			return &router.Response{Error: fmt.Errorf("topic name must match %q", util.TopicNameRE), StatusCode: http.StatusBadRequest}
		}
	}
	// TODO: this is a huge performance hit; metadata should be upated concurrently, not per request
	if err := c.updateMetadata(); err != nil {
		return &router.Response{Error: fmt.Errorf("error consuming: %v", err), StatusCode: http.StatusInternalServerError}
	}
	// make a local copy of buffers
	c.lock.Lock()
	buffers := make([]*Buffer, 0)
	for _, id := range topics {
		if t, ok := c.topics[id]; ok {
			for _, id := range t.Buffers {
				if b, ok := c.buffers[id]; ok {
					buffers = append(buffers, b)
				}
			}
		}
	}
	c.lock.Unlock()
	if len(buffers) == 0 {
		//WARN.Printf("no buffers for topic[s] %q found", mux.Vars(req)["topic"])
		return &router.Response{StatusCode: http.StatusNoContent}
	}
	consumer := req.URL.Query().Get("c")
	if consumer == "" {
		consumer = "-"
	}
	if !util.TopicNameRE.MatchString(consumer) {
		return &router.Response{Error: fmt.Errorf("invalid consumer name"), StatusCode: http.StatusBadRequest}
	}
	n := rand.Intn(len(buffers))
	for i := n; i < n+len(buffers); i++ {
		b := buffers[i%len(buffers)]
		url := b.URL + "/consumers/" + consumer + "/_next"
		//DEBUG.Println(url)
		resp, err := client.Post(url, "", nil)
		if err != nil {
			return &router.Response{Error: fmt.Errorf("error consuming: %v", err), StatusCode: http.StatusInternalServerError}
		}
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			if err != nil {
				return &router.Response{Error: fmt.Errorf("error consuming: %v", err), StatusCode: http.StatusInternalServerError}
			}
			return &router.Response{Body: body, ContentType: resp.Header.Get("Content-Type")}
		}
	}
	return &router.Response{StatusCode: http.StatusNoContent}
}

func (c *Client) handleDeleteTopic(req *http.Request) *router.Response {
	t := mux.Vars(req)["topic"]
	r, _ := http.NewRequest("DELETE", c.ControllerURL+"/topics/"+t, nil)
	resp, err := client.Do(r)
	if err != nil {
		return &router.Response{Error: fmt.Errorf("error deleting topic: %v", err), StatusCode: http.StatusInternalServerError}
	}
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return &router.Response{StatusCode: http.StatusOK}
	}
	return &router.Response{
		Error:      fmt.Errorf("error deleting topic: %d %v", resp.StatusCode, string(body)),
		StatusCode: http.StatusInternalServerError,
	}
}

//func (c *Client) handleGetBuffers(req *http.Request) *router.Response {
//	if err := c.updateMetadata(); err != nil {
//		return &router.Response{Error: fmt.Errorf("error getting buffers: %v", err), StatusCode: http.StatusInternalServerError}
//	}
//	j, _ := json.Marshal(c.buffers)
//	return &router.Response{Body: j}
//}
