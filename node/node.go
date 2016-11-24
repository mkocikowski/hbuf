package node

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gorilla/mux"
	"github.com/mkocikowski/hbuf/client"
	"github.com/mkocikowski/hbuf/controller"
	"github.com/mkocikowski/hbuf/stats"
	"github.com/mkocikowski/hbuf/util"
	"github.com/mkocikowski/hbuf/worker"
)

var (
	INFO = log.New(os.Stderr, "[INFO] ", 0)
)

type Tenant struct {
	Id         string
	URL        string
	Controller *controller.Controller
	Worker     *worker.Worker
	Client     *client.Client
	Dir        string
	router     *mux.Router
}

func (t *Tenant) addController() error {
	i := util.Uid()
	n := &controller.Node{
		Id:            i,
		Tenant:        t.Id,
		URL:           t.URL + "/nodes/" + i,
		ControllerURL: t.URL + "/nodes/" + i,
		Dir:           t.Dir,
	}
	c, err := controller.NewController(n, t.router)
	if err != nil {
		return fmt.Errorf("error creating controller: %v", err)
	}
	t.Controller = c
	return nil
}

func (t *Tenant) addWorker() error {
	i := util.Uid()
	n := &worker.Node{
		Id:            i,
		Tenant:        t.Id,
		URL:           t.URL + "/nodes/" + i,
		ControllerURL: t.Controller.URL,
		Dir:           t.Dir,
	}
	w, err := worker.NewWorker(n, t.router)
	if err != nil {
		return fmt.Errorf("error creating worker: %v", err)
	}
	t.Worker = w
	return nil
}

func (t *Tenant) addClient() error {
	i := util.Uid()
	c := &client.Node{
		Id:            i,
		Tenant:        t.Id,
		URL:           t.URL,
		ControllerURL: t.Controller.URL,
	}
	t.Client = client.NewClient(c, t.router)
	return nil
}

func (t *Tenant) Stop() {
	t.Worker.Stop()
	t.Controller.Stop()
}

type Node struct {
	URL     string
	Dir     string
	tenants map[string]*Tenant
	router  *mux.Router
}

func (n *Node) Init() *Node {
	n.tenants = make(map[string]*Tenant)
	n.router = mux.NewRouter()
	n.router.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, string(stats.Json()))
	})
	return n
}

func (n *Node) Load() {
	files, _ := ioutil.ReadDir(filepath.Join(n.Dir, "tenants"))
	for _, f := range files {
		n.AddTenant(f.Name(), "")
	}
}

func (n *Node) AddTenant(id string, seed string) (*Tenant, error) {
	if _, ok := n.tenants[id]; ok {
		return nil, fmt.Errorf("tenant %q already exists", id)
	}
	t := &Tenant{
		Id:     id,
		URL:    n.URL + "/tenants/" + id,
		Dir:    filepath.Join(n.Dir, "tenants", id),
		router: n.router,
	}
	if err := t.addController(); err != nil {
		return nil, err
	}
	//log.Println("c:", t.Controller.URL)
	if err := t.addWorker(); err != nil {
		return nil, err
	}
	t.addClient()
	n.tenants[t.Id] = t
	//j, _ := json.Marshal(t)
	//log.Println(string(j))
	INFO.Println("running.")
	return t, nil
}

func (n *Node) Stop() {
	for _, t := range n.tenants {
		t.Stop()
	}
}

func (n *Node) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	n.router.ServeHTTP(w, req)
}
