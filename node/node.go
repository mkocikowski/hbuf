package node

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"

	"github.com/gorilla/mux"
	"github.com/mkocikowski/hbuf/log"
	"github.com/mkocikowski/hbuf/stats"
	"github.com/mkocikowski/hbuf/tenant"
)

type Node struct {
	URL     string
	Path    string
	tenants map[string]*tenant.Tenant
	router  *mux.Router
}

func (n *Node) Init() *Node {
	n.tenants = make(map[string]*tenant.Tenant)
	n.router = mux.NewRouter()
	//
	n.router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		j, _ := json.Marshal(n.tenants)
		fmt.Fprintln(w, string(j))
	}).Methods("GET")
	//
	n.router.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, string(stats.Json()))
	}).Methods("GET")
	n.router.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		stats.Reset()
	}).Methods("DELETE")
	//
	log.DEBUG.Printf("starting node %q ...", n.URL)
	return n
}

func (n *Node) Load() {
	//
	files, _ := ioutil.ReadDir(filepath.Join(n.Path, "tenants"))
	for _, f := range files {
		log.DEBUG.Printf("loading data for tenant %q", f.Name())
		n.AddTenant(f.Name())
	}
}

func (n *Node) AddTenant(id string) (*tenant.Tenant, error) {
	//
	if _, ok := n.tenants[id]; ok {
		return nil, fmt.Errorf("tenant already exists")
	}
	t := &tenant.Tenant{
		ID:   id,
		URL:  n.URL + "/tenants/" + id,
		Path: filepath.Join(n.Path, "tenants", id),
	}
	if err := t.Init(n.router, ""); err != nil {
		log.WARN.Println(err)
		return nil, fmt.Errorf("error initializing tenant: %v", err)
	}
	n.tenants[t.ID] = t
	log.DEBUG.Printf("added tenant %q", t.ID)
	return t, nil
}

func (n *Node) Stop() {
	for _, t := range n.tenants {
		t.Stop()
	}
	log.DEBUG.Println("node stopped")
}

func (n *Node) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	n.router.ServeHTTP(w, req)
}
