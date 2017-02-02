package tenant

import (
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/gorilla/mux"
	"github.com/mkocikowski/hbuf/client"
	"github.com/mkocikowski/hbuf/controller"
	"github.com/mkocikowski/hbuf/log"
	"github.com/mkocikowski/hbuf/router"
	"github.com/mkocikowski/hbuf/util"
	"github.com/mkocikowski/hbuf/worker"
)

type Tenant struct {
	ID      string                 `json:"-"`
	URL     string                 `json:"-"`
	Path    string                 `json:"-"`
	Manager *controller.Controller `json:"manager"`
	Worker  *worker.Worker         `json:"worker"`
	Client  *client.Client         `json:"client"`
}

func (t *Tenant) Init(r *mux.Router, cURL string) error {
	var id string
	var u *url.URL
	//
	id = util.Uid()
	m := &controller.Controller{
		ID:     id,
		Tenant: t.ID,
		//URL:    t.URL + "/nodes/" + id,
		URL:  t.URL + "/manager",
		Path: filepath.Join(t.Path, "manager"),
	}
	if _, err := m.Init(); err != nil {
		return fmt.Errorf("error initializing manager for tenant %q: %v", t.ID, err)
	}
	u, _ = url.Parse(m.URL)
	router.RegisterRoutes(r, u.Path, m.Routes())
	t.Manager = m
	log.DEBUG.Printf("registered manager %q for tenant %q", m.ID, t.ID)
	cURL = m.URL
	//
	id = util.Uid()
	w := &worker.Worker{
		ID: id,
		//URL:        t.URL + "/nodes/" + id,
		URL:        t.URL + "/worker",
		Tenant:     t.ID,
		Controller: cURL,
		Path:       filepath.Join(t.Path, "worker"),
	}
	if _, err := w.Init(); err != nil {
		return fmt.Errorf("error initializing worker for tenant %q: %v", t.ID, err)
	}
	u, _ = url.Parse(w.URL)
	router.RegisterRoutes(r, u.Path, w.Routes())
	t.Worker = w
	log.DEBUG.Printf("registered worker %q for tenant %q", w.ID, t.ID)
	//
	id = util.Uid()
	c := &client.Client{
		ID:         id,
		URL:        t.URL + "/nodes/" + id,
		Tenant:     t.ID,
		Controller: cURL,
	}
	c.Init()
	u, _ = url.Parse(c.URL)
	router.RegisterRoutes(r, u.Path, c.Routes())
	if t.ID == "-" {
		// if this is default tenant, register client also on the / route
		router.RegisterRoutes(r, "", c.Routes())
	}
	t.Client = c
	log.DEBUG.Printf("registered client %q for tenant %q", c.ID, t.ID)
	//
	log.DEBUG.Printf("tenant %q initialized", t.ID)
	return nil
}

func (t *Tenant) Stop() {
	t.Worker.Stop()
	t.Manager.Stop()
	log.DEBUG.Printf("tenant %q stopped", t.ID)
}
