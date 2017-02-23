package buffer

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/mkocikowski/hbuf/message"
	"github.com/mkocikowski/hbuf/util"
)

func TestReplica(t *testing.T) {

	dir, err := ioutil.TempDir("", "hbuf")
	if err != nil {
		log.Fatal(err)
	}
	//log.Println(dir)
	defer os.RemoveAll(dir)

	// this is the local buffer which will have repliation set up
	b := &Buffer{ID: util.Uid(), Path: dir}
	if err := b.Init(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := &message.Message{Type: "text/plain", Body: []byte("foo")}
	if err := b.Write(m); err != nil {
		t.Fatal(err)
	}

	// this is the remote worker "running" the buffer to which data is replicated
	// it always reports the buffer length as 0
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"len":0}`)
	}))
	defer worker.Close()

	// the manager will give the replicator the information on where to find the remote buffer
	mux := http.NewServeMux()
	mux.HandleFunc("/manager/buffers/r1", func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprintf(w, `{"url":"`+worker.URL+`"}`)
	})
	manager := httptest.NewServer(mux)
	defer manager.Close()

	log.Println(worker.URL)

	// set up a replicator on the local buffer; it will query the manager about
	// the location of the remote buffer "r1"
	r := &replica{ID: "r1", manager: manager.URL + "/manager", buffer: b}
	r.Init()
	<-r.sync
	if r.Len() != 1 {
		t.Fatal("replica not working", r.Len())
	}

	r.Stop()
}
