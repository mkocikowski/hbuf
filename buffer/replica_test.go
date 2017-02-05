package buffer

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mkocikowski/hbuf/message"
)

func TestReplica(t *testing.T) {

	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer worker.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/manager/buffers/r1", func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprintf(w, `{"url":"`+worker.URL+`"}`)
	})
	manager := httptest.NewServer(mux)
	defer manager.Close()

	r := &Replica{ID: "r1", Controller: manager.URL + "/manager"}
	r.Init()

	m := &message.Message{Type: "text/plain", Body: []byte("foo")}
	r.data <- m
	if err := <-m.Error; err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	r.Stop()
}
