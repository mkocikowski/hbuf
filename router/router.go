package router

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

type HandlerFunc func(*http.Request) *Response

type Route struct {
	Path    string      `json:"path"`
	Methods []string    `json:"methods"`
	Handler HandlerFunc `json:"-"`
	Info    string      `json:"info"`
}

type Response struct {
	Body        []byte
	StatusCode  int
	Error       error
	ContentType string
}

func RegisterRoutes(router *mux.Router, base string, routes []*Route) {
	for _, r := range routes {
		r := r // see section 5.6.1 in "the go programming language" very important caveat
		f := func(w http.ResponseWriter, req *http.Request) {
			resp := r.Handler(req)
			if resp.Error != nil {
				if resp.StatusCode == 0 {
					resp.StatusCode = http.StatusInternalServerError
				}
				http.Error(w, resp.Error.Error(), resp.StatusCode)
				return
			}
			if resp.ContentType == "" {
				resp.ContentType = "application/json"
			}
			w.Header().Set("Content-Type", resp.ContentType)
			if resp.StatusCode == 0 {
				resp.StatusCode = http.StatusOK
			}
			w.WriteHeader(resp.StatusCode)
			_, err := w.Write(resp.Body)
			if err != nil {
				log.Printf("error sending response body to client: %v", err)
				// TODO: anything else in the way to cleanup or signaling to
				// the client? the response is partially sent at this point, so
				// can't change the status code / headers
			}
		}
		router.HandleFunc(base+r.Path, f).Methods(r.Methods...)
	}
}
