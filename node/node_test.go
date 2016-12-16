package node

import (
	"bytes"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

//func TestCluster(t *testing.T) {
//
//	s1 := NewServer("")
//	ts1 := httptest.NewServer(s1)
//	defer ts1.Close()
//	s1.URL = ts1.URL
//	te1, _ := s1.AddTenant("default", "")
//	log.Printf("%#v", te1.Controller)
//
//	s2 := NewServer("")
//	ts2 := httptest.NewServer(s2)
//	defer ts2.Close()
//	s2.URL = ts2.URL
//	te2, _ := s2.AddTenant("default", ts1.URL)
//	log.Printf("%#v", te2.Controller)
//
//}

func TestNode(t *testing.T) {
	log.SetFlags(log.Lshortfile)

	dir, err := ioutil.TempDir("", "hbuf")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	//defer os.RemoveAll(dir)

	n := &Node{Dir: dir}
	ts := httptest.NewServer(n)
	defer ts.Close()
	n.URL = ts.URL
	n.Init()

	tenant, err := n.AddTenant("-", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// don't allow duplicates
	_, err = n.AddTenant("-", "")
	if err == nil {
		t.Fatalf("expected error, didn't get it")
	}

	resp, _ := http.Post(tenant.Client.URL+"/topics/foo", "text/plain", bytes.NewBufferString("bar"))
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		log.Println(string(b))
		t.Fatalf("unexpected status code: %d", resp.StatusCode)
	}

	resp, _ = http.Get(tenant.Client.URL + "/topics/foo/next")
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		log.Println(string(b))
		t.Fatalf("unexpected status code: %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if string(body) != "bar" {
		t.Fatalf("expected %q got %q", "foo", string(body))
	}

	n.Stop()
	ts.Close()

	// make sure the server loads saved data
	n = &Node{Dir: dir}
	ts = httptest.NewServer(n)
	defer ts.Close()
	n.URL = ts.URL
	n.Init()
	n.Load()

	// don't allow duplicates; tenant should have been loaded from disk
	_, err = n.AddTenant("-", "")
	if err == nil {
		t.Fatalf("expected error, didn't get it")
	}
	log.Println(err)

	tenant = n.tenants["-"]

	if tenant == nil {
		log.Printf("%#v", n.tenants)
		t.Fatalf("tenant is unexpectedly nil")
	}

	resp, err = http.Get(tenant.Client.URL + "/topics/foo/next")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status code: %d", resp.StatusCode)
	}

	resp, _ = http.Get(tenant.Client.URL + "/topics/foo/next?c=baz")
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		log.Println(string(b))
		t.Fatalf("unexpected status code: %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, _ = ioutil.ReadAll(resp.Body)
	if string(body) != "bar" {
		t.Fatalf("expected %q got %q", "foo", string(body))
	}

	resp, err = http.Get(tenant.Client.URL + "/topics/XXX/next")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status code: %d", resp.StatusCode)
	}

}

func TestParallel(t *testing.T) {
	log.SetFlags(log.Lshortfile)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	dir, err := ioutil.TempDir("", "hbuf")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	//defer os.RemoveAll(dir)

	n := &Node{Dir: dir}
	ts := httptest.NewServer(n)
	defer ts.Close()
	n.URL = ts.URL
	n.Init()

	tenant, err := n.AddTenant("-", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// https://github.com/golang/go/issues/16012#issuecomment-224948823
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 500
	var wg sync.WaitGroup
	done := make(chan bool)
	data := make(chan string)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		time.Sleep(1 * time.Millisecond)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					//log.Println("done")
					return
				default:
				}
				resp, err := http.Post(tenant.Client.URL+"/topics/XXX", "text/plain", bytes.NewBufferString("bar"))
				if err != nil {
					log.Println(err)
					continue
				}
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					log.Println(err)
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					log.Println(resp.StatusCode, string(body))
				}
				//time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		time.Sleep(1 * time.Millisecond)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					//log.Println("done")
					return
				default:
				}
				resp, err := http.Get(tenant.Client.URL + "/topics/XXX/next")
				if err != nil {
					log.Printf("unexpected error: %v", err)
					continue
				}
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					log.Printf("unexpected error: %v", err)
				}
				resp.Body.Close()
				if resp.StatusCode == http.StatusNoContent {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				if resp.StatusCode == http.StatusOK {
					//log.Printf("data: %q", string(body))
					select {
					case data <- string(body):
					case <-done:
						//log.Println("done")
						return
					}
					resp.Body.Close()
				}
			}
		}()

	}

	for i := 0; i < 10000; i++ {
		s := <-data
		//log.Println(s)
		if s != "bar" {
			log.Printf("unexpected value: %q", s)
		}
	}

	log.Println("closing...")
	close(done)
	wg.Wait()

}
