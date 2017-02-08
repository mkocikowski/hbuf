package node

import (
	"bytes"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

func TestNode(t *testing.T) {

	dir, err := ioutil.TempDir("", "hbuf")
	if err != nil {
		t.Fatal(err)
	}
	//log.Println(dir)
	defer os.RemoveAll(dir)
	node := &Node{Path: dir}
	server := httptest.NewServer(node)
	defer server.Close()
	node.URL = server.URL
	node.Init()

	tenant, err := node.AddTenant("-")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// don't allow duplicate tenants
	_, err = node.AddTenant("-")
	if err.Error() != "tenant already exists" {
		t.Fatalf("not the error i expected: %v", err)
	}
	resp, _ := http.Post(tenant.Client.URL+"/topics/foo", "text/plain", bytes.NewBufferString("bar"))
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		t.Fatalf("unexpected status code: (%d) %v", resp.StatusCode, string(b))
	}
	resp, _ = http.Get(tenant.Client.URL + "/topics/foo/next")
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		t.Fatalf("unexpected status code: (%d) %v", resp.StatusCode, string(b))
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if string(body) != "bar" {
		t.Fatalf("expected %q got %q", "foo", string(body))
	}

	node.Stop()
	server.Close()
	// make sure the server loads saved data
	node = &Node{Path: dir}
	server = httptest.NewServer(node)
	defer server.Close()
	node.URL = server.URL
	node.Init()
	node.Load()
	defer node.Stop()

	// tenant should have been loaded from disk
	tenant = node.tenants["-"]
	if tenant == nil {
		log.Printf("%#v", node.tenants)
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
	//
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	dir, err := ioutil.TempDir("", "hbuf")
	if err != nil {
		t.Fatal(err)
	}
	//log.Println(dir)
	defer os.RemoveAll(dir)
	node := &Node{Path: dir}
	server := httptest.NewServer(node)
	defer server.Close()
	node.URL = server.URL
	node.Init()
	defer node.Stop()

	tenant, err := node.AddTenant("-")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// https://github.com/golang/go/issues/16012#issuecomment-224948823
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 50000

	var readerWG sync.WaitGroup
	var writerWG sync.WaitGroup
	readerDone := make(chan bool)
	writerDone := make(chan bool)
	data := make(chan string)

	for i := 0; i < 5; i++ {
		writerWG.Add(1)
		time.Sleep(1 * time.Millisecond)
		go func() {
			defer writerWG.Done()
			for {
				select {
				case <-writerDone:
					return
				default:
				}
				resp, err := http.Post(
					tenant.Client.URL+"/topics/XXX",
					"text/plain",
					bytes.NewBufferString("bar"),
				)
				if err != nil {
					log.Println("!!!!!", err)
					return
				}
				body, _ := ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					log.Println("!!!!!", resp.StatusCode, string(body))
					return
				}
				//time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	log.Println("started all the writers")

	for i := 0; i < 5; i++ {
		readerWG.Add(1)
		time.Sleep(1 * time.Millisecond)
		go func() {
			defer readerWG.Done()
			for {
				resp, err := http.Get(tenant.Client.URL + "/topics/XXX/next")
				if err != nil {
					log.Println("!!!!!", err)
					return
				}
				body, err := ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					log.Println("!!!!!", err)
					continue
				}
				if resp.StatusCode == http.StatusNoContent {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				if resp.StatusCode != http.StatusOK {
					log.Println("!!!!!", resp.StatusCode, string(body))
					continue
				}
				select {
				case data <- string(body):
				case <-readerDone:
					return
				}
			}
		}()
	}
	log.Println("started all readers")

	for i := 0; i < 10000; i++ {
		s := <-data
		if s != "bar" {
			t.Fatalf("unexpected value: %q", s)
		}
		if n := i % 1000; n == 0 {
			log.Println(i)
		}
	}

	close(readerDone)
	readerWG.Wait()
	close(writerDone)
	writerWG.Wait()
}

func BenchmarkWrite(b *testing.B) {

	dir, err := ioutil.TempDir("", "hbuf")
	if err != nil {
		b.Fatal(err)
	}
	//log.Println(dir)
	defer os.RemoveAll(dir)
	node := &Node{Path: dir}
	server := httptest.NewServer(node)
	defer server.Close()
	node.URL = server.URL
	node.Init()
	defer node.Stop()
	tenant, err := node.AddTenant("-")
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	// https://github.com/golang/go/issues/16012#issuecomment-224948823
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 5000

	for i := 0; i < b.N; i++ {
		resp, err := http.Post(tenant.Client.URL+"/topics/XXX", "text/plain", bytes.NewBuffer([]byte("foo")))
		if err != nil {
			b.Fatal(err)
		}
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}

}
