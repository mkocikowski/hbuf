package segment

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mkocikowski/hbuf/message"
)

func TestMarshalUnmarshal(t *testing.T) {
	//m := &message.Message{ID: 1, TS: time.Now().UTC(), Type: "text/plain", Body: []byte("foo")}
	m := message.New("text/plain", []byte("foo"))
	b := marshal(m)
	log.Println(string(b))
	n, err := unmarshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(m.Body, n.Body) {
		t.Fatal("unserialized message body doesn't match serialized")
	}
	if m.TS != n.TS {
		t.Fatal("unserialized timestamp body doesn't match serialized")
	}
}

func TestReadWrite(t *testing.T) {

	dir, err := ioutil.TempDir("", "hbuf_")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	//defer os.RemoveAll(dir)

	s := &Segment{Path: filepath.Join(dir, "segment_0"), First: 5}
	s.Open()

	var m *message.Message
	ts := time.Now().UTC()

	tests := []struct {
		n int
		s string
	}{
		{5, "foo"},
		{6, "bar"},
		{7, "baz"},
	}
	for _, test := range tests {
		//m := &message.Message{TS: ts, Type: "text/plain", Body: []byte(test.s)}
		m := message.New("text/plain", []byte(test.s))
		m.TS = ts
		if err := s.Write(m); err != nil {
			t.Fatal(err)
		}
		if m.ID != test.n {
			t.Fatalf("unexpected sequence number: %v", m.ID)
		}
	}
	for i, test := range tests {
		m, err := s.Read(test.n)
		if err != nil {
			t.Fatalf("test %d: %v", i, err)
		}
		if bytes.Compare([]byte(test.s), m.Body) != 0 {
			t.Fatalf("read message not same as written: %q, %q", []byte(test.s), m.Body)
		}
	}
	s.Close()

	s = &Segment{Path: filepath.Join(dir, "segment_0")}
	s.Open()
	s.Close() // closing for writes
	if s.First != 5 {
		t.Fatalf("expected 5, got %d", s.First)
	}
	m, err = s.Read(6)
	if string(m.Body) != "bar" {
		t.Errorf("barf")
	}
	if m.TS != ts {
		t.Errorf("timestamps don't match: %v %v", ts, m.TS)
	}
	//m = &message.Message{Type: "text/plain", Body: []byte("baz")}
	m = message.New("text/plain", []byte("baz"))
	err = s.Write(m)
	if err != ErrorSegmentClosed {
		t.Fatal("expected error")
	}
	s.Close()

	s = &Segment{Path: filepath.Join(dir, "segment_0")}
	s.Open()
	m = message.New("text/plain", []byte("monkey"))
	err = s.Write(m)
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != 8 {
		t.Fatalf("unexpected sequence number: %v", m.ID)
	}
	if s.Len() != 4 {
		t.Fatalf("expected length 4, got: %d", s.Len())
	}

	m, err = s.Last()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.ID != 8 {
		t.Fatalf("unexpected sequence number: %v", m.ID)
	}

	m, err = s.Read(8)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.ID != 8 {
		t.Fatalf("unexpected sequence number: %v", m.ID)
	}

	m, err = s.Read(9)
	if err != ErrorOutOfBounds {
		t.Fatalf("unexpected error: %v", err)
	}

	err = s.verify()
	if err != nil {
		t.Fatal(err)
	}

	s.Close()
}

func TestRWParallel(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	dir, err := ioutil.TempDir("", "hbuf_")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	//defer os.RemoveAll(dir)

	s := &Segment{Path: filepath.Join(dir, "segment_0")}
	s.Open()

	N := 1 << 10

	for i := 0; i < N; i++ {
		//m := &message.Message{Type: "text/plain", Body: []byte(fmt.Sprintf("%d", i))}
		m := message.New("text/plain", []byte(fmt.Sprintf("%d", i)))
		if err := s.Write(m); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	done := make(chan bool)

	for p := 0; p < 10; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				//m := &message.Message{Type: "text/plain", Body: []byte(fmt.Sprintf("%d", i))}
				m := message.New("text/plain", []byte(fmt.Sprintf("%d", i)))
				if err := s.Write(m); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}

	for p := 0; p < 100; p++ {
		wg.Add(1)
		time.Sleep(5 * time.Millisecond)
		i := p
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				_, err := s.Read(rand.Intn(N))
				//log.Println(i, m)
				if err != nil {
					t.Errorf("[%d] error reading: %v", i, err)
					return
				}
			}
		}()
	}

	close(done)
	wg.Wait()

}

func BenchmarkWrite(b *testing.B) {

	dir, err := ioutil.TempDir("", "hbuf")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	defer os.RemoveAll(dir)

	s := &Segment{Path: filepath.Join(dir, "segment_0")}
	s.Open()
	defer s.Close()

	for i := 0; i < b.N; i++ {
		body := bytes.Repeat([]byte("x"), rand.Intn(1<<10))
		//m := &message.Message{TS: time.Now(), Type: "text/plain", Body: body}
		m := message.New("text/plain", body)
		err := s.Write(m)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRandomRead(b *testing.B) {

	dir, err := ioutil.TempDir("", "hbuf")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	defer os.RemoveAll(dir)

	s := &Segment{Path: filepath.Join(dir, "segment_0")}
	s.Open()
	defer s.Close()

	N := 10000
	for i := 0; i < N; i++ {
		body := bytes.Repeat([]byte("x"), rand.Intn(1<<10))
		//m := &message.Message{Type: "text/plain", Body: body}
		m := message.New("text/plain", body)
		s.Write(m)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := s.Read(rand.Intn(N))
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}

}

func BenchmarkSequentialRead(b *testing.B) {

	dir, err := ioutil.TempDir("", "hbuf")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	defer os.RemoveAll(dir)

	s := &Segment{Path: filepath.Join(dir, "segment_0")}
	s.Open()
	defer s.Close()

	N := 10000
	for i := 0; i < N; i++ {
		body := bytes.Repeat([]byte("x"), rand.Intn(1<<10))
		//m := &message.Message{Type: "text/plain", Body: body}
		m := message.New("text/plain", body)
		s.Write(m)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := s.Read(i % N)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}

}
