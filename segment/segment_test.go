package segment

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mkocikowski/hbuf/message"
)

func TestMarshalUnmarshal(t *testing.T) {
	m := &message.Message{ID: 1, TS: time.Now().UTC(), Type: "text/plain", Body: []byte("foo")}
	b, err := marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	log.Println(string(b))
	n := new(message.Message)
	err = unmarshal(b, n)
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != n.ID {
		t.Fatal("unserialized id doesn't match serialized")
	}
	if m.TS != n.TS {
		t.Fatal("unserialized timestamp doesn't match serialized")
	}
	if m.Type != n.Type {
		t.Fatal("unserialized type doesn't match serialized")
	}
	if !bytes.Equal(m.Body, n.Body) {
		t.Fatal("unserialized message body doesn't match serialized")
	}
}

func BenchmarkMarshal(b *testing.B) {
	m := &message.Message{
		ID:   1,
		TS:   time.Now().UTC(),
		Type: "text/plain",
		Body: bytes.Repeat([]byte("x"), rand.Intn(1<<10)),
	}
	m.Sum(nil)
	for i := 0; i < b.N; i++ {
		_, err := marshal(m)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestReadWrite(t *testing.T) {

	dir, err := ioutil.TempDir("", "hbuf_")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	//defer os.RemoveAll(dir)

	s, err := New(dir, 5)
	if err != nil {
		t.Fatal(err)
	}
	path := s.Path

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
		m := &message.Message{ID: test.n, TS: ts, Type: "text/plain", Body: []byte(test.s)}
		if err := s.Write(m); err != nil {
			t.Fatal(err)
		}
	}
	for i, test := range tests {
		m, err := s.Read(i)
		if err != nil {
			t.Fatalf("test %d: %v", i, err)
		}
		if bytes.Compare([]byte(test.s), m.Body) != 0 {
			t.Fatalf("read message not same as written: %q, %q", []byte(test.s), m.Body)
		}
	}
	s.Close()

	var m *message.Message

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.First != 5 {
		t.Fatalf("expected 5, got %d", s.First)
	}
	m, err = s.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if string(m.Body) != "bar" {
		t.Errorf("barf")
	}
	if m.TS != ts {
		t.Errorf("timestamps don't match: %v %v", ts, m.TS)
	}
	s.Close()
	m = &message.Message{Type: "text/plain", Body: []byte("baz")}
	err = s.Write(m)
	if err != ErrorSegmentClosed {
		t.Fatal("expected error")
	}

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	m = &message.Message{ID: 99, Type: "text/plain", Body: []byte("monkey")}
	err = s.Write(m)
	if err != nil {
		t.Fatal(err)
	}
	if s.Len() != 4 {
		t.Fatalf("expected length 4, got: %d", s.Len())
	}
	m, err = s.Last()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// segment does no enforcement of message IDs
	if m.ID != 99 {
		t.Fatalf("unexpected sequence number: %v", m.ID)
	}
	m, err = s.Read(3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.ID != 99 {
		t.Fatalf("unexpected sequence number: %v", m.ID)
	}
	m, err = s.Read(4)
	if err != ErrorOutOfBounds {
		t.Fatalf("unexpected error: %v", err)
	}
	m, err = s.Read(-1)
	if err != ErrorOutOfBounds {
		t.Fatalf("unexpected error: %v", err)
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

	s, err := New(dir, 0)
	if err != nil {
		t.Fatal(err)
	}

	N := 1 << 10

	// first, write N messages so that consumers below don't throw errors
	for i := 0; i < N; i++ {
		m := &message.Message{Type: "text/plain", Body: []byte(fmt.Sprintf("%d", i))}
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
				m := &message.Message{Type: "text/plain", Body: []byte(fmt.Sprintf("%d", i))}
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

	s, err := New(dir, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	for i := 0; i < b.N; i++ {
		body := bytes.Repeat([]byte("x"), rand.Intn(1<<10))
		m := &message.Message{TS: time.Now(), Type: "text/plain", Body: body}
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

	s, err := New(dir, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	N := 10000
	for i := 0; i < N; i++ {
		body := bytes.Repeat([]byte("x"), rand.Intn(1<<10))
		m := &message.Message{Type: "text/plain", Body: body}
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

	s, err := New(dir, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	N := 10000
	for i := 0; i < N; i++ {
		body := bytes.Repeat([]byte("x"), rand.Intn(1<<10))
		m := &message.Message{Type: "text/plain", Body: body}
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
