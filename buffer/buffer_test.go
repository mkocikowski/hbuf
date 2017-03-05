package buffer

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"testing"

	"github.com/mkocikowski/hbuf/message"
	"github.com/mkocikowski/hbuf/segment"
	"github.com/mkocikowski/hbuf/util"
)

func TestBufferBasics(t *testing.T) {

	dir, err := ioutil.TempDir("", "hbuf")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	//defer os.RemoveAll(dir)

	uid := util.Uid()
	b := &Buffer{ID: uid, Path: dir}
	err = b.Init()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = b.Read(0)
	if err == nil {
		t.Errorf("expected error, didn't get it")
	}
	_, err = b.Consume("-")
	if err == nil {
		t.Errorf("expected error, didn't get it")
	}

	m := &message.Message{Type: "text/plain", Body: []byte("foo")}
	if err := b.Write(m); err != nil {
		t.Fatal(err)
	}

	x, err := b.Read(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(x.Body) != string(m.Body) {
		t.Error("read message not same as written")
	}
	_, err = b.Read(1)
	if err == nil {
		t.Errorf("expected error, didn't get it")
	}
	x, err = b.Consume("-")
	if string(x.Body) != string(m.Body) {
		t.Error("read message not same as written")
	}
	x, err = b.Consume("-")
	if err == nil {
		t.Errorf("expected error, didn't get it")
	}

	b.Stop()

	b = &Buffer{ID: uid, Path: dir}
	if err := b.Init(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.Len != 1 {
		t.Fatalf("expected Len==1, got: %v", b.Len)
	}

	x, err = b.Consume("xxx")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(x.Body) != string(m.Body) {
		t.Error("read message not same as written")
	}
	if x.ID != 0 {
		t.Fatalf("expected message id==0, got: %v", x.ID)
	}
	m = &message.Message{Type: "text/plain", Body: []byte("monkey")}
	if err := b.Write(m); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.ID != 1 {
		t.Fatalf("expected message id==1, got: %v", m.ID)
	}

}

// test creating a buffer that doesn't start at 0 (say the early segments have
// been trimmed, or maybe this is a replica which didn't start at 0
func TestBufferOffset(t *testing.T) {

	dir, err := ioutil.TempDir("", "hbuf")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	//defer os.RemoveAll(dir)

	uid := util.Uid()
	b := &Buffer{ID: uid, Len: 10, Path: dir}
	err = b.Init()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m := &message.Message{Type: "text/plain", Body: []byte("foo")}
	if err := b.Write(m); err != nil {
		t.Fatal(err)
	}
	if m.ID != 10 {
		t.Fatalf("expected id 10, got: %v", m.ID)
	}
	if b.Len != 11 {
		t.Fatalf("expected buffer len 11, got: %v", b.Len)
	}
	x, err := b.Read(10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(x.Body) != string(m.Body) {
		t.Error("read message not same as written")
	}
	x, err = b.Consume("-")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(x.Body) != string(m.Body) {
		t.Error("read message not same as written")
	}
	b.Stop()

	b = &Buffer{ID: uid, Path: dir}
	if err := b.Init(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.Len != 11 {
		t.Fatalf("expected Len==1, got: %v", b.Len)
	}
	b.Stop()
}

func TestRotateSegments(t *testing.T) {

	dir, err := ioutil.TempDir("", "hbuf")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	//defer os.RemoveAll(dir)

	uid := util.Uid()
	b := &Buffer{ID: uid, Path: dir}
	err = b.Init()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	b.SegmentMaxMessages = 2
	b.BufferMaxSegments = 2

	for i := 0; i < 5; i++ {
		m := &message.Message{Type: "text/plain", Body: []byte(fmt.Sprintf("foo-%d", i))}
		err = b.Write(m)
	}
	_, err = b.Read(0)
	if err != segment.ErrorOutOfBounds {
		t.Fatalf("not the error i expected: %v", err)
	}
	m, err := b.Read(4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(m.Body) != "foo-4" {
		t.Fatalf("unexpected body: %v", string(m.Body))
	}

	b.Stop()

	b = &Buffer{ID: uid, Path: dir}
	b.Init()

	_, err = b.Read(0)
	if err != segment.ErrorOutOfBounds {
		t.Fatalf("not the error i expected")
	}
	m, err = b.Read(4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(m.Body) != "foo-4" {
		t.Fatalf("unexpected body: %v", string(m.Body))
	}

}

func BenchmarkSaveConsumers(b *testing.B) {

	dir, err := ioutil.TempDir("", "hbuf")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	//defer os.RemoveAll(dir)
	uid := util.Uid()
	buffer := &Buffer{ID: uid, Path: dir}
	err = buffer.Init()
	if err != nil {
		b.Errorf("unexpected error: %v", err)
	}

	buffer.consumers["foo"] = &Consumer{"foo", 1}
	for i := 0; i < b.N; i++ {
		buffer.consumers["foo"].N += 1
		buffer.saveConsumers()
	}

}

func BenchmarkConsume(b *testing.B) {

	dir, err := ioutil.TempDir("", "hbuf")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	//defer os.RemoveAll(dir)

	uid := util.Uid()
	buffer := &Buffer{ID: uid, Path: dir}
	err = buffer.Init()
	if err != nil {
		b.Errorf("unexpected error: %v", err)
	}

	N := 10000
	for i := 0; i < N; i++ {
		body := bytes.Repeat([]byte("x"), rand.Intn(1<<10))
		m := &message.Message{Type: "text/plain", Body: body}
		if err := buffer.Write(m); err != nil {
			log.Println(err)
		}
	}

	b.ResetTimer()

	c := 0
	for i := 0; i < b.N; i++ {
		_, err := buffer.Consume("c-" + string(c))
		if err == segment.ErrorOutOfBounds {
			c += 1
			log.Println("consumer exhausted, starting new consumer:", c)
			continue
		}
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}

}
