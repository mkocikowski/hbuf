package buffer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/mkocikowski/hbuf/message"
	"github.com/mkocikowski/hbuf/segment"
	"github.com/mkocikowski/hbuf/stats"
)

type Consumer struct {
	ID string
	N  int
}

const (
	DefaultBufferMaxBytes     = 1 << 30 // 1GiB
	DefaultBufferMaxSegments  = 16
	DefaultMessageMaxBytes    = 1 << 24 // 16MiB
	DefaultSegmentMaxBytes    = 1 << 26 // 64MiB
	DefaultSegmentMaxMessages = 1 << 16 // number of messages impacts random seek time
)

type Config struct {
	BufferMaxBytes     int64 `json:"buffer_max_bytes"`
	BufferMaxSegments  int   `json:"buffer_max_segments"`
	MessageMaxBytes    int32 `json:"message_max_bytes"`
	SegmentMaxBytes    int64 `json:"segment_max_bytes"`
	SegmentMaxMessages int   `json:"segment_max_messages"`
}

func DefaultConfig() *Config {
	return &Config{
		BufferMaxBytes:     DefaultBufferMaxBytes,
		BufferMaxSegments:  DefaultBufferMaxSegments,
		MessageMaxBytes:    DefaultMessageMaxBytes,
		SegmentMaxBytes:    DefaultSegmentMaxBytes,
		SegmentMaxMessages: DefaultSegmentMaxMessages,
	}
}

type Buffer struct {
	*Config
	ID         string `json:"id"`
	URL        string `json:"url"`
	Tenant     string `json:"tenant"`
	Controller string `json:"-"`
	Path       string `json:"dir"`
	Len        int    `json:"len"`
	running    bool
	replicas   map[string]*replica
	consumers  map[string]*Consumer
	segments   []*segment.Segment
	lock       *sync.Mutex
}

func (b *Buffer) Init() error {
	//
	if b.Config == nil {
		b.Config = DefaultConfig()
	}
	b.lock = new(sync.Mutex)
	// TODO: is this needed?
	if err := os.MkdirAll(b.Path, 0755); err != nil {
		return fmt.Errorf("error creating buffer dir: %v", err)
	}
	if err := b.openSegments(); err != nil {
		return fmt.Errorf("error opening segments: %v", err)
	}
	if err := b.loadConsumers(); err != nil {
		return fmt.Errorf("error loading consumers: %v", err)
	}
	if err := b.loadReplicas(); err != nil {
		return fmt.Errorf("error loading replicas: %v", err)
	}
	b.running = true
	return nil
}

func (b *Buffer) openSegments() error {
	//
	b.segments = make([]*segment.Segment, 0)
	files, _ := ioutil.ReadDir(b.Path)
	segments := make([]string, 0, len(files))
	for _, f := range files {
		if !strings.HasPrefix(f.Name(), "segment_") {
			continue
		}
		segments = append(segments, f.Name())
	}
	sort.Strings(segments)
	for _, f := range segments {
		s := &segment.Segment{Path: filepath.Join(b.Path, f)}
		err := s.Open()
		if err != nil {
			return fmt.Errorf("error opening segment %q: %v", s.Path, err)
		}
		b.segments = append(b.segments, s)
		//b.Len += s.Count
		b.Len = s.First + s.Len()
	}
	// TODO: close all but the last segment for writing?
	// TODO: trip segments?
	return nil
}

func (b *Buffer) loadConsumers() error {
	//
	b.consumers = make(map[string]*Consumer)
	f := filepath.Join(b.Path, "offsets")
	d, err := ioutil.ReadFile(f)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("error reading consumer offsets: %v", err)
	}
	if err := json.Unmarshal(d, &b.consumers); err != nil {
		return fmt.Errorf("error parsing consumer offsets: %v", err)
	}
	return nil
}

func (b *Buffer) saveConsumers() error {
	//
	j, _ := json.Marshal(b.consumers)
	f := filepath.Join(b.Path, "offsets")
	if err := ioutil.WriteFile(f, j, 0644); err != nil {
		return fmt.Errorf("error saving consumer offsets: %v", err)
	}
	return nil
}

func (b *Buffer) Consumers() []byte {
	b.lock.Lock()
	defer b.lock.Unlock()
	j, _ := json.Marshal(b.consumers)
	return j
}

func (b *Buffer) Replicas() []string {
	//
	b.lock.Lock()
	defer b.lock.Unlock()
	replicas := make([]string, 0, len(b.replicas))
	for r, _ := range b.replicas {
		replicas = append(replicas, r)
	}
	return replicas
}

func (b *Buffer) SetReplicas(replicas []string) {
	//
	if b.replicas == nil {
		b.replicas = make(map[string]*replica)
	}
	for _, r := range replicas {
		if _, ok := b.replicas[r]; ok {
			continue
		}
		n := &replica{ID: r, manager: b.Controller, buffer: b}
		n.Init()
		b.lock.Lock()
		b.replicas[r] = n
		b.lock.Unlock()
		log.Printf("set replica %q for buffer %q", n.ID, b.ID)
	}
}

func (b *Buffer) loadReplicas() error {
	//
	f := filepath.Join(b.Path, "replicas")
	d, err := ioutil.ReadFile(f)
	switch {
	case os.IsNotExist(err):
		log.Printf("no replicas on disk for buffer %q", b.ID)
		return nil
	case err != nil:
		return fmt.Errorf("error reading buffer replicas: %v", err)
	}
	replicas := make([]string, 0)
	if err := json.Unmarshal(d, &replicas); err != nil {
		return fmt.Errorf("error parsing buffer replicas: %v", err)
	}
	b.SetReplicas(replicas)
	return nil
}

func (b *Buffer) saveReplicas() error {
	//
	replicas := b.Replicas()
	j, _ := json.Marshal(replicas)
	f := filepath.Join(b.Path, "replicas")
	if err := ioutil.WriteFile(f, j, 0644); err != nil {
		return fmt.Errorf("error saving buffer replicas: %v", err)
	}
	return nil
}

func (b *Buffer) Stop() {
	//
	b.lock.Lock()
	b.running = false
	for _, r := range b.replicas {
		r.Stop()
	}
	for _, s := range b.segments {
		s.Close()
	}
	b.lock.Unlock()
	if err := b.saveReplicas(); err != nil {
		log.Println(err)
	}
	if err := b.saveConsumers(); err != nil {
		log.Println(err)
	}
	log.Printf("buffer %q stopped; replicas: %v", b.ID, b.Replicas())
}

func (b *Buffer) Delete() error {
	b.Stop()
	return os.RemoveAll(b.Path)
}

// ---------------------------------------------------------------------

func (b *Buffer) trimSegments() error {
	//
	var s *segment.Segment
	for len(b.segments) > b.BufferMaxSegments {
		s, b.segments = b.segments[0], b.segments[1:]
		s.Close()
		if err := os.Remove(s.Path); err != nil {
			log.Printf("error removing segment file: %v", err)
		}
	}
	return nil
}

func (b *Buffer) addSegment() error {
	//
	if len(b.segments) > 0 {
		b.segments[len(b.segments)-1].Close()
	}
	f := filepath.Join(b.Path, fmt.Sprintf("segment_%08x", b.Len))
	s := &segment.Segment{Path: f, First: b.Len}
	err := s.Open()
	if err != nil {
		return fmt.Errorf("error creating segment: %v", err)
	}
	b.segments = append(b.segments, s)
	return nil
}

func (b *Buffer) getSegment() (*segment.Segment, error) {
	//
	if len(b.segments) == 0 {
		if err := b.addSegment(); err != nil {
			return nil, err
		}
	}
	s := b.segments[len(b.segments)-1]
	if s.Len() >= b.SegmentMaxMessages {
		if err := b.addSegment(); err != nil {
			return nil, err
		}
		s = b.segments[len(b.segments)-1]
	}
	if s.SizeB() >= b.SegmentMaxBytes {
		if err := b.addSegment(); err != nil {
			return nil, err
		}
		s = b.segments[len(b.segments)-1]
	}
	if err := b.trimSegments(); err != nil {
		return nil, err
	}
	return s, nil
}

func (b *Buffer) write(m *message.Message) error {
	//
	var err error
	b.lock.Lock()
	defer b.lock.Unlock()
	s, err := b.getSegment()
	if err != nil {
		return err
	}
	if err := s.Write(m); err != nil {
		return err
	}
	b.Len += 1
	// this signals to replicas that there is data to be syncd
	for _, r := range b.replicas {
		select {
		case r.data <- true:
		default:
		}
	}
	return nil
}

//func (b *Buffer) writer() {
//	log.Printf("started writer for buffer %q", b.ID)
//	for m := range b.data {
//		err := b.write(m)
//		if err != nil {
//			log.Println(err)
//			m.Error <- err
//		}
//		close(m.Error)
//	}
//	for _, r := range b.replicas {
//		r.Stop()
//	}
//}

func (b *Buffer) Write(m *message.Message) error {
	b.lock.Lock()
	running := b.running
	b.lock.Unlock()
	if !running {
		return fmt.Errorf("buffer closed")
	}
	err := b.write(m)
	return err
}

// ---------------------------------------------------------------------

func (b *Buffer) read(id int) (*message.Message, error) {
	if len(b.segments) == 0 {
		return nil, segment.ErrorOutOfBounds
	}
	var i int
	for j, s := range b.segments {
		// TODO: what if the segment containing the message has been deleted?
		if s.First > id {
			break
		}
		i = j
	}
	// can return segment.ErrorOutOfBounds
	return b.segments[i].Read(id)
}

func (b *Buffer) Read(id int) (*message.Message, error) {
	if !b.running {
		return nil, fmt.Errorf("buffer not running")
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.read(id)
}

func (b *Buffer) Consume(id string) (*message.Message, error) {
	if !b.running {
		return nil, fmt.Errorf("buffer not running")
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	c, ok := b.consumers[id]
	if !ok {
		c = &Consumer{ID: id}
		b.consumers[id] = c
	}
	m, err := b.read(c.N)
	if err == nil {
		c.N += 1
		// TODO: optimize this
		// cutting this out improves performance 100x
		b.saveConsumers()
		stats.Stats <- &stats.Stat{Name: "buffer_message_consume_n", Kind: stats.Counter, IntVal: 1}
		stats.Stats <- &stats.Stat{Name: "buffer_message_consume_b", Kind: stats.Counter, IntVal: len(m.Body)}
	}
	return m, err
}
