package buffer

import (
	"encoding/json"
	"fmt"
	"io"
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
	Id string
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
	Id        string `json:"id"`
	Tenant    string `json:"tenant"`
	URL       string `json:"url"`
	Dir       string `json:"dir"`
	Len       int    `json:"len"`
	running   bool
	consumers map[string]*Consumer
	segments  []*segment.Segment
	lock      *sync.Mutex
}

func (b *Buffer) Init() error {
	if b.Config == nil {
		b.Config = DefaultConfig()
	}
	b.lock = new(sync.Mutex)
	if err := os.MkdirAll(b.Dir, 0755); err != nil {
		return fmt.Errorf("error creating buffer dir: %v", err)
	}
	if err := b.openSegments(); err != nil {
		return fmt.Errorf("error opening segments: %v", err)
	}
	b.consumers = make(map[string]*Consumer)
	b.loadConsumers()
	b.running = true
	//log.Printf("buffer %q running", b.Dir)
	return nil
}

func (b *Buffer) addSegment() (*segment.Segment, error) {
	if len(b.segments) > 0 {
		b.segments[len(b.segments)-1].Close()
	}
	s := &segment.Segment{
		Path:           filepath.Join(b.Dir, fmt.Sprintf("segment_%08x", b.Len)),
		FirstMessageId: b.Len,
	}
	err := s.Open(true)
	if err != nil {
		return nil, fmt.Errorf("error creating segment: %v", err)
	}
	b.segments = append(b.segments, s)
	return s, nil
}

func (b *Buffer) trimSegments() error {
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

func (b *Buffer) openSegments() error {
	b.segments = make([]*segment.Segment, 0)
	files, _ := ioutil.ReadDir(b.Dir)
	for _, f := range files {
		if !strings.HasPrefix(f.Name(), "segment_") {
			continue
		}
		s := &segment.Segment{Path: filepath.Join(b.Dir, f.Name())}
		err := s.Open(false)
		if err != nil {
			return fmt.Errorf("error opening segment: %v", err)
		}
		b.segments = append(b.segments, s)
		b.Len = s.FirstMessageId + s.MessageCount
	}
	// make the last segment the append-to segment
	if len(b.segments) > 0 {
		sort.Sort(segment.Asc(b.segments))
		b.segments[len(b.segments)-1].Open(true)
	}
	if err := b.trimSegments(); err != nil {
		return err
	}
	return nil
}

func (b *Buffer) loadConsumers() error {
	d, err := ioutil.ReadFile(filepath.Join(b.Dir, "offsets"))
	if err != nil {
		return fmt.Errorf("error reading buffer offsets: %v", err)
	}
	if err := json.Unmarshal(d, &b.consumers); err != nil {
		return fmt.Errorf("error parsing buffer offsets: %v", err)
	}
	return nil
}

func (b *Buffer) saveConsumers() error {
	j, _ := json.Marshal(b.consumers)
	if err := ioutil.WriteFile(filepath.Join(b.Dir, "offsets"), j, 0644); err != nil {
		return fmt.Errorf("error saving buffer offsets: %v", err)
	}
	//log.Printf("saved consumers: %v", b.Dir)
	return nil
}

func (b *Buffer) GetConsumers() []byte {
	b.lock.Lock()
	defer b.lock.Unlock()
	j, _ := json.Marshal(b.consumers)
	return j
}

func (b *Buffer) Stop() error {
	b.running = false
	b.lock.Lock()
	defer b.lock.Unlock()
	for _, s := range b.segments {
		s.Close()
	}
	return nil
}

func (b *Buffer) Delete() error {
	b.Stop()
	return os.RemoveAll(b.Dir)
}

func (b *Buffer) Write(m *message.Message) error {
	if len(m.Type) > 64 {
		return fmt.Errorf("content type string longer than 64 bytes")
	}
	if !b.running {
		return fmt.Errorf("buffer not running")
	}
	var err error
	b.lock.Lock()
	defer b.lock.Unlock()
	if len(b.segments) == 0 {
		if _, err := b.addSegment(); err != nil {
			return err
		}
	}
	s := b.segments[len(b.segments)-1]
	if s.MessageCount >= b.SegmentMaxMessages || s.SizeB >= b.SegmentMaxBytes {
		if s, err = b.addSegment(); err != nil {
			return err
		}
	}
	if err := s.Write(m); err != nil {
		return err
	}
	stats.Stats <- &stats.Stat{Name: "buffer_message_write_n", Kind: stats.Counter, IntVal: 1}
	stats.Stats <- &stats.Stat{Name: "buffer_message_write_b", Kind: stats.Counter, IntVal: len(m.Body)}
	b.Len += 1
	if err := b.trimSegments(); err != nil {
		return err
	}
	return nil
}

func (b *Buffer) read(id int) (*message.Message, error) {
	if len(b.segments) == 0 {
		return nil, io.EOF
	}
	var i int
	for j, s := range b.segments {
		if s.FirstMessageId > id {
			break
		}
		i = j
	}
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
		c = &Consumer{Id: id}
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
