package segment

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/mkocikowski/hbuf/message"
)

const (
	DEFAULT_OFFSET_CACHE_SIZE = 1 << 10
)

var (
	DEBUG = log.New(os.Stderr, "[DEBUG] ", log.Lshortfile)
)

type Config struct {
	OffsetCacheSize int `json:"segment_offset_cache_size"`
}

type Segment struct {
	Path           string `json:"path"`
	FirstMessageId int    `json:"first_id"` // seq is the message sequential number, incremental, unique to a buffer across all segments
	MessageCount   int    `json:"count"`
	SizeB          int64  `json:"size_b"`
	writer         *os.File
	reader         *os.File
	lock           *sync.Mutex
	offsets        map[int]int64
	lru            []int
}

type Asc []*Segment

func (s Asc) Len() int           { return len(s) }
func (s Asc) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Asc) Less(i, j int) bool { return s[i].FirstMessageId < s[j].FirstMessageId }

func (s *Segment) Open(append bool) error {
	s.lock = new(sync.Mutex)
	s.offsets = make(map[int]int64)
	s.lru = make([]int, 0, DEFAULT_OFFSET_CACHE_SIZE)
	var err error
	if append {
		s.writer.Close()
		s.writer, err = os.OpenFile(s.Path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("error opening segment file for writing: %v", err)
		}
	}
	s.reader.Close()
	s.reader, err = os.Open(s.Path)
	if err != nil {
		return fmt.Errorf("error opening segment file for reading: %v", err)
	}
	// get id of first message
	m, err := s.read()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return fmt.Errorf("error opening segment file for reading: %v", err)
	}
	s.FirstMessageId = m.Id
	s.MessageCount = s.count()
	s.SizeB, _ = s.reader.Seek(0, 2)
	j, _ := json.Marshal(s)
	DEBUG.Println(string(j))
	return nil
}

func (s *Segment) Close() {
	s.writer.Close()
	s.writer = nil
}

func (s *Segment) count() int {
	s.reader.Seek(0, 0)
	var err error
	var i int
	for i = 0; ; i++ {
		if err = s.next(); err != nil {
			break
		}
	}
	return i
}

func (s *Segment) next() error {
	var len int64
	_, err := fmt.Fscanf(s.reader, "%08x", &len)
	if err != nil {
		return fmt.Errorf("error parsing message size: %v", err)
	}
	_, err = s.reader.Seek(len, 1)
	if err != nil {
		return fmt.Errorf("error seeking end of message: %v", err)
	}
	return nil
}

func (s *Segment) read() (*message.Message, error) {
	startOffset, _ := s.reader.Seek(0, 1)
	var recordLen, id, ts int64
	var typ string
	if _, err := fmt.Fscanf(s.reader, "%08x %d %d %q\n", &recordLen, &id, &ts, &typ); err != nil {
		return nil, err
	}
	currentOffset, _ := s.reader.Seek(0, 1)
	headLen := currentOffset - startOffset
	bodyLen := recordLen - (headLen - 8) // first 8 bytes is the record byte size
	m := &message.Message{
		Id:   int(id),
		Type: typ,
		Body: make([]byte, bodyLen),
	}
	if _, err := s.reader.Read(m.Body); err != nil {
		return nil, err
	}
	// TODO: check for empty message body?
	m.Body = m.Body[:len(m.Body)-1] // remove trailing newline
	return m, nil
}

func (s *Segment) seek(n int) error {
	if n < 0 {
		return ErrorOutOfBounds
	}
	if pos, ok := s.offsets[n]; ok {
		s.reader.Seek(pos, 0)
		return nil
	}
	var i int
	if pos, ok := s.offsets[n-1]; ok {
		s.reader.Seek(pos, 0)
		i = n - 1

	} else {
		s.reader.Seek(0, 0)
	}
	for ; i < n; i++ {
		if err := s.next(); err != nil {
			return err
		}
	}
	if len(s.lru) == DEFAULT_OFFSET_CACHE_SIZE {
		delete(s.offsets, s.lru[0])
		s.lru = s.lru[1:]
	}
	pos, _ := s.reader.Seek(0, 1)
	s.offsets[n] = pos
	s.lru = append(s.lru, n)
	return nil
}

func (s *Segment) Read(id int) (*message.Message, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	n := id - s.FirstMessageId
	if n >= s.MessageCount {
		return nil, ErrorOutOfBounds
	}
	if err := s.seek(n); err != nil {
		return nil, err
	}
	m, err := s.read()
	if err != nil {
		return nil, fmt.Errorf("error reading message from segment: %v", err)
	}
	return m, nil
}

var ErrorSegmentClosed = fmt.Errorf("segment closed")
var ErrorOutOfBounds = fmt.Errorf("message id out of segment bounds")

func (s *Segment) write(m *message.Message) error {
	body := fmt.Sprintf(" %d %d %q\n%s\n", m.Id, time.Now().UnixNano(), m.Type, string(m.Body))
	head := fmt.Sprintf("%08x", int32(len(body)))
	_, err := s.writer.WriteString(head + body)
	if err != nil {
		return err
	}
	s.SizeB += int64(len(head) + len(body))
	// skipping Sync() improves performance by order of magnitude
	return s.writer.Sync()
}

func (s *Segment) Write(m *message.Message) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.writer == nil {
		return ErrorSegmentClosed
	}
	m.Id = s.FirstMessageId + s.MessageCount
	if err := s.write(m); err != nil {
		return fmt.Errorf("error writing message to segment: %s", err)
	}
	s.MessageCount += 1
	return nil
}
