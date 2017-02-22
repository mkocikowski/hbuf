// Segment persists buffer data to disk.
// There can be many segments per buffer; they can be rotated. The segment doesn't "understand" the messages' IDs or SHAs: the buffer is responsible for managing and validating these. For any segment, the first message is 0.
package segment

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/mkocikowski/hbuf/message"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

const (
	DEFAULT_OFFSET_CACHE_SIZE = 1 << 10
)

var (
	ErrorSegmentClosed = fmt.Errorf("segment closed")
	ErrorOutOfBounds   = fmt.Errorf("message id out of segment bounds")
)

type Config struct {
	OffsetCacheSize int `json:"segment_offset_cache_size"`
}

type Segment struct {
	Path      string
	First     int
	len       int
	lenLock   *sync.Mutex
	sizeB     int64
	sizeBLock *sync.Mutex
	writer    *os.File
	reader    *os.File
	lock      *sync.Mutex
	offsets   map[int]int64
	lru       []int
}

func (s *Segment) init() *Segment {
	s.lenLock = new(sync.Mutex)
	s.sizeBLock = new(sync.Mutex)
	s.lock = new(sync.Mutex)
	s.offsets = make(map[int]int64)
	s.lru = make([]int, 0, DEFAULT_OFFSET_CACHE_SIZE)
	return s
}

func New(path string, first int) (*Segment, error) {
	//
	var err error
	s := (&Segment{
		Path:  filepath.Join(path, fmt.Sprintf("segment_%016x", first)),
		First: first,
	}).init()
	s.writer, err = os.OpenFile(s.Path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening segment file for writing: %v", err)
	}
	s.reader, err = os.Open(s.Path)
	if err != nil {
		return nil, fmt.Errorf("error opening segment file for reading: %v", err)
	}
	return s, nil
}

func Open(path string) (*Segment, error) {
	//
	var err error
	s := (&Segment{Path: path}).init()
	s.writer, err = os.OpenFile(s.Path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening segment file for writing: %v", err)
	}
	s.reader, err = os.Open(s.Path)
	if err != nil {
		return nil, fmt.Errorf("error opening segment file for reading: %v", err)
	}
	// get id of first message
	m, err := s.read()
	if err == io.EOF {
		// the segment is empty
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("error reading id of first message: %v", err)
	}
	s.First = m.ID
	s.sizeB, _ = s.reader.Seek(0, 2)
	// loop throught all messages, count them
	s.len, err = s.count()
	if err != nil {
		return nil, fmt.Errorf("error counting segment records: %v", err)
	}
	return s, nil
}

func (s *Segment) Close() {
	s.writer.Close()
	s.writer = nil
}

func (s *Segment) Len() int {
	s.lenLock.Lock()
	defer s.lenLock.Unlock()
	return s.len
}

func (s *Segment) SizeB() int64 {
	s.sizeBLock.Lock()
	defer s.sizeBLock.Unlock()
	return s.sizeB
}

func (s *Segment) count() (int, error) {
	s.reader.Seek(0, 0)
	var err error
	for i := 0; ; i++ {
		err = s.next()
		if err == io.EOF {
			return i, nil
		}
		if err != nil {
			return i, err
		}
	}
}

//func (s *Segment) verify() error {
//	s.reader.Seek(0, 0)
//	m, err := s.read()
//	if err == io.EOF {
//		return nil
//	}
//	if m.ID == 0 {
//		h := sha256.New()
//		h.Write(marshal(m))
//		if !bytes.Equal(h.Sum(nil), m.Sha) {
//			return fmt.Errorf("invalid hash of first message")
//		}
//	}
//	for i := 1; ; i++ {
//		h := sha256.New()
//		h.Write(m.Sha)
//		m, err = s.read()
//		if err == io.EOF {
//			return nil
//		}
//		if err != nil {
//			return fmt.Errorf("error reading message: %v", err)
//		}
//		// this is inefficient, deserializing the message first, then serializing it again
//		// but for now this is quick and dirty code, trying to get basic verification working
//		h.Write(marshal(m))
//		if !bytes.Equal(h.Sum(nil), m.Sha) {
//			return fmt.Errorf("running hash %x doesn't match message hash %x", h.Sum(nil), m.Sha)
//		}
//		//log.Printf("%d: %x", i, h.Sum(nil))
//	}
//}

func (s *Segment) next() error {
	var len int64
	if _, err := fmt.Fscanf(s.reader, "%08x", &len); err != nil {
		return err
	}
	if _, err := s.reader.Seek(len, 1); err != nil {
		return err
	}
	return nil
}

func (s *Segment) seek(n int) error {
	if n < 0 {
		return ErrorOutOfBounds
	}
	if pos, ok := s.offsets[n]; ok {
		_, err := s.reader.Seek(pos, 0)
		return err
	}
	var i int
	if pos, ok := s.offsets[n-1]; ok {
		s.reader.Seek(pos, 0)
		i = n - 1
	} else {
		s.reader.Seek(0, 0)
	}
	for ; i < n; i++ {
		err := s.next()
		switch {
		case err == io.EOF:
			return ErrorOutOfBounds
		case err != nil:
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

func unmarshal(b []byte, m *message.Message) error {
	buff := bytes.NewBuffer(b)
	meta, err := buff.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("error reading message metadata: %v", err)
	}
	if err := json.Unmarshal(meta, m); err != nil {
		return fmt.Errorf("error parsing message metadata: %v", err)
	}
	m.Body = buff.Bytes()
	m.Body = m.Body[:len(m.Body)-1] // strip trailing newline
	return nil
}

func (s *Segment) read() (*message.Message, error) {
	var len int64
	_, err := fmt.Fscanf(s.reader, "%08x", &len)
	if err == io.EOF {
		return nil, ErrorOutOfBounds
	}
	if err != nil {
		return nil, fmt.Errorf("error parsing message size: %v", err)
	}
	b := make([]byte, int(len))
	if _, err := s.reader.Read(b); err != nil {
		return nil, err
	}
	m := new(message.Message)
	if err := unmarshal(b, m); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *Segment) Read(n int) (*message.Message, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.seek(n); err != nil {
		return nil, err
	}
	return s.read()
}

func (s *Segment) Last() (*message.Message, error) {
	return s.Read(s.Len() - 1)
}

func marshal(m *message.Message) ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	b = append(b, []byte("\n")...)
	b = append(b, m.Body...)
	b = append(b, []byte("\n")...)
	return b, nil
}

func (s *Segment) write(m *message.Message) error {
	b, _ := marshal(m)
	head := fmt.Sprintf("%08x", int32(len(b)))
	if _, err := s.writer.WriteString(head); err != nil {
		return err
	}
	if _, err := s.writer.Write(b); err != nil {
		return err
	}
	s.sizeBLock.Lock()
	s.sizeB += int64(len(head) + len(b))
	s.sizeBLock.Unlock()
	// skipping Sync() improves performance by order of magnitude
	return s.writer.Sync()
}

func (s *Segment) Write(m *message.Message) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.writer == nil {
		return ErrorSegmentClosed
	}
	if err := s.write(m); err != nil {
		return fmt.Errorf("error writing message to segment: %s", err)
	}
	s.lenLock.Lock()
	s.len += 1
	s.lenLock.Unlock()
	return nil
}
