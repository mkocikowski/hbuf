package segment

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/mkocikowski/hbuf/message"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

const (
	DEFAULT_OFFSET_CACHE_SIZE = 1 << 10
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
	sha       []byte
	shaLock   *sync.Mutex
}

func (s *Segment) Open() error {
	//
	s.lenLock = new(sync.Mutex)
	s.sizeBLock = new(sync.Mutex)
	s.shaLock = new(sync.Mutex)
	s.lock = new(sync.Mutex)
	s.offsets = make(map[int]int64)
	s.lru = make([]int, 0, DEFAULT_OFFSET_CACHE_SIZE)
	//
	var err error
	s.writer.Close()
	// create segment file if it doesn't exist
	s.writer, err = os.OpenFile(s.Path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("error opening segment file for writing: %v", err)
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
		return fmt.Errorf("error reading id of first segment message: %v", err)
	}
	s.First = m.ID
	// loop throught all messages, count them
	s.len, err = s.count()
	if err != nil {
		return fmt.Errorf("error counting segment messages: %v", err)
	}
	// validate the chain of message SHAs (verify segment integrity)
	if err := s.verify(); err != nil {
		return fmt.Errorf("error verifying segment integrity: %v", err)
	}
	//
	s.sizeB, _ = s.reader.Seek(0, 2)
	// set segment's sha to the sha of the last message
	m, err = s.Last()
	if err == nil {
		s.sha = m.Sha
	}
	j, _ := json.Marshal(s)
	log.Println(string(j))
	return nil
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

func (s *Segment) verify() error {
	s.reader.Seek(0, 0)
	m, err := s.read()
	if err == io.EOF {
		return nil
	}
	if m.ID == 0 {
		h := sha256.New()
		h.Write(marshal(m))
		if !bytes.Equal(h.Sum(nil), m.Sha) {
			return fmt.Errorf("invalid hash of first message")
		}
	}
	for i := 1; ; i++ {
		h := sha256.New()
		h.Write(m.Sha)
		m, err = s.read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("error reading message: %v", err)
		}
		// this is inefficient, deserializing the message first, then serializing it again
		// but for now this is quick and dirty code, trying to get basic verification working
		h.Write(marshal(m))
		if !bytes.Equal(h.Sum(nil), m.Sha) {
			return fmt.Errorf("running hash %x doesn't match message hash %x", h.Sum(nil), m.Sha)
		}
		//log.Printf("%d: %x", i, h.Sum(nil))
	}
}

func (s *Segment) next() error {
	var len int64
	_, err := fmt.Fscanf(s.reader, "%08x", &len)
	if err == io.EOF {
		return err
	}
	if err != nil {
		return fmt.Errorf("error parsing message size: %v", err)
	}
	// the +66 accounts for :sha256:
	_, err = s.reader.Seek(len+66, 1)
	if err != nil {
		return fmt.Errorf("error seeking end of message: %v", err)
	}
	return nil
}

func unmarshal(b []byte) (*message.Message, error) {
	u := bytes.NewBuffer(b)
	meta, err := u.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	m := new(message.Message)
	err = json.Unmarshal(meta, m)
	if err != nil {
		return nil, err
	}
	m.Body = u.Bytes()
	m.Body = m.Body[:len(m.Body)-1]
	return m, nil
}

func (s *Segment) read() (*message.Message, error) {
	//
	var len int64
	var sha []byte
	_, err := fmt.Fscanf(s.reader, "%08x:%64x:", &len, &sha)
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("error parsing message size: %v", err)
	}
	b := make([]byte, int(len))
	if _, err := s.reader.Read(b); err != nil {
		return nil, err
	}
	m, err := unmarshal(b)
	if err != nil {
		return nil, err
	}
	m.Sha = sha
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

func (s *Segment) Last() (*message.Message, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	n := s.Len() - 1
	if err := s.seek(n); err != nil {
		return nil, err
	}
	m, err := s.read()
	if err != nil {
		return nil, fmt.Errorf("error reading message from segment: %v", err)
	}
	return m, nil
}

func (s *Segment) Read(id int) (*message.Message, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if id < s.First {
		return nil, ErrorOutOfBounds
	}
	n := id - s.First
	if n >= s.Len() {
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

func marshal(m *message.Message) []byte {
	b, _ := json.Marshal(m)
	b = append(b, []byte("\n")...)
	b = append(b, m.Body...)
	b = append(b, []byte("\n")...)
	return b
}

func (s *Segment) write(m *message.Message) error {
	b := marshal(m)
	// sha of the message is the current segment sha + sha of the serialized message
	h := sha256.New()
	s.shaLock.Lock()
	h.Write(s.sha)
	s.shaLock.Unlock()
	h.Write(b)
	m.Sha = h.Sum(nil)
	//
	head := fmt.Sprintf("%08x:%64x:", int32(len(b)), m.Sha)
	if _, err := s.writer.WriteString(head); err != nil {
		return err
	}
	if _, err := s.writer.Write(b); err != nil {
		return err
	}
	s.sizeBLock.Lock()
	s.sizeB += int64(len(head) + len(b))
	s.sizeBLock.Unlock()
	s.shaLock.Lock()
	s.sha = m.Sha
	s.shaLock.Unlock()
	// skipping Sync() improves performance by order of magnitude
	return s.writer.Sync()
}

func (s *Segment) Write(m *message.Message) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.writer == nil {
		return ErrorSegmentClosed
	}
	m.ID = s.First + s.Len()
	if err := s.write(m); err != nil {
		return fmt.Errorf("error writing message to segment: %s", err)
	}
	s.lenLock.Lock()
	s.len += 1
	s.lenLock.Unlock()
	return nil
}
