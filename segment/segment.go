package segment

import (
	"bytes"
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
	Path    string `json:"path"`
	First   int    `json:"first"`
	Count   int    `json:"count"`
	SizeB   int64  `json:"size_b"`
	writer  *os.File
	reader  *os.File
	lock    *sync.Mutex
	offsets map[int]int64
	lru     []int
}

type Asc []*Segment

func (s Asc) Len() int           { return len(s) }
func (s Asc) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Asc) Less(i, j int) bool { return s[i].First < s[j].First }

func (s *Segment) Open() error {
	//
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
	s.Count = s.count() // loops through all messages
	s.SizeB, _ = s.reader.Seek(0, 2)
	j, _ := json.Marshal(s)
	log.Println(string(j))
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
	_, err := fmt.Fscanf(s.reader, "%08x", &len)
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
	return m, err
}

//func (s *Segment) read() (*message.Message, error) {
//	startOffset, _ := s.reader.Seek(0, 1)
//	var recordLen, id, ts int64
//	var typ string
//	if _, err := fmt.Fscanf(s.reader, "%08x %d %d %q\n", &recordLen, &id, &ts, &typ); err != nil {
//		return nil, err
//	}
//	currentOffset, _ := s.reader.Seek(0, 1)
//	headLen := currentOffset - startOffset
//	bodyLen := recordLen - (headLen - 8) // first 8 bytes is the record byte size
//	m := &message.Message{
//		ID:   int(id),
//		TS:   time.Unix(0, ts),
//		Type: typ,
//		Body: make([]byte, bodyLen),
//	}
//	if _, err := s.reader.Read(m.Body); err != nil {
//		return nil, err
//	}
//	// TODO: check for empty message body?
//	m.Body = m.Body[:len(m.Body)-1] // remove trailing newline
//	return m, nil
//}

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
	n := id - s.First
	if n >= s.Count {
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
	head := fmt.Sprintf("%08x", int32(len(b)))
	if _, err := s.writer.WriteString(head); err != nil {
		return err
	}
	if _, err := s.writer.Write(b); err != nil {
		return err
	}
	s.SizeB += int64(len(head) + len(b))
	// skipping Sync() improves performance by order of magnitude
	return s.writer.Sync()
}

//func (s *Segment) write(m *message.Message) error {
//	meta := fmt.Sprintf(" %d %d %q\n", m.ID, m.TS.UnixNano(), m.Type)
//	head := fmt.Sprintf("%08x", int32(len(meta)+len(m.Body)+1))
//	_, err := s.writer.WriteString(head + meta)
//	if err != nil {
//		return err
//	}
//	s.writer.Write(m.Body)
//	s.writer.Write([]byte{'\n'})
//	s.SizeB += int64(len(head) + len(meta) + len(m.Body) + 1)
//	// skipping Sync() improves performance by order of magnitude
//	return s.writer.Sync()
//}

func (s *Segment) Write(m *message.Message) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.writer == nil {
		return ErrorSegmentClosed
	}
	m.ID = s.First + s.Count
	if err := s.write(m); err != nil {
		return fmt.Errorf("error writing message to segment: %s", err)
	}
	s.Count += 1
	return nil
}
