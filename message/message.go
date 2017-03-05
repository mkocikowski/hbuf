package message

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"time"
)

type Message struct {
	ID   int       `json:"id"`
	TS   time.Time `json:"ts`
	Type string    `json:"type"`
	Body []byte    `json:"-"`
	Sha  []byte    `json:"sha"`
}

func (m *Message) Sum(previous []byte) []byte {
	h := sha256.New()
	b := bytes.NewBuffer(previous)
	fmt.Fprint(b, m.ID, m.TS, m.Type)
	h.Write(b.Bytes())
	h.Write(m.Body)
	m.Sha = h.Sum(nil)
	return m.Sha
}
