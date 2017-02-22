package message

import (
	"crypto/sha256"
	"encoding/json"
	"time"
)

type Message struct {
	ID   int       `json:"id"`
	TS   time.Time `json:"ts"`
	Type string    `json:"type"`
	Body []byte    `json:"-"`
	Sha  []byte    `json:"sha"`
}

func (m *Message) Sum(previous []byte) []byte {
	h := sha256.New()
	m.Sha = nil
	j, _ := json.Marshal(m)
	h.Write(previous)
	h.Write(j)
	h.Write(m.Body)
	m.Sha = h.Sum(nil)
	return m.Sha
}
