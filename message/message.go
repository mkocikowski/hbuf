package message

import "time"

type Message struct {
	ID   int       `json:"id"`
	TS   time.Time `json:"ts"`
	Type string    `json:"type"`
	Body []byte    `json:"-"`
	Sha  []byte    `json:"-"`
}

func New(t string, body []byte) *Message {
	m := &Message{
		TS:   time.Now().UTC(),
		Type: t,
		Body: body,
	}
	return m
}
