package message

import "time"

type Location struct {
	Segment string
	Offset  int64
}

type Message struct {
	ID    int        `json:"id"`
	TS    time.Time  `json:"ts"`
	Type  string     `json:"type"`
	Body  []byte     `json:"-"`
	Error chan error `json:"-"`
}

func New(t string, body []byte) *Message {
	m := &Message{
		TS:    time.Now().UTC(),
		Type:  t,
		Body:  body,
		Error: make(chan error, 1),
	}
	return m
}
