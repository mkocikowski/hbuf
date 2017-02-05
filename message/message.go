package message

type Location struct {
	Segment string
	Offset  int64
}

type Message struct {
	ID    int
	Type  string
	Body  []byte
	Error chan error
}
