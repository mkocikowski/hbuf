package message

type Location struct {
	Segment string
	Offset  int64
}

type Message struct {
	Id   int
	Type string
	Body []byte
}
