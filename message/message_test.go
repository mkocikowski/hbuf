package message

import (
	"crypto/sha256"
	"testing"
	"time"
)

func BenchmarkShasum(b *testing.B) {
	s := sha256.Sum256([]byte("foo"))
	m := &Message{
		ID:   1,
		TS:   time.Now().UTC(),
		Type: "text/plain",
		Body: []byte("hello, world!"),
	}
	for i := 0; i < b.N; i++ {
		m.Sum(s[:])
	}
}
