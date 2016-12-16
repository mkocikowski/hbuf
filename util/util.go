package util

import (
	"encoding/hex"
	"math/rand"
	"regexp"
	"time"
)

var (
	TopicNameRE    = regexp.MustCompile(`^[a-zA-Z0-9_\-]{1,256}$`)
	ConsumerNameRE = regexp.MustCompile(`^[a-zA-Z0-9_\-]{1,256}$`)
	//Client      = &http.Client{
	//Transport: &http.Transport{
	//MaxIdleConnsPerHost: 256,
	//},
	//Timeout: 5 * time.Second,
	//}
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func Uid() string {
	c := 8 // 64 bits
	b := make([]byte, c)
	rand.Read(b)
	return hex.EncodeToString(b)
}
