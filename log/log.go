package log

import (
	"log"
	"os"
)

var (
	DEBUG = log.New(os.Stderr, "[DEBUG] ", log.Lshortfile)
	INFO  = log.New(os.Stderr, "[INFO] ", log.Lshortfile)
	WARN  = log.New(os.Stderr, "[WARNING] ", log.Lshortfile)
	ERROR = log.New(os.Stderr, "[ERROR] ", log.Lshortfile)
)
