SHELL:=/bin/bash

HASH:=$(shell git rev-parse --short HEAD)
DATE:=$(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

VERSION:=0.0.2a

.PHONY: hbuf
hbuf: 
	go install \
		-ldflags "-X main.Version=$(VERSION) -X main.BuildHash=$(HASH) -X main.BuildDate=$(DATE)" \
		'github.com/mkocikowski/hbuf/cmd/hbuf'

test:
	go test ./... -short

clean:
	rm -fr data

loc:
	find . -name '*.go' | xargs wc
	# no test files, no white space
	find . -name '*.go' | grep -v test | xargs cat | grep . | wc


dist: hbuf-linux-amd64 hbuf-darwin-amd64

.INTERMEDIATE: hbuf-linux-amd64
.INTERMEDIATE: hbuf-darwin-amd64
hbuf-%-amd64: URL=gs://peakunicorn/bin/amd64/$*/hbuf
hbuf-%-amd64: ./cmd/hbuf/main.go
	GOARCH=amd64 GOOS=$* go build \
		-ldflags "-X main.Version=$(VERSION) -X main.BuildHash=$(HASH) -X main.BuildDate=$(DATE)" \
		-o $@ $<
	gsutil cp $@ $(URL)
	gsutil setmeta -h "Cache-Control:public, max-age=60" $(URL)
	gsutil acl ch -u AllUsers:R $(URL)


net:
	#netstat -a -f inet -p tcp
	lsof -a -c 'hbuf' -nP -i4TCP@127.0.0.1

