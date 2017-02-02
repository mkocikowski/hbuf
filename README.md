Purpose
---
User friendly implementation of ideas behind "Kafka". 

Quick start
---
Download (change "darwin" to "linux" as needed):
```shell
curl -Os https://storage.googleapis.com/peakunicorn/bin/amd64/darwin/hbuf
chmod 0755 hbuf
```
Run:
```shell
./hbuf
```
Produce message:
```shell
curl localhost:8080/topics/foo -d'bar'
```
Consume message:
```shell
curl localhost:8080/topics/foo/next
```
Stats:
```shell
curl localhost:8080/stats
```
Get more info (this will show all available routes and info about them):
```shell
curl localhost:8080 | jq .
```

CLI
---
`hbuf -h` built in tools:
- producer
- consumer
- load generator ("stress")

Reason
---
I see Kafka not as a message queue, but as an append-only file; a place where
data (usually logs of some kind) gets dumped, and then read and re-read at
leisure. A semi-permanent data store; a buffer. I find this hugely useful,
especially if messages can be assigned unique IDs, which allows for idempotent
consumption.

- 
