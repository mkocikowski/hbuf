- simple ops:
    - single binary
    - no dependencies (zookeeper, etcd)
    - files on disk
    - ability to do a rolloing upgrade of nodes in a cluster
    - ability to have nodes running different versions be part of a cluster
    - backups and restores in a running cluster (see ability to "pause" a buffer)
- secure:
    - client and inter-node comms encrypted
    - ACLs +? RBAC
    - PKI encryption of files on disk?
    - ability to delete messages (right-to-forget) ?
    - message chaining - being able to verify messages haven't been tampered with, like journald audit ? this precludes the one above ^^. but TTL still possible.
- user friendly:
    - http/s for both client and inter-node comms
    - files on disk grep-able

- the basic unit is a buffer
- for a buffer there is 1 primary shard + N replicas
- both writes and reads go against the primary only
- 1+ buffers can be grouped into topics ; topics are like elasticsearch aliases
- a buffer can belong to multiple topics
- files on disk stored as http requests (header + body)
- ability to delete individual messages (zero out their content)
- like in kafka, each message has unique number, counting from first message in the buffer
- when a node goes down, degrade cluster status, but DON'T rebalance shards automatically

- multi-tenant from the ground up
- a "server" is an hbuf process
- each server has 1 or more tenants
- each tenant has a manager, worker, and client (not necessarily all of them)

- ability to "close" a topic (/buffer?) for writing
- ability to "pause" a buffer - no changes will be made to segments or to consumers - so that backups can be done easily; the idea is that if there are multiple buffers per topic, the pasue wouldn't really be noticed? essentially ability to shut down a buffer without triggering a rebalance

TODO: set up explicit "expectations" and plan / test agaist them:
- max message size
- max number of messages
- max number of consumers / buffer
- max number of buffers / worker
- max number of buffers / topic

UUIDs. good ardument for having new UUID for each node every time it starts up is that way if a VM with a running node is snapshotted and then restarted on a new machine, there will be no duplicates in the cluster. TODO: think though what would happen in this situation - if someone took a snapshot, and then fired it up on another machine, and had it join the same cluster - what would happen to the duplicated buffers? SO THERE HAS TO BE LOGIC PREVENTING DUPLICATE BUFFER REGISTRATION

TAGS. have ability to "tag" messages, say with "x-hbuf-tag:" header; these tags could be used for filtering messages on the worker side, retrieving only matching messages; so say these are log items, you could tag them per host, and then retrieve only consume messages matching given host. this filtering could significantly reduce network traffic.

KEYS. routing keys. "x-hbuf-key:" header.

REPLICATION. with kafka, when a replica fails, this is a big burden on the primary: it has to go back to the beginning, which blows the fs cache; right at the time when things are bad, extra pressure is put on the cluster. what if by default replication started off the tail of the primary? for data with relatively short ttl, this makes a lot of sense: chances are data will expire before buffer failure. anyway, the MAIN IDEA IS TO HAVE PULL REPLICATION - a replica buffer consumes from the origin buffer; so these can be daisy chained, to reduce read loads. also, consumers can filter to TAGs, so buffers can be populated with only a subset of data. and since a topic is only a logical grouping of buffers, you can combine these filtered replicas into new topics... because of the way that data is stored on disk (buffer's directory is portable and self-contined, segments are immutable) disk / fs backups are a very viable tool.




see: https://github.com/redis/redis-rcp/blob/master/RCP11.md
