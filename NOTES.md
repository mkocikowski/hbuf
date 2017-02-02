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
    - message chaining - being able to verify messages haven't been tampered with, like journald audit ?
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
- like in kafka, meach message has unique number, counting from first message in the buffer
- when a node goes down, degrade cluster status, but DON'T rebalance shards automatically

- multi-tenant from the ground up
- multiple "nodes" per server; each "node" serves only 1 tenant; 3 types of "nodes":
    - control
    - data
    - client

- ability to "close" a topic (/buffer?) for writing
- ability to "pause" a buffer - no changes will be made to segments or to consumers - so that backups can be done easily; the idea is that if there are multiple buffers per topic, the pasue wouldn't really be noticed? essentially ability to shut down a buffer without triggering a rebalance

TODO: set up explicit "expectations" and plan / test agaist them:
- max message size
- max number of messages
- max number of consumers / buffer
- max number of buffers / worker
- max number of buffers / topic

UUIDs. good ardument for having new UUID for each node every time it starts up is that way if a VM with a running node is snapshotted and then restarted on a new machine, there will be no duplicates in the cluster. TODO: think though what would happen in this situation - if someone took a snapshot, and then fired it up on another machine, and had it join the same cluster - what would happen to the duplicated buffers?


see: https://github.com/redis/redis-rcp/blob/master/RCP11.md
