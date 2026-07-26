# Distributed KV Store

A persistent key-value store built in C++, inspired by Redis/DynamoDB style systems. Started as a single-node store with a write-ahead log, then added async replication between nodes, then a simplified version of Raft for leader election across a cluster.

Built as a systems project to get hands-on with the stuff that usually only shows up in interviews as theory - write-ahead logging, replication, consensus, what actually happens when a node dies mid-write.

## What it does

- Basic `GET` / `SET` / `DELETE` over TCP
- Write-ahead log so it survives a crash (tested by `kill -9`'ing it mid-run and checking state after restart)
- Can run as a primary + replica pair, primary streams writes to the replica asynchronously
- Can also run as a 3-node cluster with real leader election (simplified Raft) - kill the leader and a new one gets elected within a few seconds

## Prerequisites

- A C++17 compiler (`g++` or `clang++`)
- `make`
- Linux or macOS (uses POSIX sockets directly, no Windows support)

## Getting started

Clone the repo and build:
```
git clone https://github.com/sarthak19ok/distributed-kv-store.git
cd distributed-kv-store
make
```

This produces a `kvserver` binary.

### Run a single node
```
./kvserver <port> <wal_path>
```
Example:
```
./kvserver 6380 data/wal.log
```

### Run a primary + replica pair
```
./kvserver 7002 data/replica/wal.log
./kvserver 7001 data/primary/wal.log --replica-of=127.0.0.1:7002
```
The primary streams every write to the replica in the background. Writes to the primary succeed even if the replica is down.

### Run a 3-node Raft cluster
Run each of these in its own terminal:
```
./kvserver 8001 data/n1/wal.log --raft=1,2:127.0.0.1:8002,3:127.0.0.1:8003
./kvserver 8002 data/n2/wal.log --raft=2,1:127.0.0.1:8001,3:127.0.0.1:8003
./kvserver 8003 data/n3/wal.log --raft=3,1:127.0.0.1:8001,2:127.0.0.1:8002
```
One node becomes leader within a few seconds. Check with `STATUS` on any node. Only the leader accepts writes - hitting a follower with `SET`/`DEL` returns an error telling you the current leader's id.

### Talking to it

It's a plain-text TCP protocol, so you can use `nc`, `telnet`, or the included `test_client.py`:
```
python3 test_client.py <port>
```

## Protocol

One command per line, newline-terminated:
```
SET <key> <value>
GET <key>
DEL <key>
STATUS
```
Replication and Raft use their own prefixed commands internally (`REPL_SET`, `REQUEST_VOTE`, `APPEND_ENTRIES`, etc) - see `src/server.cpp` for the full wire format.

## Project structure
```
include/       header files (wal, store, replicator, raft)
src/           implementation + server entry point
test_client.py simple Python client for manual testing
Makefile       build config
```

## Design notes and tradeoffs

**Why WAL-first:** every write hits the log (and gets `fsync`'d) before it touches the in-memory map. If the process dies in between, replaying the log on restart gets back to the same state. Verified by killing the server mid-session and restarting - data was intact.

**Async replication:** the primary doesn't wait for the replica before acking a client write. This keeps it available even if the replica is down, but if the primary dies before it manages to replicate a write, that write can be lost to the replica. Classic availability-vs-consistency tradeoff - went with availability since it's simpler and still demonstrates the real lesson.

Found a genuinely annoying bug while testing this: a successful `send()` doesn't mean the replica actually got the data. TCP will happily buffer bytes locally even if the other side is already dead, so killing the replica looked totally fine to the primary until much later. Fixed by requiring the replica to send back an explicit ack after every write, with a timeout, so a dead replica gets noticed in ~2 seconds instead of an unpredictable delay.

**Raft:** implemented the core loop - randomized election timeouts, terms, majority voting, heartbeats. Tested by starting 3 nodes and killing the leader with `kill -9` to watch a new one get elected. It worked - term incremented correctly, new leader took over, and the old leader stepped down when it came back online instead of fighting for leadership.

Didn't implement the full Raft paper. Missing on purpose:
- No log-matching/repair between leader and followers (this is most of what makes the actual paper long)
- Client write gets acked as soon as the leader's local WAL commit succeeds, not waiting for a majority of nodes to have it like real Raft would
- Term/vote state is only in memory, doesn't survive a restart (a real implementation persists this to disk)
- No snapshotting/log compaction, no adding or removing nodes at runtime

The goal was a working leader election + failover you can actually test, not a full reproduction of the paper. Felt like the right tradeoff given the time available.

## What's next

- Persist `term`/`voted_for` so a restarted node can't accidentally double-vote in the same term
- Add the log consistency check so a leader can catch and repair a follower with a diverged log
- Make writes wait for a majority ack before telling the client "OK" (actual Raft-level consistency)

## License

No license file yet - treat as source-available for reference; ask before reusing commercially.
