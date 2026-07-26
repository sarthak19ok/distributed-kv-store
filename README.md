# Distributed Key-Value Store

A persistent key-value store with write-ahead logging, built in C++.
Stage 1 of a larger project targeting replication + simplified Raft consensus.

## Status
- [x] Stage 1: single-node store, WAL persistence, crash recovery, graceful shutdown
- [x] Stage 2: two-node async replication with ack-based failure detection (tested)
- [x] Stage 2 polish: STATUS command (role, key count, replica connectivity, pending queue depth), bounded shutdown delay documented
- [x] Stage 3: simplified Raft — leader election, terms, heartbeats, 3-node cluster (tested, including killing the leader and watching failover)
- [ ] Tradeoffs writeup (ongoing, see below)

## Build
```
make
```

## Run standalone
```
./kvserver <port> <wal_path>
```

## Run as a replicated pair
```
# terminal 1: replica
./kvserver 7002 data/replica/wal.log

# terminal 2: primary, streaming commits to the replica
./kvserver 7001 data/primary/wal.log --replica-of=127.0.0.1:7002
```

## Run a 3-node Raft cluster
```
# terminal 1
./kvserver 8001 data/n1/wal.log --raft=1,2:127.0.0.1:8002,3:127.0.0.1:8003
# terminal 2
./kvserver 8002 data/n2/wal.log --raft=2,1:127.0.0.1:8001,3:127.0.0.1:8003
# terminal 3
./kvserver 8003 data/n3/wal.log --raft=3,1:127.0.0.1:8001,2:127.0.0.1:8002
```
`--raft=MY_ID,PEER_ID:HOST:PORT,PEER_ID:HOST:PORT,...` — every node needs
a unique numeric id and the full peer list. One node will win an election
within a few seconds; check with `STATUS` on any node. Client `SET`/`DEL`
only succeed against the current leader — followers respond with
`ERR not leader, current leader is node <id>`.

## Protocol
Plain-text, newline-terminated, over TCP:
```
SET <key> <value>   -> OK
GET <key>            -> VALUE <value>  |  NOTFOUND
DEL <key>            -> OK
STATUS               -> ROLE <primary|standalone> KEYS <n> [REPLICA_CONNECTED <yes|no> PENDING <n>]
                        (Raft nodes instead report: ROLE <leader|candidate|follower> TERM <n> LEADER_ID <id> KEYS <n>)
```
Replication uses the same port/connection loop with a separate,
length-prefixed command pair (`REPL_SET` / `REPL_DEL`) so keys and values
containing spaces are handled safely — see `src/server.cpp`.

## Design notes

**Why a WAL, and why append-before-apply:**
Every mutation is written to the WAL and flushed *before* it touches the
in-memory map. If the process crashes between those two steps, replaying
the WAL on restart reconstructs the exact same state — the write is never
silently lost, and never silently duplicated in a way that changes the
final value (SET/DEL are idempotent when replayed in order).

**Why a text-based WAL format:**
Slower and larger on disk than a binary format, but trivially inspectable
(`cat data/wal.log`) while debugging replication in later stages. At this
project's scale, debuggability wins over raw throughput.

**Durability:** every append does `fflush()` (libc buffer → OS) followed
by `fsync()` (OS → physical disk), so an acknowledged write survives not
just a process crash but an OS crash or power loss too. Verified by
`kill -9`'ing the server mid-session and confirming state on restart.

**Graceful shutdown:** SIGINT/SIGTERM flip an atomic flag and unblock the
listening `accept()` via `shutdown()`, so the server exits cleanly instead
of looking like a crash every time you stop it. Known simplification:
already-connected client threads are detached, not joined, on shutdown —
acceptable for a demo, called out here rather than hidden.

**Concurrency model:**
One thread per connection, `shared_mutex` guarding the map (many readers,
one writer at a time). Fine at demo scale; a thread-pool + epoll model
would be the next step for a "real" system, and that tradeoff belongs in
the writeup too.

## Stage 2: replication design and CAP tradeoffs

**Chose async primary-replica over Raft-style quorum commit.** A client
write is durable (WAL + fsync) and acknowledged on the PRIMARY alone;
replication to the replica happens afterward, on a background thread.
This is explicitly an **availability-over-consistency** choice: the
replica can lag or even be fully down, and the primary keeps serving
client reads and writes the whole time. The cost is that a client's
write is only guaranteed durable on the node it hit — if the primary
dies before replicating, the replica may never see that write, even
though the primary already told the client "OK".

The alternative (a Raft-style quorum: block the client's ack until a
majority of nodes have the write) trades that availability for a
stronger guarantee — an acknowledged write provably survives the loss of
any one node. That's Stage 3, if time allows.

**Bug found and fixed during testing:** the first implementation treated
a successful `send()` to the replica as proof of delivery. It isn't —
TCP will accept bytes into the local kernel send buffer even when the
peer process is already dead (e.g. killed with SIGKILL), so a dead
replica looked identical to a healthy one until a write happened to hit
a retransmit timeout, sometimes minutes later. Fixed by requiring an
explicit application-level `REPL_OK` acknowledgment (with a receive
timeout) after every replicated record, so failure is detected in
seconds, not minutes. Verified with a real kill-and-observe test.

**Verified failure-mode behavior (via `kill -9` on the replica process):**
- Replica down → primary keeps accepting client writes (availability
  preserved), replicator logs failed sends and retries on a timer.
- Replica restarted → replicator reconnects automatically; the write
  that failed mid-outage is resent and lands correctly, because failed
  sends requeue the record rather than dropping it.
- Remaining known gap: if the **primary** itself crashes in the narrow
  window between a local WAL commit and a successful replication ack,
  that record is lost to the replica (though never lost outright — it's
  already durable in the primary's own WAL). Closing this fully needs a
  persistent per-replica replication offset/cursor, which is the kind of
  bookkeeping Raft's log-matching property exists to formalize.

**Replica durability:** a replica applies incoming records through its
own WAL (`Store::applyReplicated`), not just its in-memory map — so a
replica can independently crash-recover its own state, rather than being
a pure cache that goes blank on restart.

## Stage 3: simplified Raft leader election

**What's implemented:** every node runs identically (`RaftNode`), starting
as a `Follower`. Each tracks a `current_term` and, on a randomized
1.5–3s timeout with no heartbeat from a legitimate leader, becomes a
`Candidate`: increments its term, votes for itself, and sends
`REQUEST_VOTE` to every peer. A peer grants at most one vote per term
(first-come, term-gated). A majority of votes (2 of 3) wins the
election and the candidate becomes `Leader`, which then sends periodic
`APPEND_ENTRIES` heartbeats (empty entry lists) to keep followers from
timing out and starting their own elections. Client `SET`/`DEL` are only
accepted by the current leader; a client hitting a follower gets
redirected with the current leader's id. Client writes are replicated
to followers **immediately** via `AppendEntries` (not just piggybacked
on the next heartbeat), so a write doesn't sit unreplicated for up to
a full heartbeat interval.

**Verified by actually killing the leader process** (`kill -9`) mid-run
on a live 3-node cluster:
- Old leader (node 1, term 1) killed.
- Within ~4 seconds, node 2 started and won an election for **term 2**
  (correctly incremented), and node 3 recognized node 2 as the new
  leader — both nodes agreeing on the same term.
- Data written before the crash (already replicated) survived on both
  surviving nodes.
- The new leader (node 2) accepted a fresh write and replicated it to
  node 3 immediately.
- When the old leader (node 1) was restarted, it saw node 2's higher
  term via a heartbeat and correctly stepped down to `Follower` rather
  than contending for leadership — this is Raft's core safety property:
  any RPC carrying a higher term forces the recipient to defer to it.

**Deliberately out of scope (documented, not hidden — see `raft.hpp`):**
- **Log matching / consistency checking.** Real Raft's `AppendEntries`
  includes `prevLogIndex`/`prevLogTerm` so a follower can detect and
  reject an entry that doesn't extend its log correctly, and the leader
  can then repair a follower's divergent suffix. This implementation
  trusts the leader's stream in order and does not detect or repair log
  divergence. This is the single biggest piece of complexity cut from
  full Raft — implementing it properly is most of what makes the Raft
  paper long.
- **No majority-commit gating on the client response.** In full Raft, a
  leader doesn't tell the client "OK" until a majority of nodes have
  durably stored the entry. Here, the client gets "OK" as soon as the
  leader's own WAL write succeeds; replication to followers happens
  right after, best-effort. A leader that crashes between its own
  commit and successfully replicating could still lose that write from
  the cluster's perspective (though, as in Stage 2, it's never lost
  from the leader's own WAL if the leader itself survives).
- **No persistent term/vote state.** `current_term` and `voted_for` are
  in-memory only. Real Raft persists these to disk, because a node that
  restarts and forgets it already voted this term could vote twice —
  a safety violation. A production version would extend the existing
  WAL/fsync machinery to also durably record term and vote.
- **No log compaction, snapshotting, or dynamic cluster membership** —
  the peer set is fixed at startup via the `--raft=` argument.

**Why this scope was the right cut for the time available:** the
properties people actually mean when they say "understands consensus"
— a cluster that elects a leader, keeps that leader stable via
heartbeats, detects a dead leader and elects a new one automatically,
and enforces that only one node accepts writes at a time — are all
implemented and tested here. The cut features (log repair, majority
commit, persistent state) are real gaps in a from-scratch system, but
they're incremental hardening on top of a working core, not
prerequisites for demonstrating the core idea.
