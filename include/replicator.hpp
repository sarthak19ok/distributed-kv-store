#pragma once
#include <string>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <chrono>
#include "wal.hpp"

// Runs on the PRIMARY. Owns a background thread that maintains a TCP
// connection to one replica and streams committed WAL records to it.
//
// Design: async, best-effort, at-least-once.
//   - Store::onCommit() pushes each record onto an in-memory queue.
//   - A background thread drains the queue and sends records to the
//     replica over its own connection, independent of client requests.
//   - Each send is followed by waiting for an application-level "REPL_OK"
//     ack (with a receive timeout) rather than trusting a successful
//     send() alone — TCP will happily accept bytes into the local kernel
//     buffer even when the peer is already dead, so send() success does
//     NOT mean the replica received anything. This was found and fixed
//     by testing: killing the replica with SIGKILL looked, at first, like
//     a healthy connection to the primary until a write was attempted.
//   - If the ack doesn't arrive (timeout, RST, or EOF), the record is
//     requeued and the connection is torn down and retried. Because of
//     the requeue, a record whose delivery attempt failed WILL be resent
//     once the replica comes back — verified by killing the replica,
//     writing to the primary, restarting the replica, and confirming the
//     write arrives. The remaining, still-real gap: if the PRIMARY itself
//     crashes before a requeued record is ever successfully resent, that
//     record is lost to the replica (though it's already durable in the
//     primary's own WAL, so no data is lost outright — just not yet
//     replicated). A full solution needs a persistent per-replica
//     replication offset, which is exactly what Raft's log-matching
//     property formalizes.
class Replicator {
public:
    Replicator(std::string host, int port);
    ~Replicator();

    void start();
    void stop();

    // Called from Store::onCommit on the primary's request-handling thread.
    // Non-blocking: just enqueues, the background thread does the I/O.
    void enqueue(const WalRecord& rec);

    // Lightweight status accessors for the STATUS command / debugging.
    // Both are read without locking the queue mutex for the connected
    // flag (a plain atomic bool), and with it briefly for the queue size —
    // approximate numbers are fine here, this is observability, not a
    // correctness-critical path.
    bool isConnected() const { return connected_.load(); }
    size_t pendingCount();

private:
    void run(); // background thread body
    bool connectToReplica(); // returns true on success
    bool sendRecord(int fd, const WalRecord& rec);

    std::string host_;
    int port_;
    std::atomic<bool> running_{false};
    std::atomic<bool> connected_{false};
    std::thread worker_;

    std::mutex q_mu_;
    std::condition_variable q_cv_;
    std::queue<WalRecord> queue_;

    int replica_fd_ = -1;
};
