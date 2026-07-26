#pragma once
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <random>
#include <chrono>
#include <functional>
#include "wal.hpp"
#include "store.hpp"

// Simplified Raft consensus module.
//
// SCOPE (deliberately simplified vs full Raft — documented, not hidden):
//   - Leader election with randomized timeouts, terms, majority voting: YES
//   - AppendEntries as both heartbeat and replication carrier: YES
//   - Log matching / consistency check (prevIndex/prevTerm) before
//     accepting entries: NO — this demo trusts the leader's stream in
//     order and does not detect/repair a divergent follower log. Real
//     Raft's log-matching property and conflict-resolution (overwriting
//     a follower's conflicting suffix) is the single biggest chunk of
//     complexity we're cutting here.
//   - Log compaction / snapshotting: NO
//   - Cluster membership changes (adding/removing nodes at runtime): NO
//     — the peer set is fixed at startup via command-line config.
//   - Persistent term/vote state surviving a restart: NO — current_term_
//     and voted_for_ are in-memory only. A real implementation persists
//     these (they're as safety-critical as the log itself: a node that
//     forgets it already voted this term could double-vote after a
//     restart). Documented as a known gap.
//
// This buys a working, testable 3-node leader election you can kill -9
// the leader on and watch a new one get elected, which is the core
// property people mean when they say "understands consensus" — without
// trying to reproduce the full Raft paper's log-repair machinery in a
// student project's timeframe.
enum class NodeState { FOLLOWER, CANDIDATE, LEADER };

struct PeerConfig {
    std::string host;
    int port;
    int id; // stable numeric id, used as candidate_id / leader_id
};

class RaftNode {
public:
    RaftNode(int my_id, std::vector<PeerConfig> peers, Store* store);
    ~RaftNode();

    void start();
    void stop();

    // Called by the server's connection handler when an incoming
    // REQUEST_VOTE or APPEND_ENTRIES line arrives on any accepted socket.
    // Returns the response line to write back.
    std::string handleRequestVote(int term, int candidate_id);
    std::string handleAppendEntries(int term, int leader_id,
                                     const std::vector<WalRecord>& entries);

    NodeState state() const { return state_.load(); }
    int currentTerm() const { return current_term_.load(); }
    int leaderId() const { return leader_id_.load(); }
    int myId() const { return my_id_; }

    // Called by Store::onCommit, only meaningful when this node is the
    // current leader. Sends the single entry to every peer immediately via
    // AppendEntries, rather than waiting for the next periodic heartbeat —
    // otherwise a client write could sit unreplicated for up to the full
    // heartbeat interval. Fire-and-forget from the caller's perspective:
    // does its own connection per peer, best-effort, no client-visible ack
    // beyond the local commit (this demo does not block the client on
    // followers acknowledging, unlike full Raft's majority-commit rule —
    // see README tradeoffs).
    void replicateEntry(const WalRecord& rec);

private:
    void run(); // background thread: drives elections + leader heartbeats
    void runElection();
    void sendHeartbeats();
    void becomeFollower(int term);
    void becomeLeader();
    int randomElectionTimeoutMs();

    // Raw client helpers to talk to a peer over a fresh short-lived
    // connection per RPC — simplest correct approach for a demo; a real
    // system would keep persistent peer connections like Stage 2 did.
    bool sendRequestVote(const PeerConfig& peer, int term, int* out_term, bool* out_granted);
    bool sendAppendEntries(const PeerConfig& peer, int term,
                            const std::vector<WalRecord>& entries,
                            int* out_term, bool* out_ok);

    int my_id_;
    std::vector<PeerConfig> peers_;
    Store* store_;

    std::atomic<NodeState> state_{NodeState::FOLLOWER};
    std::atomic<int> current_term_{0};
    std::atomic<int> leader_id_{-1};
    int voted_for_ = -1; // guarded by mu_
    int voted_for_term_ = -1; // guarded by mu_

    std::atomic<bool> running_{false};
    std::thread worker_;

    std::mutex mu_;
    std::condition_variable cv_;
    std::chrono::steady_clock::time_point last_heartbeat_;

    std::mt19937 rng_{std::random_device{}()};
};
