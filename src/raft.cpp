#include "raft.hpp"
#include <iostream>
#include <sstream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

RaftNode::RaftNode(int my_id, std::vector<PeerConfig> peers, Store* store)
    : my_id_(my_id), peers_(std::move(peers)), store_(store) {
    last_heartbeat_ = std::chrono::steady_clock::now();
}

RaftNode::~RaftNode() { stop(); }

void RaftNode::start() {
    running_.store(true);
    worker_ = std::thread(&RaftNode::run, this);
}

void RaftNode::stop() {
    if (!running_.exchange(false)) return;
    cv_.notify_all();
    if (worker_.joinable()) worker_.join();
}

int RaftNode::randomElectionTimeoutMs() {
    // Randomized within a range so that not all followers time out
    // simultaneously and split the vote every single election — this
    // spread is what makes Raft's leader election converge quickly in
    // practice rather than looping through repeated ties.
    std::uniform_int_distribution<int> dist(1500, 3000);
    return dist(rng_);
}

void RaftNode::becomeFollower(int term) {
    current_term_.store(term);
    state_.store(NodeState::FOLLOWER);
    std::lock_guard<std::mutex> lock(mu_);
    last_heartbeat_ = std::chrono::steady_clock::now();
}

void RaftNode::becomeLeader() {
    state_.store(NodeState::LEADER);
    leader_id_.store(my_id_);
    std::cerr << "[raft] node " << my_id_ << " became LEADER for term "
              << current_term_.load() << "\n";
}

std::string RaftNode::handleRequestVote(int term, int candidate_id) {
    std::lock_guard<std::mutex> lock(mu_);

    if (term < current_term_.load()) {
        return "VOTE_DENIED\t" + std::to_string(current_term_.load()) + "\n";
    }

    if (term > current_term_.load()) {
        // Newer term discovered: step down and reset our vote for this
        // new term, per Raft's rule that any RPC carrying a higher term
        // forces the recipient to become a follower of that term.
        current_term_.store(term);
        state_.store(NodeState::FOLLOWER);
        voted_for_ = -1;
        voted_for_term_ = -1;
    }

    bool can_vote = (voted_for_term_ != term) || (voted_for_ == candidate_id);
    if (can_vote) {
        voted_for_ = candidate_id;
        voted_for_term_ = term;
        last_heartbeat_ = std::chrono::steady_clock::now(); // granting a vote also resets our timer
        return "VOTE_GRANTED\t" + std::to_string(term) + "\n";
    }
    return "VOTE_DENIED\t" + std::to_string(current_term_.load()) + "\n";
}

std::string RaftNode::handleAppendEntries(int term, int leader_id,
                                           const std::vector<WalRecord>& entries) {
    if (term < current_term_.load()) {
        return "APPEND_FAIL\t" + std::to_string(current_term_.load()) + "\n";
    }

    {
        std::lock_guard<std::mutex> lock(mu_);
        current_term_.store(term);
        state_.store(NodeState::FOLLOWER);
        leader_id_.store(leader_id);
        last_heartbeat_ = std::chrono::steady_clock::now();
    }

    for (const auto& rec : entries) {
        store_->applyReplicated(rec);
    }

    return "APPEND_OK\t" + std::to_string(term) + "\n";
}

bool RaftNode::sendRequestVote(const PeerConfig& peer, int term, int* out_term, bool* out_granted) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    struct timeval tv{}; tv.tv_sec = 1; tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(peer.port);
    inet_pton(AF_INET, peer.host.c_str(), &addr.sin_addr);

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return false;
    }

    std::string msg = "REQUEST_VOTE\t" + std::to_string(term) + "\t" + std::to_string(my_id_) + "\n";
    if (send(fd, msg.c_str(), msg.size(), 0) != (ssize_t)msg.size()) {
        close(fd);
        return false;
    }

    char buf[128];
    ssize_t n = recv(fd, buf, sizeof(buf) - 1, 0);
    close(fd);
    if (n <= 0) return false;
    buf[n] = '\0';

    std::string reply(buf);
    std::istringstream iss(reply);
    std::string tag;
    int t;
    iss >> tag >> t;
    *out_term = t;
    *out_granted = (tag == "VOTE_GRANTED");
    return true;
}

bool RaftNode::sendAppendEntries(const PeerConfig& peer, int term,
                                  const std::vector<WalRecord>& entries,
                                  int* out_term, bool* out_ok) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    struct timeval tv{}; tv.tv_sec = 1; tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(peer.port);
    inet_pton(AF_INET, peer.host.c_str(), &addr.sin_addr);

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return false;
    }

    std::ostringstream oss;
    oss << "APPEND_ENTRIES\t" << term << "\t" << my_id_ << "\t" << entries.size();
    for (const auto& rec : entries) {
        if (rec.op == WalOp::SET) {
            oss << "\tSET\t" << rec.key.size() << "\t" << rec.key
                << "\t" << rec.value.size() << "\t" << rec.value;
        } else {
            oss << "\tDEL\t" << rec.key.size() << "\t" << rec.key;
        }
    }
    oss << "\n";
    std::string msg = oss.str();

    if (send(fd, msg.c_str(), msg.size(), 0) != (ssize_t)msg.size()) {
        close(fd);
        return false;
    }

    char buf[128];
    ssize_t n = recv(fd, buf, sizeof(buf) - 1, 0);
    close(fd);
    if (n <= 0) return false;
    buf[n] = '\0';

    std::string reply(buf);
    std::istringstream iss(reply);
    std::string tag;
    int t;
    iss >> tag >> t;
    *out_term = t;
    *out_ok = (tag == "APPEND_OK");
    return true;
}

void RaftNode::runElection() {
    int term = current_term_.fetch_add(1) + 1;
    state_.store(NodeState::CANDIDATE);
    {
        std::lock_guard<std::mutex> lock(mu_);
        voted_for_ = my_id_;
        voted_for_term_ = term;
    }
    std::cerr << "[raft] node " << my_id_ << " starting election for term " << term << "\n";

    int votes = 1; // vote for self
    int majority = (int)(peers_.size() + 1) / 2 + 1;

    for (const auto& peer : peers_) {
        if (state_.load() != NodeState::CANDIDATE) break;

        int reply_term = 0;
        bool granted = false;
        if (sendRequestVote(peer, term, &reply_term, &granted)) {
            if (reply_term > term) {
                becomeFollower(reply_term);
                return;
            }
            if (granted) votes++;
        }
    }

    if (state_.load() == NodeState::CANDIDATE && votes >= majority) {
        becomeLeader();
    } else if (state_.load() == NodeState::CANDIDATE) {
        becomeFollower(term);
    }
}

void RaftNode::replicateEntry(const WalRecord& rec) {
    // Only meaningful if we're the leader; a stale/former leader calling
    // this after losing an election just wastes a few connection attempts
    // that peers will reject once term comparison fails their AppendEntries
    // check. Cheap enough not to guard here.
    if (state_.load() != NodeState::LEADER) return;

    int term = current_term_.load();
    std::vector<WalRecord> entries{rec};

    for (const auto& peer : peers_) {
        int reply_term = 0;
        bool ok = false;
        if (sendAppendEntries(peer, term, entries, &reply_term, &ok)) {
            if (reply_term > term) {
                becomeFollower(reply_term);
                return;
            }
        }
        // Best-effort: an unreachable/lagging follower just misses this
        // entry until the next successful AppendEntries (heartbeat or
        // another client write) catches it up. No majority-ack gating on
        // the client's response — that's the biggest simplification vs
        // full Raft, documented in raft.hpp and the README.
    }
}

void RaftNode::sendHeartbeats() {
    int term = current_term_.load();
    for (const auto& peer : peers_) {
        int reply_term = 0;
        bool ok = false;
        if (sendAppendEntries(peer, term, {}, &reply_term, &ok)) {
            if (reply_term > term) {
                becomeFollower(reply_term);
                return;
            }
        }
    }
}

void RaftNode::run() {
    const auto heartbeat_interval = std::chrono::milliseconds(500);
    int timeout_ms = randomElectionTimeoutMs();

    while (running_.load()) {
        if (state_.load() == NodeState::LEADER) {
            sendHeartbeats();
            std::this_thread::sleep_for(heartbeat_interval);
            // Pick a fresh timeout for whenever we next become a follower.
            timeout_ms = randomElectionTimeoutMs();
            continue;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        std::chrono::steady_clock::time_point last;
        {
            std::lock_guard<std::mutex> lock(mu_);
            last = last_heartbeat_;
        }
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - last).count();

        if (elapsed >= timeout_ms && running_.load()) {
            runElection();
            // Fresh randomized timeout for the next round, whether this
            // election won, lost, or split — avoids repeated synchronized
            // retries between competing candidates.
            timeout_ms = randomElectionTimeoutMs();
        }
    }
}
