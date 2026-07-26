#include "replicator.hpp"
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

Replicator::Replicator(std::string host, int port)
    : host_(std::move(host)), port_(port) {}

Replicator::~Replicator() { stop(); }

void Replicator::start() {
    running_.store(true);
    worker_ = std::thread(&Replicator::run, this);
}

void Replicator::stop() {
    if (!running_.exchange(false)) return;
    q_cv_.notify_all();
    // If the worker is mid-recv() waiting for an ack, it won't notice
    // running_ flipping until the 2s SO_RCVTIMEO fires. Bounded and
    // acceptable for a demo; a tighter shutdown would additionally
    // shutdown(replica_fd_) here to unblock recv() immediately.
    if (worker_.joinable()) worker_.join();
}

void Replicator::enqueue(const WalRecord& rec) {
    {
        std::lock_guard<std::mutex> lock(q_mu_);
        queue_.push(rec);
    }
    q_cv_.notify_one();
}

size_t Replicator::pendingCount() {
    std::lock_guard<std::mutex> lock(q_mu_);
    return queue_.size();
}

bool Replicator::connectToReplica() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    if (inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) <= 0) {
        close(fd);
        return false;
    }

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return false;
    }

    // A dead peer often won't fail send()/recv() immediately — TCP has no
    // way to know the other side is gone until a retransmit times out,
    // which can take a long while. A receive timeout bounds how long we
    // wait for an ack before treating the connection as broken.
    struct timeval tv{};
    tv.tv_sec = 2;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    replica_fd_ = fd;
    connected_.store(true);
    return true;
}

bool Replicator::sendRecord(int fd, const WalRecord& rec) {
    std::string line;
    if (rec.op == WalOp::SET) {
        line = "REPL_SET\t" + std::to_string(rec.key.size()) + "\t" + rec.key +
               "\t" + std::to_string(rec.value.size()) + "\t" + rec.value + "\n";
    } else {
        line = "REPL_DEL\t" + std::to_string(rec.key.size()) + "\t" + rec.key + "\n";
    }
    ssize_t sent = send(fd, line.c_str(), line.size(), 0);
    if (sent != (ssize_t)line.size()) return false;

    // A successful send() only means the bytes reached the LOCAL kernel
    // send buffer — not that the replica received or applied them. Waiting
    // for the application-level "REPL_OK\n" ack is what actually confirms
    // the replica processed this record. Without this, killing a replica
    // with SIGKILL would look identical to a healthy connection from the
    // primary's point of view until a much later retransmit timeout.
    char ackbuf[64];
    ssize_t n = recv(fd, ackbuf, sizeof(ackbuf) - 1, 0);
    if (n <= 0) return false; // timed out, connection reset, or EOF
    ackbuf[n] = '\0';
    std::string ack(ackbuf);
    return ack.find("REPL_OK") != std::string::npos;
}

void Replicator::run() {
    const auto retry_delay = std::chrono::milliseconds(1000);

    while (running_.load()) {
        if (replica_fd_ < 0) {
            if (!connectToReplica()) {
                std::cerr << "[replicator] could not connect to "
                          << host_ << ":" << port_ << ", retrying...\n";
                std::this_thread::sleep_for(retry_delay);
                continue;
            }
            std::cerr << "[replicator] connected to replica "
                      << host_ << ":" << port_ << "\n";
        }

        WalRecord rec;
        {
            std::unique_lock<std::mutex> lock(q_mu_);
            q_cv_.wait(lock, [this] { return !queue_.empty() || !running_.load(); });
            if (!running_.load() && queue_.empty()) break;
            rec = queue_.front();
            queue_.pop();
        }

        if (!sendRecord(replica_fd_, rec)) {
            std::cerr << "[replicator] send failed, will reconnect\n";
            close(replica_fd_);
            replica_fd_ = -1;
            connected_.store(false);
            // Put the record back so we don't silently drop it — it'll be
            // resent once we reconnect. Documented limitation: if the
            // primary crashes before this resend succeeds, the replica
            // never gets it (see class-level comment on catch-up).
            std::lock_guard<std::mutex> lock(q_mu_);
            queue_.push(rec);
        }
    }

    if (replica_fd_ >= 0) close(replica_fd_);
}
