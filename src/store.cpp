#include "store.hpp"

Store::Store(const std::string& wal_path) {
    wal_ = std::make_unique<WriteAheadLog>(wal_path);
}

void Store::recover() {
    std::unique_lock lock(mu_);
    wal_->replay([this](const WalRecord& rec) {
        if (rec.op == WalOp::SET) {
            map_[rec.key] = rec.value;
        } else {
            map_.erase(rec.key);
        }
    });
}

void Store::onCommit(std::function<void(const WalRecord&)> cb) {
    on_commit_ = std::move(cb);
}

bool Store::set(const std::string& key, const std::string& value) {
    // Durability first: if we crash between the WAL write and the map
    // update, recovery will replay the WAL and reach the same state.
    WalRecord rec{WalOp::SET, key, value};
    if (!wal_->append(rec)) {
        return false;
    }
    {
        std::unique_lock lock(mu_);
        map_[key] = value;
    }
    // Fan out to replicas AFTER the local commit is durable. This is the
    // crux of the async-replication tradeoff: the client already got their
    // local ack; replication happens "best effort" after the fact. See
    // README for the consistency/availability discussion.
    if (on_commit_) on_commit_(rec);
    return true;
}

bool Store::del(const std::string& key) {
    WalRecord rec{WalOp::DEL, key, ""};
    if (!wal_->append(rec)) {
        return false;
    }
    {
        std::unique_lock lock(mu_);
        map_.erase(key);
    }
    if (on_commit_) on_commit_(rec);
    return true;
}

bool Store::applyReplicated(const WalRecord& rec) {
    // Same durability discipline as a primary-side write: this replica's
    // own WAL is updated first, so the replica can recover independently
    // even if the primary is gone by the time this replica restarts.
    if (!wal_->append(rec)) return false;
    std::unique_lock lock(mu_);
    if (rec.op == WalOp::SET) {
        map_[rec.key] = rec.value;
    } else {
        map_.erase(rec.key);
    }
    return true;
}

std::optional<std::string> Store::get(const std::string& key) {
    std::shared_lock lock(mu_);
    auto it = map_.find(key);
    if (it == map_.end()) return std::nullopt;
    return it->second;
}

size_t Store::size() {
    std::shared_lock lock(mu_);
    return map_.size();
}
