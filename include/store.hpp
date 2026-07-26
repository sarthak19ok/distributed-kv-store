#pragma once
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <optional>
#include <memory>
#include <functional>
#include "wal.hpp"

// The actual key-value store. Every write goes through the WAL first
// (durability), then updates the in-memory map (speed). Reads only touch
// the in-memory map — that's the whole point of keeping it in memory.
class Store {
public:
    explicit Store(const std::string& wal_path);

    // Replays the WAL to rebuild state. Call this once at startup.
    void recover();

    // Client-facing writes: append to WAL, apply to map, then (if this is a
    // primary) fan the record out to any registered replication callback.
    bool set(const std::string& key, const std::string& value);
    bool del(const std::string& key);
    std::optional<std::string> get(const std::string& key);

    // Replica-facing write path: apply a record that was received from the
    // primary. Still goes through this node's own WAL first, so a replica
    // can independently crash-recover — it's not just a cache of the
    // primary, it's a durable copy in its own right.
    bool applyReplicated(const WalRecord& rec);

    // Registers a callback invoked after every successful local commit
    // (set/del), so a primary can stream the record out to replicas.
    // Not called for applyReplicated (a replica doesn't re-forward writes
    // in this simplified single-primary design, avoiding replication loops).
    void onCommit(std::function<void(const WalRecord&)> cb);

    size_t size();

private:
    std::unique_ptr<WriteAheadLog> wal_;
    std::unordered_map<std::string, std::string> map_;
    std::shared_mutex mu_; // multiple readers, single writer
    std::function<void(const WalRecord&)> on_commit_;
};
