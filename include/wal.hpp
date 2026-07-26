#pragma once
#include <string>
#include <fstream>
#include <functional>
#include <mutex>
#include <cstdio>

// Write-Ahead Log
//
// Every mutating operation (SET/DELETE) is appended here BEFORE it is applied
// to the in-memory map, and fsync'd (real fsync(2), not just a libc flush)
// before we tell the caller it succeeded. This is what lets us survive a
// crash: on restart we replay the whole log and rebuild in-memory state
// from scratch. fsync matters because a flush only pushes bytes to the OS
// page cache — a power loss or OS crash (not just a process crash) could
// still lose unfsync'd writes.
//
// Format per record (text-based for easy debugging/inspection):
//   SET\t<key_len>\t<key>\t<val_len>\t<val>\n
//   DEL\t<key_len>\t<key>\n
//
// Trade-off: text format is slower and bigger on disk than a binary format,
// but it's trivial to `cat` the log while debugging replication issues,
// which matters a lot more at this project's scale than raw throughput.

enum class WalOp { SET, DEL };

struct WalRecord {
    WalOp op;
    std::string key;
    std::string value; // unused for DEL
};

class WriteAheadLog {
public:
    explicit WriteAheadLog(const std::string& path);
    ~WriteAheadLog();

    // Appends a record and fsyncs before returning. Returns false on I/O error.
    bool append(const WalRecord& rec);

    // Replays every record in the log, invoking cb(rec) for each, in order.
    // Used both for crash recovery and for streaming to a fresh replica.
    void replay(const std::function<void(const WalRecord&)>& cb);

    // Truncates the log file. Called after a snapshot has been taken, so we
    // don't replay entries already captured in the snapshot.
    void truncate();

private:
    std::string path_;
    FILE* fp_ = nullptr;
    std::mutex mu_;
};
