#include "wal.hpp"
#include <iostream>
#include <unistd.h>

WriteAheadLog::WriteAheadLog(const std::string& path) : path_(path) {
    // "a+" so we append on write but can still read from the start on replay.
    fp_ = std::fopen(path_.c_str(), "a+");
    if (!fp_) {
        std::cerr << "FATAL: could not open WAL at " << path_ << "\n";
    }
}

WriteAheadLog::~WriteAheadLog() {
    if (fp_) std::fclose(fp_);
}

bool WriteAheadLog::append(const WalRecord& rec) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!fp_) return false;

    int written;
    if (rec.op == WalOp::SET) {
        written = std::fprintf(fp_, "SET\t%zu\t%s\t%zu\t%s\n",
                                rec.key.size(), rec.key.c_str(),
                                rec.value.size(), rec.value.c_str());
    } else {
        written = std::fprintf(fp_, "DEL\t%zu\t%s\n",
                                rec.key.size(), rec.key.c_str());
    }
    if (written < 0) return false;

    // fflush pushes libc's buffer to the OS; fsync then forces the OS to
    // push that all the way to physical disk. Both are needed — without
    // fflush, fsync has nothing to sync; without fsync, a page-cache-only
    // write can still vanish on power loss.
    if (std::fflush(fp_) != 0) return false;
    if (fsync(fileno(fp_)) != 0) return false;

    return true;
}

void WriteAheadLog::replay(const std::function<void(const WalRecord&)>& cb) {
    std::lock_guard<std::mutex> lock(mu_);
    std::ifstream in(path_);
    if (!in.is_open()) return; // no log yet, nothing to replay

    std::string op;
    while (in >> op) {
        if (op == "SET") {
            size_t klen, vlen;
            std::string key, val;
            if (!(in >> klen)) break;
            in.ignore(1); // tab
            key.resize(klen);
            in.read(&key[0], klen);
            if (!(in >> vlen)) break;
            in.ignore(1);
            val.resize(vlen);
            in.read(&val[0], vlen);
            in.ignore(1); // trailing newline
            if (!in.good() && !in.eof()) break; // malformed/truncated record
            cb(WalRecord{WalOp::SET, key, val});
        } else if (op == "DEL") {
            size_t klen;
            std::string key;
            if (!(in >> klen)) break;
            in.ignore(1);
            key.resize(klen);
            in.read(&key[0], klen);
            in.ignore(1);
            if (!in.good() && !in.eof()) break;
            cb(WalRecord{WalOp::DEL, key, ""});
        } else {
            break; // unrecognized record, stop rather than misparse further
        }
    }
}

void WriteAheadLog::truncate() {
    std::lock_guard<std::mutex> lock(mu_);
    if (fp_) std::fclose(fp_);
    fp_ = std::fopen(path_.c_str(), "w+");
}
