// Simple line-based TCP server for the KV store.
//
// Client protocol (newline-terminated commands, space-separated):
//   SET <key> <value>\n   -> "OK\n" or "ERR <reason>\n"
//   GET <key>\n           -> "VALUE <value>\n" or "NOTFOUND\n"
//   DEL <key>\n           -> "OK\n"
//   STATUS\n              -> "ROLE <primary|standalone> KEYS <n> [REPLICA_CONNECTED <yes|no> PENDING <n>]\n"
//
// Replication protocol (same port, same connection loop — a replica is
// just a node that happens to receive these instead of client commands):
//   REPL_SET\t<klen>\t<key>\t<vlen>\t<value>\n  -> "REPL_OK\n"
//   REPL_DEL\t<klen>\t<key>\n                    -> "REPL_OK\n"
// Length-prefixed so keys/values may contain spaces or tabs safely.
//
// This is intentionally text-based and minimal (no RESP protocol, no
// pipelining) so the focus stays on persistence/replication/consensus
// rather than protocol design. One thread per connection — fine at this
// scale, documented as a known limitation for a "real" system.

#include <iostream>
#include <sstream>
#include <thread>
#include <cstring>
#include <csignal>
#include <atomic>
#include <memory>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include "store.hpp"
#include "replicator.hpp"
#include "raft.hpp"

static std::atomic<bool> g_shutdown{false};
static int g_server_fd = -1;

static void handle_signal(int) {
    // Signal handlers must stay async-signal-safe: no logging, no locks.
    // Just flip a flag and close the listening socket so accept() unblocks.
    g_shutdown.store(true);
    if (g_server_fd >= 0) {
        shutdown(g_server_fd, SHUT_RDWR);
    }
}

static void handle_client(int client_fd, Store* store, Replicator* replicator, RaftNode* raft) {
    char buf[4096];
    std::string pending;

    while (true) {
        ssize_t n = read(client_fd, buf, sizeof(buf));
        if (n <= 0) break; // client disconnected or error

        pending.append(buf, n);

        size_t pos;
        while ((pos = pending.find('\n')) != std::string::npos) {
            std::string line = pending.substr(0, pos);
            pending.erase(0, pos + 1);
            if (!line.empty() && line.back() == '\r') line.pop_back();

            std::istringstream iss(line);
            std::string cmd;
            iss >> cmd;

            std::string response;
            if (cmd == "SET") {
                if (raft && raft->state() != NodeState::LEADER) {
                    response = "ERR not leader, current leader is node " +
                               std::to_string(raft->leaderId()) + "\n";
                } else {
                    std::string key, value;
                    iss >> key;
                    std::getline(iss, value);
                    if (!value.empty() && value[0] == ' ') value.erase(0, 1);
                    if (key.empty()) {
                        response = "ERR missing key\n";
                    } else {
                        bool ok = store->set(key, value);
                        response = ok ? "OK\n" : "ERR write failed\n";
                    }
                }
            } else if (cmd == "GET") {
                std::string key;
                if (!(iss >> key)) {
                    response = "ERR missing key\n";
                } else {
                    auto val = store->get(key);
                    response = val.has_value() ? ("VALUE " + *val + "\n") : "NOTFOUND\n";
                }
            } else if (cmd == "DEL") {
                if (raft && raft->state() != NodeState::LEADER) {
                    response = "ERR not leader, current leader is node " +
                               std::to_string(raft->leaderId()) + "\n";
                } else {
                    std::string key;
                    if (!(iss >> key)) {
                        response = "ERR missing key\n";
                    } else {
                        bool ok = store->del(key);
                        response = ok ? "OK\n" : "ERR write failed\n";
                    }
                }
            } else if (cmd == "STATUS") {
                if (raft) {
                    std::string role = raft->state() == NodeState::LEADER ? "leader"
                                      : raft->state() == NodeState::CANDIDATE ? "candidate"
                                      : "follower";
                    response = "ROLE " + role + " TERM " + std::to_string(raft->currentTerm()) +
                               " LEADER_ID " + std::to_string(raft->leaderId()) +
                               " KEYS " + std::to_string(store->size()) + "\n";
                } else if (replicator) {
                    response = "ROLE primary KEYS " + std::to_string(store->size()) +
                               " REPLICA_CONNECTED " + (replicator->isConnected() ? "yes" : "no") +
                               " PENDING " + std::to_string(replicator->pendingCount()) + "\n";
                } else {
                    response = "ROLE standalone KEYS " + std::to_string(store->size()) + "\n";
                }
            } else if (cmd == "REQUEST_VOTE" && raft) {
                int term, candidate_id;
                iss >> term >> candidate_id;
                response = raft->handleRequestVote(term, candidate_id);
            } else if (cmd == "APPEND_ENTRIES" && raft) {
                int term, leader_id, count;
                iss >> term >> leader_id >> count;
                std::vector<WalRecord> entries;
                for (int i = 0; i < count; i++) {
                    std::string op;
                    iss >> op;
                    if (op == "SET") {
                        size_t klen, vlen;
                        std::string key, val;
                        iss >> klen;
                        iss.ignore(1);
                        key.resize(klen);
                        iss.read(&key[0], klen);
                        iss >> vlen;
                        iss.ignore(1);
                        val.resize(vlen);
                        iss.read(&val[0], vlen);
                        entries.push_back(WalRecord{WalOp::SET, key, val});
                    } else {
                        size_t klen;
                        std::string key;
                        iss >> klen;
                        iss.ignore(1);
                        key.resize(klen);
                        iss.read(&key[0], klen);
                        entries.push_back(WalRecord{WalOp::DEL, key, ""});
                    }
                }
                response = raft->handleAppendEntries(term, leader_id, entries);
            } else if (cmd == "REPL_SET") {
                // Length-prefixed fields so keys/values with spaces are safe.
                size_t klen, vlen;
                std::string key, val;
                iss >> klen;
                iss.ignore(1);
                key.resize(klen);
                iss.read(&key[0], klen);
                iss >> vlen;
                iss.ignore(1);
                val.resize(vlen);
                iss.read(&val[0], vlen);
                bool ok = store->applyReplicated(WalRecord{WalOp::SET, key, val});
                response = ok ? "REPL_OK\n" : "REPL_ERR\n";
            } else if (cmd == "REPL_DEL") {
                size_t klen;
                std::string key;
                iss >> klen;
                iss.ignore(1);
                key.resize(klen);
                iss.read(&key[0], klen);
                bool ok = store->applyReplicated(WalRecord{WalOp::DEL, key, ""});
                response = ok ? "REPL_OK\n" : "REPL_ERR\n";
            } else {
                response = "ERR unknown command\n";
            }

            ssize_t wn = write(client_fd, response.c_str(), response.size());
            if (wn < 0) break; // client gone; stop trying to serve it
        }
    }
    close(client_fd);
}

int main(int argc, char** argv) {
    int port = argc > 1 ? std::stoi(argv[1]) : 6380;
    std::string wal_path = argc > 2 ? argv[2] : "data/wal.log";

    // Optional 3rd arg is one of:
    //   --replica-of=HOST:PORT              (Stage 2: fixed primary/replica)
    //   --raft=MY_ID,ID:HOST:PORT,ID:HOST:PORT,...   (Stage 3: N-node cluster)
    // Mutually exclusive. Omit for a standalone node.
    std::unique_ptr<Replicator> replicator;
    std::unique_ptr<RaftNode> raft;
    Store* store_ptr = nullptr; // set once store exists, needed by raft closures below

    std::string raft_arg;
    if (argc > 3) {
        std::string arg = argv[3];
        const std::string replica_prefix = "--replica-of=";
        const std::string raft_prefix = "--raft=";
        if (arg.rfind(replica_prefix, 0) == 0) {
            std::string target = arg.substr(replica_prefix.size());
            size_t colon = target.find(':');
            if (colon != std::string::npos) {
                std::string rhost = target.substr(0, colon);
                int rport = std::stoi(target.substr(colon + 1));
                replicator = std::make_unique<Replicator>(rhost, rport);
            }
        } else if (arg.rfind(raft_prefix, 0) == 0) {
            raft_arg = arg.substr(raft_prefix.size());
        }
    }

    Store store(wal_path);
    store_ptr = &store;
    std::cout << "Recovering from WAL at " << wal_path << "...\n";
    store.recover();
    std::cout << "Recovered " << store.size() << " keys.\n";

    if (replicator) {
        replicator->start();
        store.onCommit([&replicator](const WalRecord& rec) {
            replicator->enqueue(rec);
        });
        std::cout << "Replicating commits to configured replica.\n";
    }

    if (!raft_arg.empty()) {
        // Parse "MY_ID,ID:HOST:PORT,ID:HOST:PORT,..."
        std::istringstream rss(raft_arg);
        std::string first;
        std::getline(rss, first, ',');
        int my_id = std::stoi(first);

        std::vector<PeerConfig> peers;
        std::string peer_tok;
        while (std::getline(rss, peer_tok, ',')) {
            size_t c1 = peer_tok.find(':');
            size_t c2 = peer_tok.find(':', c1 + 1);
            int pid = std::stoi(peer_tok.substr(0, c1));
            std::string phost = peer_tok.substr(c1 + 1, c2 - c1 - 1);
            int pport = std::stoi(peer_tok.substr(c2 + 1));
            peers.push_back(PeerConfig{phost, pport, pid});
        }

        raft = std::make_unique<RaftNode>(my_id, peers, store_ptr);

        // When this node is leader, fan every client-committed write out
        // to peers immediately via replicateEntry rather than waiting for
        // the next periodic heartbeat. If we're not currently leader,
        // replicateEntry is a no-op (see its implementation) — followers
        // don't get client writes in the first place, since handle_client
        // rejects SET/DEL with "not leader" unless raft->state() == LEADER.
        RaftNode* raft_ptr = raft.get();
        store.onCommit([raft_ptr](const WalRecord& rec) {
            raft_ptr->replicateEntry(rec);
        });

        raft->start();
        std::cout << "Raft node " << my_id << " started with " << peers.size() << " peer(s).\n";
    }

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "socket() failed\n";
        return 1;
    }
    g_server_fd = server_fd;

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(server_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "bind() failed on port " << port << "\n";
        return 1;
    }

    if (listen(server_fd, 16) < 0) {
        std::cerr << "listen() failed\n";
        return 1;
    }

    std::cout << "KV store listening on port " << port << "\n";

    while (!g_shutdown.load()) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            // Either a real error, or our own shutdown() unblocking accept().
            if (g_shutdown.load()) break;
            continue;
        }

        std::thread(handle_client, client_fd, &store, replicator.get(), raft.get()).detach();
    }

    close(server_fd);
    std::cout << "Shutting down gracefully.\n";
    // Note: detached client threads are not joined here. For a demo-scale
    // project this is an accepted simplification — documented in the
    // tradeoffs writeup rather than silently left unmentioned. A production
    // version would track threads (or use a thread pool) and join them
    // with a timeout before exiting.
    return 0;
}
