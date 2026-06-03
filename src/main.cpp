
// main.cpp — Distributed redis-lite Entry Point

#include "server.h"
#include "store.h"
#include "eviction.h"
#include "wal.h"
#include "resp.h"
#include "replication.h"
#include "replica_client.h"
#include "rdb.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <iostream>
#include <unordered_map>
#include <string>
#include <vector>
#include <csignal>
#include <cstring>
#include <cerrno>
#include "platform.h"
#include <memory>
#include <sstream>

// SERVER STATE
struct ServerConfig {
    int         port         = 6379;
    bool        is_replica   = false;
    std::string primary_host = "";
    int         primary_port = 0;
    std::string wal_path     = "appendonly.aof";
    size_t      cache_size   = 100000;
};

// Global state 
static EventLoop*                      g_loop       = nullptr;
static ReplicationManager*             g_repl_mgr   = nullptr;
static ReplicaClient*                  g_replica    = nullptr;
static bool                            g_is_replica = false;

// Per-client read buffers
static std::unordered_map<int, std::string> client_buffers;

// PARSE ARGS

ServerConfig parse_args(int argc, char* argv[]) {
    ServerConfig cfg;
    for (int i = 1; i < argc; i++) {
        std::string arg(argv[i]);
        if (arg == "--port" && i + 1 < argc)
            cfg.port = std::stoi(argv[++i]);
        else if (arg == "--replicaof" && i + 2 < argc) {
            cfg.is_replica   = true;
            cfg.primary_host = argv[++i];
            cfg.primary_port = std::stoi(argv[++i]);
        }
        else if (arg == "--role" && i + 1 < argc) {
            std::string role(argv[++i]);
            cfg.is_replica = (role == "replica");
        }
        else if (arg == "--wal" && i + 1 < argc)
            cfg.wal_path = argv[++i];
    }
    return cfg;
}


// BUILD INFO REPLICATION STRING

std::string build_replication_info() {
    std::ostringstream ss;
    if (!g_is_replica && g_repl_mgr) {
        // PRIMARY
        auto info = g_repl_mgr->get_info();
        ss << "# Replication\r\n"
           << "role:primary\r\n"
           << "connected_slaves:" << info.connected_replicas << "\r\n"
           << "master_replid:" << info.repl_id << "\r\n"
           << "master_repl_offset:" << info.master_repl_offset << "\r\n";
        for (size_t i = 0; i < info.replicas.size(); i++) {
            const auto& r = info.replicas[i];
            ss << "slave" << i << ":ip=" << r.host
               << ",port=" << r.port
               << ",offset=" << r.offset
               << ",lag=" << r.lag_bytes << "\r\n";
        }
    } else if (g_is_replica && g_replica) {
        // REPLICA
        auto info = g_replica->get_info();
        ss << "# Replication\r\n"
           << "role:slave\r\n"
           << "master_host:" << info.primary_host << "\r\n"
           << "master_port:" << info.primary_port << "\r\n"
           << "master_link_status:" << info.state << "\r\n"
           << "master_repl_offset:" << info.repl_offset << "\r\n"
           << "master_replid:" << info.primary_repl_id << "\r\n";
    } else {
        ss << "# Replication\r\nrole:primary\r\nconnected_slaves:0\r\n";
    }
    return ss.str();
}

// HANDLE CLIENT
void handle_client(int fd, Store& store, ClockPro& eviction,
                   WAL& wal, EventLoop& loop)
{
    RespParser parser;
    std::string& buf = client_buffers[fd];
    char tmp[4096];

    // Edge-triggered: drain all available data
    while (true) {
        ssize_t n = read(fd, tmp, sizeof(tmp));
        if (n == 0) {
            // Clean disconnect
            loop.del_fd(fd);
            close(fd);
            client_buffers.erase(fd);
            return;
        }
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            loop.del_fd(fd);
            close(fd);
            client_buffers.erase(fd);
            return;
        }
        buf.append(tmp, n);
    }

    while (true) {
        size_t consumed = 0;
        auto cmd = parser.parse(buf, consumed);
        if (cmd.empty()) break;
        buf.erase(0, consumed);

        if (cmd.size() == 1 && cmd[0] == "__ERROR__") {
            auto e = RespParser::error("Protocol error");
            send(fd, e.c_str(), e.size(), MSG_NOSIGNAL);
            continue;
        }
        if (cmd.empty()) continue;

        // Uppercase command name
        for (char& c : cmd[0]) c = toupper(c);

        std::string response;

        // READ-ONLY check for replicas
        static const std::vector<std::string> WRITE_CMDS = {
            "SET","DEL","EXPIRE","MSET","FLUSHALL","INCR","DECR"
        };
        if (g_is_replica) {
            bool is_write = false;
            for (const auto& wc : WRITE_CMDS)
                if (cmd[0] == wc) { is_write = true; break; }

            // REPLICAOF NO ONE is allowed on replica (promotes to primary)
            bool is_replicaof_no_one = (cmd[0] == "REPLICAOF"
                && cmd.size() == 3
                && cmd[1] == "NO" && cmd[2] == "ONE");

            if (is_write && !is_replicaof_no_one) {
                response = RespParser::error(
                    "READONLY You can't write against a read only replica.");
                send(fd, response.c_str(), response.size(), MSG_NOSIGNAL);
                continue;
            }
        }

        // COMMAND DISPATCH

        if (cmd[0] == "PING") {
            response = (cmd.size() > 1)
                ? RespParser::bulk(cmd[1])
                : RespParser::pong();
        }

        else if (cmd[0] == "SET" && cmd.size() >= 3) {
            std::optional<int> ttl;
            for (size_t i = 3; i + 1 < cmd.size(); i++) {
                std::string flag = cmd[i];
                for (char& c : flag) c = toupper(c);
                if (flag == "EX") ttl = std::stoi(cmd[i+1]);
                else if (flag == "PX") ttl = std::stoi(cmd[i+1]) / 1000;
            }
            store.set(cmd[1], cmd[2], ttl);
            std::string evicted = eviction.on_insert(cmd[1]);
            if (!evicted.empty()) store.del(evicted);
            wal.log(cmd);

            // Propagate to replicas if we are the primary
            if (!g_is_replica && g_repl_mgr)
                g_repl_mgr->propagate(cmd);

            response = RespParser::ok();
        }

        else if (cmd[0] == "GET" && cmd.size() == 2) {
            auto val = store.get(cmd[1]);
            if (val) {
                eviction.on_access(cmd[1]);
                response = RespParser::bulk(*val);
            } else {
                response = RespParser::nil();
            }
        }

        else if (cmd[0] == "DEL" && cmd.size() >= 2) {
            int64_t count = 0;
            for (size_t i = 1; i < cmd.size(); i++) {
                if (store.del(cmd[i])) {
                    eviction.on_delete(cmd[i]);
                    count++;
                }
            }
            wal.log(cmd);
            if (!g_is_replica && g_repl_mgr) g_repl_mgr->propagate(cmd);
            response = RespParser::integer(count);
        }

        else if (cmd[0] == "EXISTS" && cmd.size() == 2) {
            response = RespParser::integer(store.exists(cmd[1]) ? 1 : 0);
        }

        else if (cmd[0] == "EXPIRE" && cmd.size() == 3) {
            bool ok = store.expire(cmd[1], std::stoi(cmd[2]));
            wal.log(cmd);
            if (!g_is_replica && g_repl_mgr) g_repl_mgr->propagate(cmd);
            response = RespParser::integer(ok ? 1 : 0);
        }

        else if (cmd[0] == "TTL" && cmd.size() == 2) {
            response = RespParser::integer(store.ttl(cmd[1]));
        }

        else if (cmd[0] == "DBSIZE") {
            response = RespParser::integer((int64_t)store.dbsize());
        }

        else if (cmd[0] == "MSET" && cmd.size() >= 3 && cmd.size() % 2 == 1) {
            for (size_t i = 1; i + 1 < cmd.size(); i += 2) {
                store.set(cmd[i], cmd[i+1]);
                eviction.on_insert(cmd[i]);
            }
            wal.log(cmd);
            if (!g_is_replica && g_repl_mgr) g_repl_mgr->propagate(cmd);
            response = RespParser::ok();
        }

        else if (cmd[0] == "MGET" && cmd.size() >= 2) {
            response = "*" + std::to_string(cmd.size() - 1) + "\r\n";
            for (size_t i = 1; i < cmd.size(); i++) {
                auto val = store.get(cmd[i]);
                if (val) { eviction.on_access(cmd[i]); response += RespParser::bulk(*val); }
                else     { response += RespParser::nil(); }
            }
        }

        else if (cmd[0] == "FLUSHALL") {
            store.flushall();
            wal.log({"FLUSHALL"});
            if (!g_is_replica && g_repl_mgr) g_repl_mgr->propagate({"FLUSHALL"});
            response = RespParser::ok();
        }

        // PSYNC — replica handshake 
        else if (cmd[0] == "PSYNC" && cmd.size() == 3 && !g_is_replica) {
            if (!g_repl_mgr) {
                response = RespParser::error("Not a primary");
            } else {
                std::string replid = cmd[1];
                int64_t offset = -1;
                try { offset = std::stoll(cmd[2]); } catch (...) {}

                // Get peer address for logging
                std::string peer_host = "127.0.0.1";
                int         peer_port = 0;

                // Remove fd from epoll event loop — replication manager
                loop.del_fd(fd);
                client_buffers.erase(fd);

                g_repl_mgr->handle_psync(fd, peer_host, peer_port,
                                         replid, offset);
                // Don't send a response — handle_psync sends its own
                continue;
            }
        }

        //  REPLICAOF host port — become a replica
        //  REPLICAOF NO ONE  — become a primary
        else if (cmd[0] == "REPLICAOF" && cmd.size() == 3) {
            if (cmd[1] == "NO" && cmd[2] == "ONE") {
                // Promote replica to primary
                if (g_replica) {
                    g_replica->stop();
                    delete g_replica;
                    g_replica = nullptr;
                }
                g_is_replica = false;
                std::cout << "[server] Promoted to primary\n";
                response = RespParser::ok();
            } else {
                // Become a replica of host:port
                std::string new_host = cmd[1];
                int         new_port = std::stoi(cmd[2]);

                if (g_replica) {
                    g_replica->stop();
                    delete g_replica;
                    g_replica = nullptr;
                }

                g_is_replica = true;
                g_replica = new ReplicaClient(store, new_host, new_port,
                    [&wal](const std::vector<std::string>& c) {
                        // Log replicated commands to our WAL too
                        wal.log(c);
                    });
                g_replica->start();

                std::cout << "[server] Now replicating from "
                          << new_host << ":" << new_port << "\n";
                response = RespParser::ok();
            }
        }

        //  INFO
        else if (cmd[0] == "INFO") {
            std::string info =
                "# Server\r\n"
                "server_name:redis-lite\r\n"
                "version:2.0.0\r\n"
                "os:Linux\r\n"
                "architecture:x86_64\r\n"
                "# Storage\r\n"
                "hash_table:hopscotch\r\n"
                "eviction_policy:clock-pro\r\n"
                "io_model:epoll-edge-triggered\r\n"
                "persistence:wal-group-commit\r\n"
                "keys:" + std::to_string(store.dbsize()) + "\r\n"
                + build_replication_info();
            response = RespParser::bulk(info);
        }

        else if (cmd[0] == "COMMAND") {
            response = "*0\r\n";
        }

        else {
            response = RespParser::error(
                "unknown command '" + cmd[0] + "'");
        }

        send(fd, response.c_str(), response.size(), MSG_NOSIGNAL);
    }
}


// SIGNAL HANDLER
void signal_handler(int) {
    std::cout << "\n[server] Shutting down...\n";
    if (g_loop) g_loop->stop();
}


// MAIN
int main(int argc, char* argv[]) {
    platform_init();  
    ServerConfig cfg = parse_args(argc, argv);
    g_is_replica = cfg.is_replica;

    std::cout << "╔═══════════════════════════════════════════╗\n";
    std::cout << "║      distributed-in-memory-kv-store v1.0       ║\n";
    std::cout << "║  Hash: Hopscotch  Eviction: CLOCK-Pro        ║\n";
    std::cout << "║  I/O: epoll ET    WAL: Group Commit          ║\n";
    std::cout << "║  Replication: Async Primary-Replica          ║\n";
    std::cout << "╚══════════════════════════════════════════════╝\n";
    std::cout << "[init] Role: " << (cfg.is_replica ? "REPLICA" : "PRIMARY")
              << " | Port: " << cfg.port << "\n";

    // Components
    Store    store;
    ClockPro eviction(cfg.cache_size);
    WAL      wal(cfg.wal_path, 2);

    // Restore state from WAL
    for (const auto& cmd : wal.replay()) {
        if (cmd.empty()) continue;
        std::string name = cmd[0];
        for (char& c : name) c = toupper(c);
        if (name == "SET" && cmd.size() >= 3) {
            std::optional<int> ttl;
            for (size_t i = 3; i + 1 < cmd.size(); i++)
                if (cmd[i] == "EX") ttl = std::stoi(cmd[i+1]);
            store.set(cmd[1], cmd[2], ttl);
            eviction.on_insert(cmd[1]);
        }
        else if (name == "DEL" && cmd.size() >= 2)
            for (size_t i = 1; i < cmd.size(); i++) store.del(cmd[i]);
        else if (name == "EXPIRE" && cmd.size() == 3)
            store.expire(cmd[1], std::stoi(cmd[2]));
        else if (name == "FLUSHALL")
            store.flushall();
    }
    std::cout << "[init] Restored " << store.dbsize() << " keys from WAL\n";

    // Primary: start replication manager 
    std::unique_ptr<ReplicationManager> repl_mgr;
    if (!cfg.is_replica) {
        repl_mgr = std::make_unique<ReplicationManager>(store);
        g_repl_mgr = repl_mgr.get();
        std::cout << "[init] Replication manager started\n";
    }

    // Replica: connect to primary 
    std::unique_ptr<ReplicaClient> replica_client;
    if (cfg.is_replica) {
        replica_client = std::make_unique<ReplicaClient>(
            store,
            cfg.primary_host,
            cfg.primary_port,
            [&wal](const std::vector<std::string>& cmd) {
                wal.log(cmd);  // log replicated commands to our WAL
            }
        );
        g_replica = replica_client.get();
        replica_client->start();
        std::cout << "[init] Replica started, connecting to "
                  << cfg.primary_host << ":" << cfg.primary_port << "\n";
    }

    // Event loop 
    EventLoop loop;
    g_loop = &loop;
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    int listen_fd = create_server_socket(cfg.port);
    std::cout << "[ready] Listening on :" << cfg.port << "\n";
    if (cfg.is_replica)
        std::cout << "[ready] Mode: READ-ONLY replica (writes rejected)\n";
    else
        std::cout << "[ready] Mode: PRIMARY (accepts reads + writes)\n";
    std::cout << "[ready] Connect with: redis-cli -p " << cfg.port << "\n\n";

    // Accept new connections
    loop.add_fd(listen_fd, EV_READ, [&](int fd, uint32_t) {
        while (true) {
            sockaddr_in client_addr{};
            socklen_t addr_len = sizeof(client_addr);
            int cfd = accept(fd, (sockaddr*)&client_addr, &addr_len);
            if (cfd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                break;
            }
            std::cout << "[server] Client fd=" << cfd << " connected\n";
            loop.add_fd(cfd, EV_READ | EV_EDGE | EV_HANGUP,
                [&, cfd](int, uint32_t events) {
                    if (events & (EV_HANGUP | EV_ERROR)) {
                        loop.del_fd(cfd);
                        close(cfd);
                        client_buffers.erase(cfd);
                        return;
                    }
                    handle_client(cfd, store, eviction, wal, loop);
                });
        }
    });

    loop.run();

    //  Shutdown
    std::cout << "[server] Flushing WAL...\n";
    wal.flush_now();
    wal.stop();
    if (replica_client) replica_client->stop();
    close(listen_fd);
    std::cout << "[server] Bye.\n";
    return 0;
}
