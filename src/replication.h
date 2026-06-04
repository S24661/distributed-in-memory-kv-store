#pragma once

// replication.h — Primary-side Replication Manager

#include "store.h"
#include "rdb.h"
#include <string>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <unordered_map>
#include <deque>

// ReplicaConn: one connected replica
struct ReplicaConn {
    int         fd;
    std::string host;
    int         port;
    std::atomic<int64_t> offset{0};   // how many bytes this replica has received
    std::atomic<bool>    alive{true};
    std::thread          send_thread;  // dedicated sender thread per replica

    // Per-replica send queue 
    std::deque<std::string> send_queue;
    std::mutex              queue_mutex;
    std::condition_variable queue_cv;

    ReplicaConn(int fd, const std::string& h, int p)
        : fd(fd), host(h), port(p) {}

    ~ReplicaConn() {
        alive = false;
        queue_cv.notify_all();
        if (send_thread.joinable()) send_thread.join();
    }
};

// RingBuffer: fixed-size circular command backlog 
class RingBuffer {
public:
    explicit RingBuffer(size_t capacity);

    // Append data to the ring buffer
    void write(const std::string& data);

    // Read len bytes starting at offset
    bool read(int64_t offset, size_t len, std::string& out) const;

    // Current write position 
    int64_t current_offset() const { return write_offset_; }

    // True if offset is still in the buffer
    bool contains_offset(int64_t offset) const;

private:
    std::vector<char>    buf_;
    size_t               capacity_;
    size_t               head_ = 0;         // oldest byte position in buf_
    std::atomic<int64_t> write_offset_{0};  // total bytes written ever
    mutable std::mutex   mu_;
};

//  ReplicationManager: runs on the primary
class ReplicationManager {
public:
    // capacity_bytes: size of replication backlog (default 1MB)
    explicit ReplicationManager(Store& store,
                                size_t backlog_bytes = 1024 * 1024);
    ~ReplicationManager();

    // Called when a new replica connects and sends PSYNC.
    bool handle_psync(int replica_fd,
                      const std::string& host, int port,
                      const std::string& replid, int64_t offset);

    void propagate(const std::vector<std::string>& cmd);

    // Remove a disconnected replica
    void remove_replica(int fd);

    // Info for INFO replication command
    struct ReplicationInfo {
        std::string role = "primary";
        std::string repl_id;
        int64_t     master_repl_offset = 0;
        int         connected_replicas = 0;
        struct ReplicaInfo {
            std::string host;
            int         port;
            int64_t     offset;
            int64_t     lag_bytes;
        };
        std::vector<ReplicaInfo> replicas;
    };
    ReplicationInfo get_info() const;

    // Unique ID for this primary's replication stream
    const std::string& repl_id() const { return repl_id_; }
    int64_t            repl_offset() const { return backlog_.current_offset(); }

private:
    Store&      store_;
    RingBuffer  backlog_;
    std::string repl_id_;   // 40-char hex ID uniquely identifying this stream

    mutable std::mutex                            replicas_mutex_;
    std::vector<std::unique_ptr<ReplicaConn>>     replicas_;

    // Serialize a command to RESP for wire transmission
    static std::string serialize_cmd(const std::vector<std::string>& cmd);

    // Generate a random 40-char hex replication ID
    static std::string generate_repl_id();

    // Send full RDB snapshot to a new replica, then add to propagation list
    void full_sync(ReplicaConn& replica);

    // Dedicated sender loop for one replica
    void sender_loop(ReplicaConn* replica);
};
