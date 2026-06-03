#pragma once
// replica_client.h — Replica-side Replication Client

#include "store.h"
#include "resp.h"
#include <string>
#include <atomic>
#include <thread>
#include <mutex>
#include <functional>

enum class ReplicaState {
    DISCONNECTED,   // not connected to any primary
    CONNECTING,     // TCP connect in progress
    SYNCING,        // receiving RDB snapshot
    STREAMING,      // receiving live command stream
};

class ReplicaClient {
public:
    // on_command: called for each replicated command
    using CommandCallback = std::function<void(
        const std::vector<std::string>& cmd)>;

    ReplicaClient(Store& store, const std::string& primary_host,
                  int primary_port, CommandCallback on_command = nullptr);
    ~ReplicaClient();

    // Start background replication thread
    void start();

    // Stop replication
    void stop();

    // Current state
    ReplicaState state() const { return state_.load(); }
    bool         is_ready() const {
        return state_ == ReplicaState::STREAMING;
    }

    // Info for INFO replication
    struct Info {
        std::string role           = "replica";
        std::string primary_host;
        int         primary_port   = 0;
        std::string primary_repl_id;
        int64_t     repl_offset    = 0;
        std::string state;
    };
    Info get_info() const;

    // Replication offset (how many bytes received from primary)
    int64_t repl_offset() const { return repl_offset_; }

private:
    Store&          store_;
    std::string     primary_host_;
    int             primary_port_;
    CommandCallback on_command_;

    std::atomic<ReplicaState> state_{ReplicaState::DISCONNECTED};
    std::atomic<bool>         running_{false};
    std::thread               repl_thread_;

    int         primary_fd_ = -1;   // TCP socket to primary
    std::string repl_id_;           // primary's replication ID
    int64_t     repl_offset_ = 0;   // our replication offset

    // Accumulated read buffer 
    std::string read_buf_;

    // Internal helpers 

    // Connect to primary
    int  connect_to_primary();

    // Send PSYNC and handle +FULLRESYNC or +CONTINUE response
    bool do_psync(int fd);

    // Receive and load full RDB snapshot from primary
    bool receive_rdb(int fd);

    // Main streaming loop, apply commands as they arrive
    void streaming_loop(int fd);

    // Read exactly n bytes from fd into buf
    bool read_exact(int fd, char* buf, size_t n);

    // Read a line (\r\n terminated) from fd
    bool read_line(int fd, std::string& out);

    // The background thread body
    void replication_thread();
};
