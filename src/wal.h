#pragma once
// wal.h — Write-Ahead Log with Group Commit

#include <string>
#include <vector>
#include <mutex>
#include <thread>
#include <atomic>
#include <fstream>
#include <cstdio>

class WAL {
public:
    // path: file to write WAL to 
    // commit_interval_ms: how often to fsync (default 2ms)
    explicit WAL(const std::string& path, int commit_interval_ms = 2);
    ~WAL();

    // Log a command to the WAL buffer
    void log(const std::vector<std::string>& cmd);

    // Replay the WAL file on startup.
    std::vector<std::vector<std::string>> replay();

    // Force immediate fsync 
    void flush_now();

    void stop();

private:
    std::string  path_;
    std::ofstream file_;
    int          commit_interval_ms_;

    // Double-buffer:
    // write_buf_  = client threads write here
    // flush_buf_  = background thread reads from here
    std::vector<std::string> write_buf_;
    std::vector<std::string> flush_buf_;
    std::mutex               buf_mutex_;
    FILE*                    raw_file_ = nullptr;  // for fsync via fileno()

    std::thread              flush_thread_;
    std::atomic<bool>        running_{true};

    // Background thread: every commit_interval_ms, swap buffers and fsync
    void flush_loop();

    // Serialize a command to RESP format for storage
    static std::string serialize(const std::vector<std::string>& cmd);
};
