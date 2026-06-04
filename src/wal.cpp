
// wal.cpp — Write-Ahead Log with Group Commit

#include "wal.h"
#include <unistd.h>  
#include <cstdio>    
#include <iostream>
#include <sstream>
#include <chrono>

// Constructor 
WAL::WAL(const std::string& path, int commit_interval_ms)
    : path_(path)
    , commit_interval_ms_(commit_interval_ms)
{
    file_.open(path, std::ios::app | std::ios::binary);
    if (!file_.is_open())
        throw std::runtime_error("WAL: cannot open file: " + path);

    raw_file_ = fopen(path.c_str(), "ab");
    if (!raw_file_)
        throw std::runtime_error("WAL: cannot open raw file: " + path);

    write_buf_.reserve(256);
    flush_buf_.reserve(256);

    flush_thread_ = std::thread(&WAL::flush_loop, this);
}

WAL::~WAL() {
    stop();
}

void WAL::log(const std::vector<std::string>& cmd) {
    std::lock_guard lock(buf_mutex_);
    write_buf_.push_back(serialize(cmd));
}

void WAL::flush_loop() {
    while (running_) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(commit_interval_ms_));

        if (!running_) break;

        {
            std::lock_guard lock(buf_mutex_);
            if (write_buf_.empty()) continue;
            std::swap(write_buf_, flush_buf_);
        }

        for (const auto& entry : flush_buf_)
            file_ << entry;

        // Flush C++ stream buffer, then fsync via raw FILE*
        file_.flush();
        if (raw_file_) {
            fflush(raw_file_);
            ::fsync(fileno(raw_file_));
        }

        flush_buf_.clear();
    }

    flush_now();
}

void WAL::flush_now() {
    std::vector<std::string> remaining;
    {
        std::lock_guard lock(buf_mutex_);
        std::swap(write_buf_, remaining);
    }
    for (const auto& entry : remaining)
        file_ << entry;
    file_.flush();
    if (raw_file_) {
        fflush(raw_file_);
        ::fsync(fileno(raw_file_));
    }
}

void WAL::stop() {
    running_ = false;
    if (flush_thread_.joinable())
        flush_thread_.join();
    if (raw_file_) { fclose(raw_file_); raw_file_ = nullptr; }
    file_.close();
}

// REPLAY — Read WAL on startup, return all commands
std::vector<WAL::ReplayEntry> WAL::replay() {
    std::vector<ReplayEntry> commands;
    std::ifstream in(path_, std::ios::binary);
    if (!in.is_open()) {
        std::cout << "[WAL] No existing WAL — starting fresh\n";
        return commands;  // fresh start
    }
    int64_t elapsed_secs = 0;
    {
        struct stat st{};
        if (::stat(path_.c_str(), &st) == 0) {
            auto file_mtime = std::chrono::system_clock::from_time_t(st.st_mtime);
            auto now        = std::chrono::system_clock::now();
            elapsed_secs    = std::chrono::duration_cast<std::chrono::seconds>(
                                  now - file_mtime).count();
            if (elapsed_secs < 0) elapsed_secs = 0;  // clock skew guard
        }
        // If stat fails, elapsed_secs stays 0 
    }

    std::string line;

    while (std::getline(in, line)) {
        if (line.empty()) continue;
        if (line.back() == '\r') line.pop_back();  // handle \r\n
        if (line[0] != '*') continue;

        // Read the number of arguments
        int count;
        try { count = std::stoi(line.substr(1)); }
        catch (...) { continue; }

        std::vector<std::string> cmd;
        cmd.reserve(count);
        bool ok = true;

        for (int i = 0; i < count; i++) {
            // Read $N line
            if (!std::getline(in, line)) { ok = false; break; }
            if (line.back() == '\r') line.pop_back();

            // Read value line
            if (!std::getline(in, line)) { ok = false; break; }
            if (line.back() == '\r') line.pop_back();
            cmd.push_back(line);
        }

       if (!ok || (int)cmd.size() != count) continue;

        // Compute adjusted TTL for SET, EX N commands
        int64_t ttl_remaining = -1;  // -1 = no TTL
        if (!cmd.empty()) {
            std::string name = cmd[0];
            for (char& c : name) c = toupper(c);

            if (name == "SET" && cmd.size() >= 3) {
                for (size_t i = 3; i + 1 < cmd.size(); i++) {
                    std::string flag = cmd[i];
                    for (char& c : flag) c = toupper(c);
                    int64_t orig_ttl = 0;
                    if (flag == "EX") {
                        try { orig_ttl = std::stoll(cmd[i + 1]); } catch (...) {}
                        ttl_remaining = orig_ttl - elapsed_secs;
                        break;
                    } else if (flag == "PX") {
                        try { orig_ttl = std::stoll(cmd[i + 1]) / 1000; } catch (...) {}
                        ttl_remaining = orig_ttl - elapsed_secs;
                        break;
                    }
                }
            } else if (name == "EXPIRE" && cmd.size() == 3) {
                int64_t orig_ttl = 0;
                try { orig_ttl = std::stoll(cmd[2]); } catch (...) {}
                ttl_remaining = orig_ttl - elapsed_secs;
            }
        }

        commands.push_back({std::move(cmd), ttl_remaining});
    }

    size_t skipped = 0;
    for (const auto& e : commands)
        if (e.ttl_remaining_secs == 0) skipped++;

    std::cout << "[WAL] Replayed " << commands.size()
              << " commands from " << path_
              << " (" << skipped << " already expired)\n";
    return commands;
}

// SERIALIZE — Command to RESP format
std::string WAL::serialize(const std::vector<std::string>& cmd) {
    std::string out;
    out.reserve(64);  // pre-allocate to avoid reallocations
    out += '*';
    out += std::to_string(cmd.size());
    out += "\r\n";
    for (const auto& arg : cmd) {
        out += '$';
        out += std::to_string(arg.size());
        out += "\r\n";
        out += arg;
        out += "\r\n";
    }
    return out;
}
