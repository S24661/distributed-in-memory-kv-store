
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
std::vector<std::vector<std::string>> WAL::replay() {
    std::vector<std::vector<std::string>> commands;
    std::ifstream in(path_, std::ios::binary);
    if (!in.is_open()) {
        std::cout << "[WAL] No existing WAL — starting fresh\n";
        return commands;  // fresh start
    }

    std::string line;
    size_t line_num = 0;

    while (std::getline(in, line)) {
        line_num++;
        // Skip empty lines and non-array lines
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

        if (ok && (int)cmd.size() == count)
            commands.push_back(std::move(cmd));
    }

    std::cout << "[WAL] Replayed " << commands.size()
              << " commands from " << path_ << "\n";
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
