
// replication.cpp — Primary-side Replication Manager

#include "replication.h"
#include "resp.h"
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <random>
#include <cstring>
#include <algorithm>
#include <chrono>

// RING BUFFER
RingBuffer::RingBuffer(size_t capacity)
    : buf_(capacity, 0), capacity_(capacity) {}

// Append data to the ring buffer.
void RingBuffer::write(const std::string& data) {
    std::lock_guard lock(mu_);
    for (char c : data) {
        buf_[(write_offset_ + head_) % capacity_] = c;
        // If we've wrapped around, advance head 
        if ((size_t)write_offset_ >= capacity_)
            head_ = (head_ + 1) % capacity_;
        write_offset_++;
    }
}

// Read len bytes starting at absolute offset 
bool RingBuffer::read(int64_t offset, size_t len, std::string& out) const {
    std::lock_guard lock(mu_);
    int64_t cur = write_offset_.load();

    // Check if offset is still in the buffer
    if (offset < cur - (int64_t)capacity_) return false;  
    if (offset + (int64_t)len > cur) return false;        

    out.resize(len);
    for (size_t i = 0; i < len; i++) {
        int64_t abs_pos = offset + (int64_t)i;
        size_t  buf_pos = (size_t)abs_pos % capacity_;
        out[i] = buf_[buf_pos];
    }
    return true;
}

bool RingBuffer::contains_offset(int64_t offset) const {
    int64_t cur = write_offset_.load();
    return offset >= cur - (int64_t)capacity_ && offset <= cur;
}

// REPLICATION MANAGER

ReplicationManager::ReplicationManager(Store& store, size_t backlog_bytes)
    : store_(store), backlog_(backlog_bytes)
{
    repl_id_ = generate_repl_id();
    std::cout << "[replication] Primary started, repl_id=" << repl_id_ << "\n";
}

ReplicationManager::~ReplicationManager() {
    std::lock_guard lock(replicas_mutex_);
    replicas_.clear();  // destructors join threads
}

// GENERATE REPLICATION ID
std::string ReplicationManager::generate_repl_id() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);
    const char hex[] = "0123456789abcdef";
    std::string id(40, '0');
    for (char& c : id) c = hex[dis(gen)];
    return id;
}

// SERIALIZE COMMAND 
std::string ReplicationManager::serialize_cmd(
    const std::vector<std::string>& cmd)
{
    std::string out = "*" + std::to_string(cmd.size()) + "\r\n";
    for (const auto& arg : cmd)
        out += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
    return out;
}

// HANDLE PSYNC — New replica connecting
bool ReplicationManager::handle_psync(
    int replica_fd,
    const std::string& host, int port,
    const std::string& replid, int64_t offset)
{
    auto replica = std::make_unique<ReplicaConn>(replica_fd, host, port);
    ReplicaConn* replica_ptr = replica.get();

    bool partial_ok = (replid == repl_id_)
                   && (offset >= 0)
                   && backlog_.contains_offset(offset);

    if (partial_ok) {
        //  PARTIAL RESYNC
        std::cout << "[replication] Partial resync for "
                  << host << ":" << port
                  << " from offset=" << offset << "\n";

        std::string resp = "+CONTINUE " + repl_id_ + "\r\n";
        send(replica_fd, resp.c_str(), resp.size(), MSG_NOSIGNAL);

        // Send backlog from offset to current
        int64_t cur = backlog_.current_offset();
        if (cur > offset) {
            std::string catchup;
            backlog_.read(offset, (size_t)(cur - offset), catchup);
            send(replica_fd, catchup.c_str(), catchup.size(), MSG_NOSIGNAL);
        }
        replica->offset = cur;

    } else {
        // FULL RESYNC
        std::cout << "[replication] Full resync for "
                  << host << ":" << port << "\n";

        int64_t cur_offset = backlog_.current_offset();
        std::string resp = "+FULLRESYNC " + repl_id_ + " "
                         + std::to_string(cur_offset) + "\r\n";
        send(replica_fd, resp.c_str(), resp.size(), MSG_NOSIGNAL);

        // Serialise current store to RDB
        auto rdb_data = RDB::serialize(store_);

        // Send RDB length header then data
        std::string rdb_header = "$" + std::to_string(rdb_data.size()) + "\r\n";
        send(replica_fd, rdb_header.c_str(), rdb_header.size(), MSG_NOSIGNAL);
        send(replica_fd, rdb_data.data(), rdb_data.size(), MSG_NOSIGNAL);

        replica->offset = cur_offset;
    }

    // Start dedicated sender thread for this replica.
    replica->send_thread = std::thread(
        &ReplicationManager::sender_loop, this, replica_ptr);

    {
        std::lock_guard lock(replicas_mutex_);
        replicas_.push_back(std::move(replica));
    }

    std::cout << "[replication] Replica " << host << ":" << port
              << " fully synced\n";
    return partial_ok;
}

// PROPAGATE — Send command to all replicas
void ReplicationManager::propagate(const std::vector<std::string>& cmd) {
    std::string serialised = serialize_cmd(cmd);

    // Write to backlog for future partial resyncs
    backlog_.write(serialised);

    std::lock_guard lock(replicas_mutex_);
    for (auto& replica : replicas_) {
        if (!replica->alive) continue;
        std::lock_guard qlock(replica->queue_mutex);
        replica->send_queue.push_back(serialised);
    }
}

// SENDER LOOP — One per replica, runs in background thread
void ReplicationManager::sender_loop(ReplicaConn* replica) {
    while (replica->alive) {
        std::deque<std::string> to_send;
        {
            std::lock_guard lock(replica->queue_mutex);
            std::swap(to_send, replica->send_queue);
        }

        for (const auto& cmd : to_send) {
            ssize_t sent = send(replica->fd,
                                cmd.c_str(), cmd.size(),
                                MSG_NOSIGNAL);  // don't SIGPIPE on broken pipe
            if (sent < 0) {
                std::cerr << "[replication] Replica "
                          << replica->host << ":" << replica->port
                          << " disconnected\n";
                replica->alive = false;
                close(replica->fd);
                return;
            }
            replica->offset += sent;
        }

        if (to_send.empty())
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

// REMOVE REPLICA
void ReplicationManager::remove_replica(int fd) {
    std::lock_guard lock(replicas_mutex_);
    replicas_.erase(
        std::remove_if(replicas_.begin(), replicas_.end(),
            [fd](const auto& r) { return r->fd == fd; }),
        replicas_.end());
}

// GET INFO — For INFO replication command
ReplicationManager::ReplicationInfo ReplicationManager::get_info() const {
    ReplicationInfo info;
    info.repl_id            = repl_id_;
    info.master_repl_offset = backlog_.current_offset();

    std::lock_guard lock(replicas_mutex_);
    info.connected_replicas = 0;

    for (const auto& r : replicas_) {
        if (!r->alive) continue;
        info.connected_replicas++;
        ReplicationInfo::ReplicaInfo ri;
        ri.host      = r->host;
        ri.port      = r->port;
        ri.offset    = r->offset.load();
        ri.lag_bytes = info.master_repl_offset - ri.offset;
        info.replicas.push_back(ri);
    }
    return info;
}
