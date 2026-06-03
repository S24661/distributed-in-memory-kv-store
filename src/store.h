// store.h — Core Key-Value Store
#pragma once

#include <string>
#include <vector>
#include <optional>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <chrono>
#include <array>
#include <thread>
#include <functional>

static constexpr size_t INITIAL_BUCKETS  = 1 << 16;  # 65536 buckets
static constexpr size_t NEIGHBOURHOOD    = 8;        # hopscotch window
static constexpr size_t NUM_STRIPES      = 16;       # lock stripes
static constexpr double MAX_LOAD_FACTOR  = 0.70;     # resize at 70% full

# Entry: one slot in the hash table
struct Entry {
    std::string  key;
    std::string  value;

    # For EXPIRE/TTL support
    std::chrono::steady_clock::time_point expiry;
    bool         has_expiry = false;
    bool         occupied   = false;
    uint32_t     hop_bitmap = 0;
};

// Store
class Store {
public:
    Store();
    ~Store();

    # Non-copyable: Store owns a std::thread and raw table state.
    # Copying would silently share or duplicate those resources.
    Store(const Store&)            = delete;
    Store& operator=(const Store&) = delete;

    # Core commands
    void set(const std::string& key, const std::string& val,
             std::optional<int> ttl_seconds = std::nullopt);
    std::optional<std::string> get(const std::string& key);
    bool del(const std::string& key);

    # TTL commands
    bool            expire(const std::string& key, int seconds);
    int64_t         ttl(const std::string& key);    # -1=no ttl, -2=not found
    bool            exists(const std::string& key);

    # Info
    size_t          dbsize();
    void            flushall();

    # Snapshot: visit every live (non-expired) key-value pair.
    using SnapshotFn = std::function<void(
        const std::string& key,
        const std::string& val,
        int64_t ttl_ms)>;
    void snapshot(SnapshotFn fn);

private:
    size_t  num_buckets_;
    std::vector<Entry>table_;
    std::atomic<size_t>num_entries_{0};

    # 16 independent reader-writer locks
    std::array<std::shared_mutex, NUM_STRIPES> stripes_;

    # Background expiry purge
    std::thread   purge_thread_;
    std::atomic<bool> running_{true};
    void          purge_loop();

    # Helpers
    size_t        hash_key(const std::string& key) const;
    size_t        stripe_of(size_t bucket) const;
    bool          is_expired(const Entry& e) const;
    void          do_resize();
};
