#pragma once

#include <string>
#include <unordered_map>
#include <optional>
#include <shared_mutex>
#include <chrono>

class Store {
public:
    Store() = default;
    ~Store() = default;

    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    // Core commands
    void set(const std::string& key,
             const std::string& value,
             std::optional<int> ttl_seconds = std::nullopt);

    std::optional<std::string> get(const std::string& key);

    bool del(const std::string& key);

    bool exists(const std::string& key);

    // TTL commands
    bool expire(const std::string& key, int seconds);

    // Returns:
    // -2 => key not found
    // -1 => key exists but has no expiry
    int64_t ttl(const std::string& key);

    size_t dbsize();

    void flushall();

private:
    struct Entry {
        std::string value;

        bool has_expiry = false;

        std::chrono::steady_clock::time_point expiry;
    };

    bool is_expired(const Entry& entry) const;

    mutable std::shared_mutex mutex_;

    std::unordered_map<std::string, Entry> data_;
};
