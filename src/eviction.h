#pragma once
// eviction.h — CLOCK-Pro Eviction Algorithm

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <cstddef>

// Status of each page in the clock
enum class PageStatus {
    EMPTY,   // Slot unused
    COLD,    // In cache, accessed once recently
    HOT,     // In cache, accessed multiple times
    GHOST    // Evicted but metadata kept (detects re-access patterns)
};

struct ClockEntry {
    std::string  key;
    PageStatus   status   = PageStatus::EMPTY;
    bool         ref_bit  = false;   // Set on access, cleared by clock hand
};

class ClockPro {
public:
    explicit ClockPro(size_t capacity);

    // Notify eviction policy that key was accessed (GET)
    void on_access(const std::string& key);

    // Notify that a new key was inserted (SET)
    std::string on_insert(const std::string& key);

    // True if key is tracked as HOT or COLD (i.e. in cache)
    bool is_cached(const std::string& key) const;

    // Remove a key from tracking (on DEL)
    void on_delete(const std::string& key);

    // Stats for debugging / benchmark
    size_t num_hot()  const;
    size_t num_cold() const;
    size_t capacity() const { return capacity_; }

private:
    size_t capacity_;
    size_t num_hot_  = 0;
    size_t num_cold_ = 0;
    size_t max_hot_;             // Adaptive threshold: increases on ghost hits

    std::vector<ClockEntry>                              clock_;
    std::unordered_map<std::string, size_t>              pos_map_; 
    size_t                                               hand_ = 0; // clock hand position
    mutable std::mutex                                   mu_;

    // Advance the clock hand and perform eviction.
    // Returns the key that was evicted.
    std::string run_clock();

    // Find next empty slot in clock array
    size_t find_empty_slot();
};
