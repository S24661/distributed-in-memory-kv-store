
// eviction.cpp — CLOCK-Pro Implementation

#include "eviction.h"
#include <stdexcept>
#include <iostream>

// Constructor
ClockPro::ClockPro(size_t capacity)
    : capacity_(capacity)
    , max_hot_(capacity / 2)
    , clock_(capacity * 2)  // 2x capacity: extra space for ghost entries
{}

// ON ACCESS — Called on every GET
void ClockPro::on_access(const std::string& key) {
    std::lock_guard lock(mu_);

    auto it = pos_map_.find(key);
    if (it == pos_map_.end()) return;  // not tracked

    size_t idx = it->second;
    if (clock_[idx].status == PageStatus::COLD ||
        clock_[idx].status == PageStatus::HOT)
    {
        clock_[idx].ref_bit = true;  // mark recently accessed
    }
}


// ON INSERT — Called on every SET
std::string ClockPro::on_insert(const std::string& key) {
    std::lock_guard lock(mu_);
    std::string evicted = "";

    // Check if key is a ghost (recently evicted, now re-accessed)
    auto it = pos_map_.find(key);
    if (it != pos_map_.end()) {
        size_t idx = it->second;
        if (clock_[idx].status == PageStatus::GHOST) {
            // Re-access of ghost: recency pattern detected
            if (max_hot_ < capacity_ - 1) max_hot_++;
            // Promote ghost → COLD (re-admit to cache)
            clock_[idx].status  = PageStatus::COLD;
            clock_[idx].ref_bit = false;
            num_cold_++;
            return "";  // no eviction needed
        }
        if (clock_[idx].status == PageStatus::COLD ||
            clock_[idx].status == PageStatus::HOT) {
            return "";  // already in cache
        }
    }

    // If at capacity, evict something
    if (num_hot_ + num_cold_ >= capacity_) {
        evicted = run_clock();
    }

    // Find empty slot and insert as COLD
    size_t slot = find_empty_slot();
    clock_[slot] = {key, PageStatus::COLD, false};
    pos_map_[key] = slot;
    num_cold_++;

    return evicted;
}


// RUN_CLOCK — The core CLOCK-Pro sweep

std::string ClockPro::run_clock() {
    size_t iterations = 0;
    size_t max_iter   = clock_.size() * 2;  // safety limit

    while (iterations++ < max_iter) {
        ClockEntry& entry = clock_[hand_];
        hand_ = (hand_ + 1) % clock_.size();

        if (entry.status == PageStatus::EMPTY ||
            entry.status == PageStatus::GHOST) {
            continue;  // skip empty and ghost slots
        }

        if (entry.status == PageStatus::HOT) {
            if (entry.ref_bit) {
                // Recently accessed hot page
                entry.ref_bit = false;
            } else {
                // Hot page not recently used — demote to cold
                entry.status  = PageStatus::COLD;
                num_hot_--;
                num_cold_++;
            }
            continue;
        }

        if (entry.status == PageStatus::COLD) {
            if (entry.ref_bit) {
                // Cold page was accessed since last sweep promoted to hot
                entry.ref_bit = false;

                // But if we have too many HOT pages, don't promote
                if (num_hot_ < max_hot_) {
                    entry.status = PageStatus::HOT;
                    num_cold_--;
                    num_hot_++;
                } else {
                    // demote some hot page first
                    entry.ref_bit = false;
                }
            } else {
                // Cold page, not recently accessed → EVICT
                std::string evicted_key = entry.key;

                // Convert to GHOST: keep metadata but mark as evicted.
                entry.status  = PageStatus::GHOST;
                entry.ref_bit = false;
                num_cold_--;

                // Remove from pos_map so future SETs don't find it as cached
                pos_map_.erase(evicted_key);

                return evicted_key;  // return key to evict from store
            }
        }
    }

    return "";  // no eviction found 
}

// ON DELETE — Remove key from eviction tracking
void ClockPro::on_delete(const std::string& key) {
    std::lock_guard lock(mu_);

    auto it = pos_map_.find(key);
    if (it == pos_map_.end()) return;

    size_t idx = it->second;
    PageStatus s = clock_[idx].status;

    if (s == PageStatus::HOT)  num_hot_--;
    if (s == PageStatus::COLD) num_cold_--;

    clock_[idx].status  = PageStatus::EMPTY;
    clock_[idx].key.clear();
    pos_map_.erase(it);
}

// FIND EMPTY SLOT
size_t ClockPro::find_empty_slot() {
    // Scan for an EMPTY slot 
    for (size_t i = 0; i < clock_.size(); i++) {
        size_t idx = (hand_ + i) % clock_.size();
        if (clock_[idx].status == PageStatus::EMPTY)
            return idx;
    }
    // If no empty slot, reuse a ghost slot
    for (size_t i = 0; i < clock_.size(); i++) {
        size_t idx = (hand_ + i) % clock_.size();
        if (clock_[idx].status == PageStatus::GHOST) {
            pos_map_.erase(clock_[idx].key);
            return idx;
        }
    }
    // Fallback: evict at hand position
    return hand_;
}


// STATUS QUERIES
bool ClockPro::is_cached(const std::string& key) const {
    std::lock_guard lock(mu_);
    auto it = pos_map_.find(key);
    if (it == pos_map_.end()) return false;
    PageStatus s = clock_[it->second].status;
    return s == PageStatus::HOT || s == PageStatus::COLD;
}

size_t ClockPro::num_hot()  const { std::lock_guard lock(mu_); return num_hot_;  }
size_t ClockPro::num_cold() const { std::lock_guard lock(mu_); return num_cold_; }
