
// store.cpp — Hopscotch Hash Table Implementation

#include "store.h"
#include <stdexcept>
#include <iostream>
#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <functional>

using namespace std::chrono;

// CONSTRUCTOR / DESTRUCTOR
Store::Store()
    : num_buckets_(INITIAL_BUCKETS)
    , table_(INITIAL_BUCKETS)
{
    purge_thread_ = std::thread(&Store::purge_loop, this);
}

Store::~Store() {
    running_ = false;
    if (purge_thread_.joinable())
        purge_thread_.join();
}

// FNV-1a HASH FUNCTION
size_t Store::hash_key(const std::string& key) const {
    // FNV-1a 64-bit
    uint64_t hash = 14695981039346656037ULL;  // FNV offset basis
    for (unsigned char c : key) {
        hash ^= c;                            // XOR with byte
        hash *= 1099511628211ULL;             // FNV prime
    }
    return hash & (num_buckets_.load(std::memory_order_relaxed) - 1);
}

// STRIPE SELECTION
size_t Store::stripe_of(size_t bucket) const {
    return (bucket * NUM_STRIPES) / num_buckets_.load(std::memory_order_relaxed);
}

// EXPIRY CHECK
bool Store::is_expired(const Entry& e) const {
    if (!e.has_expiry) return false;
    return e.expiry < steady_clock::now();
}

// INSERT_LOCKED — Internal insert, all stripe locks held by caller
void Store::insert_locked(const std::string& key, const std::string& val,
                          bool has_expiry,
                          steady_clock::time_point expiry)
{
    const size_t nb = num_buckets_.load(std::memory_order_relaxed);

    size_t home = hash_key(key);

    // Check if key already exists in neighbourhood
    uint8_t bmap = table_[home].hop_bitmap;
    for (int i = 0; i < (int)NEIGHBOURHOOD; i++) {
        if (!(bmap & (1 << i))) continue;
        size_t idx = (home + i) & (nb - 1);
        if (table_[idx].occupied && table_[idx].key == key) {
            table_[idx].value      = val;
            table_[idx].has_expiry = has_expiry;
            table_[idx].expiry     = expiry;
            return;
        }
    }

    // Find nearest empty slot
    size_t empty_pos = SIZE_MAX;
    for (size_t i = 0; i < nb; i++) {
        size_t idx = (home + i) & (nb - 1);
        if (!table_[idx].occupied) {
            empty_pos = idx;
            break;
        }
    }

    if (empty_pos == SIZE_MAX) {
        std::cerr << "[store] insert_locked: no empty slot after resize — dropping key\n";
        return;
    }

    // Hopscotch displacement
    while (true) {
        size_t dist = (empty_pos - home + nb) & (nb - 1);
        if (dist < NEIGHBOURHOOD) break;

        bool displaced = false;
        for (int j = (int)NEIGHBOURHOOD - 1; j >= 1; j--) {
            size_t cand      = (empty_pos - j + nb) & (nb - 1);
            if (!table_[cand].occupied) continue;
            size_t cand_home = hash_key(table_[cand].key);
            size_t new_dist  = (empty_pos - cand_home + nb) & (nb - 1);
            if (new_dist < NEIGHBOURHOOD) {
                table_[empty_pos] = std::move(table_[cand]);
                table_[cand].occupied = false;
                size_t old_d = (cand      - cand_home + nb) & (nb - 1);
                size_t new_d = (empty_pos - cand_home + nb) & (nb - 1);
                table_[cand_home].hop_bitmap &= ~(1 << old_d);
                table_[cand_home].hop_bitmap |=  (1 << new_d);
                empty_pos = cand;
                displaced = true;
                break;
            }
        }
        if (!displaced) {
            std::cerr << "[store] insert_locked: displacement failed after resize — dropping key\n";
            return;
        }
    }

    size_t final_dist = (empty_pos - home + nb) & (nb - 1);
    if (final_dist < NEIGHBOURHOOD) {
        table_[empty_pos].key        = key;
        table_[empty_pos].value      = val;
        table_[empty_pos].occupied   = true;
        table_[empty_pos].has_expiry = has_expiry;
        table_[empty_pos].hop_bitmap = 0;
        table_[empty_pos].expiry     = expiry;
        table_[home].hop_bitmap     |= (1 << final_dist);
        num_entries_++;
    }
}

// DO_RESIZE_LOCKED — Double the table, rehash all entries

void Store::do_resize_locked() {
    size_t old_buckets = num_buckets_.load(std::memory_order_relaxed);
    size_t new_buckets = old_buckets * 2;  // always power-of-2

    std::cout << "[store] Resizing table: " << old_buckets
              << " → " << new_buckets << " buckets\n";

    // Collect all live entries before we overwrite the table
    struct Snapshot { std::string key, val; bool has_expiry; steady_clock::time_point expiry; };
    std::vector<Snapshot> live;
    live.reserve(num_entries_.load());

    for (size_t b = 0; b < old_buckets; b++) {
        const Entry& e = table_[b];
        if (!e.occupied || is_expired(e)) continue;
        live.push_back({e.key, e.value, e.has_expiry, e.expiry});
    }

    // Replace table with a fresh, empty one of double size.
    num_buckets_.store(new_buckets, std::memory_order_release);
    table_.assign(new_buckets, Entry{});
    num_entries_ = 0;

    // Re-insert every live entry
    for (auto& s : live)
        insert_locked(s.key, s.val, s.has_expiry, s.expiry);

    std::cout << "[store] Resize complete: " << num_entries_.load()
              << " keys rehashed\n";
}
// SET — Insert or update a key
void Store::set(const std::string& key, const std::string& val,
                std::optional<int> ttl_seconds)
{
    size_t entries     = num_entries_.load(std::memory_order_relaxed);
    size_t cur_buckets = num_buckets_.load(std::memory_order_acquire);
    if (entries * 100 / cur_buckets >= MAX_LOAD_FACTOR) {
        // Acquire all stripe write-locks before resizing
        std::vector<std::unique_lock<std::shared_mutex>> all_locks;
        all_locks.reserve(NUM_STRIPES);
        for (auto& s : stripes_) all_locks.emplace_back(s);
        // Re-check now that we hold all locks 
        if (num_entries_.load() * 100 / num_buckets_.load(std::memory_order_relaxed) >= MAX_LOAD_FACTOR)
            do_resize_locked();
        // all_locks destructs here, releasing every stripe
    }
    size_t home   = hash_key(key);
    size_t stripe = stripe_of(home);

    std::unique_lock lock(stripes_[stripe]);
    const size_t nb = num_buckets_.load(std::memory_order_relaxed);
    
    // Check if key already exists in neighbourhood 
    uint8_t bmap = table_[home].hop_bitmap;
    for (int i = 0; i < (int)NEIGHBOURHOOD; i++) {
        if (!(bmap & (1 << i))) continue;  // slot i not used by home
        size_t idx = (home + i) & (nb - 1);
        if (table_[idx].occupied && table_[idx].key == key) {
            // Found, update value and TTL
            table_[idx].value = val;
            if (ttl_seconds) {
                table_[idx].has_expiry = true;
                table_[idx].expiry = steady_clock::now()
                                   + seconds(*ttl_seconds);
            } else {
                table_[idx].has_expiry = false;
            }
            return; 
        }
    }

    //  Find nearest empty slot 
    size_t empty_pos = SIZE_MAX;
    for (size_t i = 0; i < nb; i++) {
        size_t idx = (home + i) & (nb - 1);
        if (!table_[idx].occupied || is_expired(table_[idx])) {
            // Treat expired entries as empty
            if (table_[idx].occupied) {
                // Clear the stale hop_bitmap bit from this expired entry's home
                size_t exp_home = hash_key(table_[idx].key);
                size_t exp_dist = (idx - exp_home + nb) & (nb - 1);
                if (exp_dist < NEIGHBOURHOOD)
                    table_[exp_home].hop_bitmap &= ~(1 << exp_dist);
                table_[idx].occupied = false;
                table_[idx].hop_bitmap = 0;
                num_entries_--;
            }
            empty_pos = idx;
            break;
        }
    }

    if (empty_pos == SIZE_MAX) {
        // Release the stripe lock, grab all locks, resize, then retry
        lock.unlock();
        {
            std::vector<std::unique_lock<std::shared_mutex>> all_locks;
            all_locks.reserve(NUM_STRIPES);
            for (auto& s : stripes_) all_locks.emplace_back(s);
            do_resize_locked();
        }
        // Tail-call: retry now that the table is larger.
        set(key, val, ttl_seconds);
        return;
    }

    // Hopscotch displacement
    while (true) {
        // Distance from home to empty slot
        size_t dist = (empty_pos - home + nb) & (nb- 1);
        if (dist < NEIGHBOURHOOD) break;  // empty is close enough

        // Try to find an entry near empty_pos that can move into it
        bool displaced = false;
        for (int j = (int)NEIGHBOURHOOD - 1; j >= 1; j--) {
            // Candidate: j slots before empty_pos
            size_t cand = (empty_pos - j + nb) & (nb - 1);
            if (!table_[cand].occupied) continue;

            size_t cand_home = hash_key(table_[cand].key);

            size_t new_dist = (empty_pos - cand_home + nb)
                             & (nb - 1);
            if (new_dist < NEIGHBOURHOOD) {
                // Move candidate from 'cand' to 'empty_pos'
                table_[empty_pos] = std::move(table_[cand]);
                table_[cand].occupied = false;

                // Update hop_bitmap of cand_home
                size_t old_d = (cand      - cand_home + nb) & (nb - 1);
                size_t new_d = (empty_pos - cand_home + nb) & (nb - 1);
                table_[cand_home].hop_bitmap &= ~(1 << old_d);
                table_[cand_home].hop_bitmap |=  (1 << new_d);

                empty_pos = cand;  // empty slot is now at cand
                displaced = true;
                break;
            }
        }

        if (!displaced) {
           // Release stripe lock, resize the entire table, then retry.
            lock.unlock();
            {
                std::vector<std::unique_lock<std::shared_mutex>> all_locks;
                all_locks.reserve(NUM_STRIPES);
                for (auto& s : stripes_) all_locks.emplace_back(s);
                do_resize_locked();
            }
            set(key, val, ttl_seconds);
            return;
        }
    }

    //  Place new entry 
    size_t final_dist = (empty_pos - home + nb) & (nb - 1);
    if (final_dist < NEIGHBOURHOOD) {
        table_[empty_pos].key        = key;
        table_[empty_pos].value      = val;
        table_[empty_pos].occupied   = true;
        table_[empty_pos].has_expiry = ttl_seconds.has_value();
        table_[empty_pos].hop_bitmap = 0;

        if (ttl_seconds) {
            table_[empty_pos].expiry = steady_clock::now()
                                      + seconds(*ttl_seconds);
        }

        // Mark this slot in home's hop_bitmap
        table_[home].hop_bitmap |= (1 << final_dist);
        num_entries_++;
    }
}

// GET — Look up a key
std::optional<std::string> Store::get(const std::string& key) {
    size_t home   = hash_key(key);
    size_t stripe = stripe_of(home);
    std::shared_lock lock(stripes_[stripe]);

    uint8_t bmap = table_[home].hop_bitmap;
    for (int i = 0; i < (int)NEIGHBOURHOOD; i++) {
        if (!(bmap & (1 << i))) continue;  // no key at home+i
        size_t idx = (home + i) & (num_buckets_ - 1);

        if (!table_[idx].occupied) continue;
        if (table_[idx].key != key) continue;

        // Key found, check expiry (lazy expiry)
        if (is_expired(table_[idx])) return std::nullopt;

        return table_[idx].value;
    }

    return std::nullopt;  // key not found
}

// DEL — Remove a key
bool Store::del(const std::string& key) {
    size_t home   = hash_key(key);
    size_t stripe = stripe_of(home);

    std::unique_lock lock(stripes_[stripe]);

    uint8_t bmap = table_[home].hop_bitmap;
    for (int i = 0; i < (int)NEIGHBOURHOOD; i++) {
        if (!(bmap & (1 << i))) continue;
        size_t idx = (home + i) & (num_buckets_ - 1);

        if (!table_[idx].occupied) continue;
        if (table_[idx].key != key) continue;

        // Clear the entry
        table_[idx].occupied   = false;
        table_[idx].key.clear();
        table_[idx].value.clear();
        table_[idx].has_expiry = false;

        // Clear the bit in home's hop_bitmap
        table_[home].hop_bitmap &= ~(1 << i);

        num_entries_--;
        return true;
    }
    return false;  // key not found
}

// EXPIRE — Set a TTL on an existing key
bool Store::expire(const std::string& key, int seconds_val) {
    size_t home   = hash_key(key);
    size_t stripe = stripe_of(home);

    std::unique_lock lock(stripes_[stripe]);

    uint8_t bmap = table_[home].hop_bitmap;
    for (int i = 0; i < (int)NEIGHBOURHOOD; i++) {
        if (!(bmap & (1 << i))) continue;
        size_t idx = (home + i) & (num_buckets_ - 1);

        if (!table_[idx].occupied || table_[idx].key != key) continue;
        if (is_expired(table_[idx])) return false;

        table_[idx].has_expiry = true;
        table_[idx].expiry = steady_clock::now() + seconds(seconds_val);
        return true;
    }
    return false;
}

// TTL — Remaining seconds until expiry
// Returns: -1 = no TTL set, -2 = key doesn't exist
int64_t Store::ttl(const std::string& key) {
    size_t home   = hash_key(key);
    size_t stripe = stripe_of(home);

    std::shared_lock lock(stripes_[stripe]);

    uint8_t bmap = table_[home].hop_bitmap;
    for (int i = 0; i < (int)NEIGHBOURHOOD; i++) {
        if (!(bmap & (1 << i))) continue;
        size_t idx = (home + i) & (num_buckets_ - 1);

        if (!table_[idx].occupied || table_[idx].key != key) continue;
        if (is_expired(table_[idx])) return -2;
        if (!table_[idx].has_expiry) return -1;

        auto remaining = table_[idx].expiry - steady_clock::now();
        int64_t secs = duration_cast<seconds>(remaining).count();
        return secs > 0 ? secs : -2;
    }
    return -2;  // key doesn't exist
}

// EXISTS
bool Store::exists(const std::string& key) {
    return get(key).has_value();
}

// DBSIZE — Number of live entries
size_t Store::dbsize() {
    return num_entries_.load();
}

// FLUSHALL — Clear everything
void Store::flushall() {
    std::vector<std::unique_lock<std::shared_mutex>> locks;
    for (auto& s : stripes_)
        locks.emplace_back(s);

    for (auto& e : table_) {
        e.occupied   = false;
        e.hop_bitmap = 0;
        e.key.clear();
        e.value.clear();
        e.has_expiry = false;
    }
    num_entries_ = 0;
}

// PURGE LOOP — Background expired key cleanup
void Store::purge_loop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (!running_) break;
        
        const size_t nb = num_buckets_.load(std::memory_order_acquire);
        size_t purged = 0;
        for (size_t bucket = 0; bucket < nb; bucket++) {
            size_t bucket_stripe = stripe_of(bucket);

            // Only lock one stripe at a time to avoid holding all locks
            std::unique_lock lock(stripes_[bucket_stripe]);

            if (!table_[bucket].occupied) continue;
            if (!table_[bucket].has_expiry) continue;
            if (!is_expired(table_[bucket])) continue;

            // Expire this entry
            size_t home = hash_key(table_[bucket].key);
            size_t home_stripe = stripe_of(home);
            size_t dist = (bucket - home + nb) & (nb - 1);
           if (dist < NEIGHBOURHOOD) {
                if (home_stripe == bucket_stripe) {
                    table_[home].hop_bitmap &= ~(1 << dist);
                } else {
                    lock.unlock();

                    size_t lo = std::min(home_stripe, bucket_stripe);
                    size_t hi = std::max(home_stripe, bucket_stripe);
                    std::unique_lock lock_lo(stripes_[lo]);
                    std::unique_lock lock_hi(stripes_[hi]);

                    // Re-validate
                    if (!table_[bucket].occupied ||
                        !table_[bucket].has_expiry ||
                        !is_expired(table_[bucket])) {
                        continue;  // entry changed, skip
                    }
                    
                    table_[home].hop_bitmap &= ~(1 << dist);
                    table_[bucket].occupied   = false;
                    table_[bucket].has_expiry = false;
                    table_[bucket].key.clear();
                    num_entries_--;
                    purged++;
                    continue;
                }
            }

            table_[bucket].occupied   = false;
            table_[bucket].has_expiry = false;
            table_[bucket].key.clear();
            num_entries_--;
            purged++;
        }

        if (purged > 0)
            std::cerr << "[purge] Expired " << purged << " keys\n";
    }
}

// SNAPSHOT — Iterate all live entries (for RDB serialisation)
void Store::snapshot(SnapshotFn fn) {
    using namespace std::chrono;

    const size_t nb = num_buckets_.load(std::memory_order_acquire);
    for (size_t bucket = 0; bucket < nb; bucket++) {
        size_t stripe = stripe_of(bucket);
        std::shared_lock lock(stripes_[stripe]);

        const Entry& e = table_[bucket];
        if (!e.occupied) continue;
        if (is_expired(e)) continue;

        int64_t ttl_ms = 0;
        if (e.has_expiry) {
            auto ms = duration_cast<milliseconds>(
                e.expiry.time_since_epoch()).count();
            ttl_ms = ms;
        }
        fn(e.key, e.value, ttl_ms);
    }
}
