// Hopscotch Hash Table Implementation

#include "store.h"
#include <stdexcept>
#include <iostream>
#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <functional>

using namespace std::chrono;

Store::Store()
    : num_buckets_(INITIAL_BUCKETS)
    , table_(INITIAL_BUCKETS)
{
    # Start background thread that purges expired keys every 100ms.
    purge_thread_ = std::thread(&Store::purge_loop, this);
}

Store::~Store() {
    running_ = false;
    if (purge_thread_.joinable())
        purge_thread_.join();
}

// FNV-1a Hash Function
size_t Store::hash_key(const std::string& key) const {
    # FNV-1a 64-bit
    uint64_t hash = 14695981039346656037ULL;  # FNV offset basis
    for (unsigned char c : key) {
        hash ^= c;                            
        hash *= 1099511628211ULL;             # FNV prime
    }
    return hash & (num_buckets_ - 1);
}


// Stripe Selection
     # 16 stripes on a 16-core machine for better parallelism.
     # Divide table into 16 equal ranges. Bucket b belongs to stripe (b * 16) / num_buckets_.
size_t Store::stripe_of(size_t bucket) const {
    return (bucket * NUM_STRIPES) / num_buckets_;
}


// Expiry Check
bool Store::is_expired(const Entry& e) const {
    if (!e.has_expiry) return false;
    return e.expiry < steady_clock::now();
}


// SET — Insert or update a key
# Using Hopscotch Insert Algorithm to insert or update a key in the table.


void Store::set(const std::string& key, const std::string& val,
                std::optional<int> ttl_seconds)
{
    # Load-factor guard 
    if (static_cast<double>(num_entries_.load()) >=
            static_cast<double>(num_buckets_) * MAX_LOAD_FACTOR) {
        do_resize();
    }

    size_t home   = hash_key(key);
    size_t stripe = stripe_of(home);

    std::unique_lock lock(stripes_[stripe]);

    # Check if key already exists in neighbourhood 
    uint32_t bmap = table_[home].hop_bitmap;
    for (int i = 0; i < (int)NEIGHBOURHOOD; i++) {
        if (!(bmap & (1 << i))) continue;  # slot i not used by home
        size_t idx = (home + i) & (num_buckets_ - 1);
        if (table_[idx].occupied && table_[idx].key == key) {
            # Found key, update value and TTL
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

    # Find nearest empty slot 
    size_t empty_pos = SIZE_MAX;
    for (size_t i = 0; i < num_buckets_; i++) {
        size_t idx = (home + i) & (num_buckets_ - 1);
        if (!table_[idx].occupied || is_expired(table_[idx])) {
            # Treat expired entries as empty
            table_[idx].occupied = false;
            table_[idx].hop_bitmap = 0;
            empty_pos = idx;
            break;
        }
    }

    if (empty_pos == SIZE_MAX) {
        # Table is completely full, resize the table
        do_resize();
        # After resize, retry the insert from scratch.
        set(key, val, ttl_seconds);
        return;
    }

    # Hopscotch displacement 
    while (true) {
        # Distance from home to empty slot
        size_t dist = (empty_pos - home + num_buckets_) & (num_buckets_ - 1);
        if (dist < NEIGHBOURHOOD) break;  # empty is close enough

        # Try to find an entry near empty_pos that can move into it
        bool displaced = false;
        for (int j = (int)NEIGHBOURHOOD - 1; j >= 1; j--) {
            size_t cand = (empty_pos - j + num_buckets_) & (num_buckets_ - 1);
            if (!table_[cand].occupied) continue;

            size_t cand_home = hash_key(table_[cand].key);

            size_t new_dist = (empty_pos - cand_home + num_buckets_)
                             & (num_buckets_ - 1);
            if (new_dist < NEIGHBOURHOOD) {
                table_[empty_pos] = std::move(table_[cand]);
                table_[cand].occupied = false;

                size_t old_d = (cand      - cand_home + num_buckets_) & (num_buckets_ - 1);
                size_t new_d = (empty_pos - cand_home + num_buckets_) & (num_buckets_ - 1);
                table_[cand_home].hop_bitmap &= ~(1 << old_d);
                table_[cand_home].hop_bitmap |=  (1 << new_d);

                empty_pos = cand;  // empty slot is now at cand
                displaced = true;
                break;
            }
        }

        if (!displaced) {
            # neighbourhood is too dense to bubble the empty slot any closer.
            lock.unlock();
            do_resize();
            set(key, val, ttl_seconds);
            return;
        }
    }

    # Place new entry
    size_t final_dist = (empty_pos - home + num_buckets_) & (num_buckets_ - 1);
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

        # Mark this slot in home's hop_bitmap
        table_[home].hop_bitmap |= (1 << final_dist);
        num_entries_++;
    }
}


// GET — Look up a key

std::optional<std::string> Store::get(const std::string& key) {
    size_t home   = hash_key(key);
    size_t stripe = stripe_of(home);

    # shared_lock: multiple GETs on same stripe run in parallel.
    std::shared_lock lock(stripes_[stripe]);

    uint32_t bmap = table_[home].hop_bitmap;
    for (int i = 0; i < (int)NEIGHBOURHOOD; i++) {
        if (!(bmap & (1 << i))) continue;  // no key at home+i
        size_t idx = (home + i) & (num_buckets_ - 1);

        if (!table_[idx].occupied) continue;
        if (table_[idx].key != key) continue;

        # Key found, check expiry
        if (is_expired(table_[idx])) {
            table_[idx].occupied   = false;
            table_[idx].has_expiry = false;
            table_[idx].key.clear();
            table_[idx].value.clear();
            table_[home].hop_bitmap &= ~(1u << i);
            num_entries_--;
            return std::nullopt;
        }

        return table_[idx].value;
    }

    return std::nullopt;  # key not found
}

// DEL — Remove a key

bool Store::del(const std::string& key) {
    size_t home   = hash_key(key);
    size_t stripe = stripe_of(home);

    std::unique_lock lock(stripes_[stripe]);

    uint32_t bmap = table_[home].hop_bitmap;
    for (int i = 0; i < (int)NEIGHBOURHOOD; i++) {
        if (!(bmap & (1 << i))) continue;
        size_t idx = (home + i) & (num_buckets_ - 1);

        if (!table_[idx].occupied) continue;
        if (table_[idx].key != key) continue;

        # Clear the entry
        table_[idx].occupied   = false;
        table_[idx].key.clear();
        table_[idx].value.clear();
        table_[idx].has_expiry = false;

        # Clear the bit in home's hop_bitmap
        table_[home].hop_bitmap &= ~(1 << i);

        num_entries_--;
        return true;
    }
    return false;  # key not found
}

// EXPIRE — Set a TTL on an existing key

bool Store::expire(const std::string& key, int seconds_val) {
    size_t home   = hash_key(key);
    size_t stripe = stripe_of(home);

    std::unique_lock lock(stripes_[stripe]);

    uint32_t bmap = table_[home].hop_bitmap;
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


# TTL — Remaining seconds until expiry 
# -1 = no TTL set
# -2 = key doesn't exist

int64_t Store::ttl(const std::string& key) {
    size_t home   = hash_key(key);
    size_t stripe = stripe_of(home);

    std::shared_lock lock(stripes_[stripe]);

    uint32_t bmap = table_[home].hop_bitmap;
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
    return -2;  # key doesn't exist
}

// EXISTS
bool Store::exists(const std::string& key) {
    return get(key).has_value();
}


// DBSIZE — Number of entries in the table
size_t Store::dbsize() {
    return num_entries_.load();
}


// FLUSHALL — Clear everything
void Store::flushall() {
    # Lock all stripes before clearing
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
# Runs every 100ms. Scans all entries and removes expired ones.

void Store::purge_loop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (!running_) break;

        size_t purged = 0;
        for (size_t bucket = 0; bucket < num_buckets_; bucket++) {
            size_t stripe = stripe_of(bucket);

            # Only lock one stripe at a time to avoid holding all locks
            std::unique_lock lock(stripes_[stripe]);

            if (!table_[bucket].occupied) continue;
            if (!table_[bucket].has_expiry) continue;
            if (!is_expired(table_[bucket])) continue;

            # Expire this entry
            size_t home = hash_key(table_[bucket].key);
            size_t dist = (bucket - home + num_buckets_) & (num_buckets_ - 1);
            if (dist < NEIGHBOURHOOD)
                table_[home].hop_bitmap &= ~(1 << dist);

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


// DO_RESIZE — Double the table and rehash every live entry
# Doubling keeps the amortised cost of N inserts at O(N).
static bool resize_insert(std::vector<Entry>& tbl,
                           size_t new_buckets,
                           Entry&& e)
{
    # Recompute home in the new table
    uint64_t hash = 14695981039346656037ULL;
    for (unsigned char c : e.key) { hash ^= c; hash *= 1099511628211ULL; }
    size_t home = hash & (new_buckets - 1);

    # Find nearest empty slot
    size_t empty_pos = SIZE_MAX;
    for (size_t i = 0; i < new_buckets; i++) {
        size_t idx = (home + i) & (new_buckets - 1);
        if (!tbl[idx].occupied) { empty_pos = idx; break; }
    }
    if (empty_pos == SIZE_MAX) return false;  # new table also full 

    # Displace until empty_pos is within neighbourhood of home
    while (true) {
        size_t dist = (empty_pos - home + new_buckets) & (new_buckets - 1);
        if (dist < NEIGHBOURHOOD) break;

        bool displaced = false;
        for (int j = (int)NEIGHBOURHOOD - 1; j >= 1; j--) {
            size_t cand = (empty_pos - j + new_buckets) & (new_buckets - 1);
            if (!tbl[cand].occupied) continue;

            uint64_t ch = 14695981039346656037ULL;
            for (unsigned char c : tbl[cand].key) { ch ^= c; ch *= 1099511628211ULL; }
            size_t cand_home = ch & (new_buckets - 1);

            size_t new_dist = (empty_pos - cand_home + new_buckets) & (new_buckets - 1);
            if (new_dist < NEIGHBOURHOOD) {
                tbl[empty_pos] = std::move(tbl[cand]);
                tbl[cand].occupied = false;

                size_t old_d = (cand      - cand_home + new_buckets) & (new_buckets - 1);
                size_t new_d = (empty_pos - cand_home + new_buckets) & (new_buckets - 1);
                tbl[cand_home].hop_bitmap &= ~(1u << old_d);
                tbl[cand_home].hop_bitmap |=  (1u << new_d);

                empty_pos = cand;
                displaced = true;
                break;
            }
        }
        if (!displaced) return false; 
    }

    # Place the entry
    size_t final_dist = (empty_pos - home + new_buckets) & (new_buckets - 1);
    tbl[empty_pos]        = std::move(e);
    tbl[empty_pos].occupied   = true;
    tbl[home].hop_bitmap |= (1u << final_dist);
    return true;
}

void Store::do_resize()
{
    # acquire all stripe locks
    std::vector<std::unique_lock<std::shared_mutex>> locks;
    locks.reserve(NUM_STRIPES);
    for (auto& s : stripes_)
        locks.emplace_back(s);

    size_t new_buckets = num_buckets_ * 2;   # always a power of 2
    std::cerr << "[resize] " << num_buckets_
              << " → " << new_buckets << " buckets ("
              << num_entries_.load() << " live entries)\n";

    # build new table 
    std::vector<Entry> rehash_tbl(new_buckets);
    size_t moved = 0;

    for (size_t i = 0; i < num_buckets_; i++) {
        Entry& e = table_[i];
        if (!e.occupied) continue;
        if (is_expired(e))  continue;   # drop expired entries during resize

        # Transfer ownership into new table
        if (resize_insert(rehash_tbl, new_buckets, std::move(e)))
            moved++;
        else
            std::cerr << "[resize] WARNING: failed to re-insert key during resize\n";
    }

    # Swap in the new table 
    table_       = std::move(rehash_tbl);
    num_buckets_ = new_buckets;
    num_entries_ = moved;   # recount
    # Locks released automatically when 'locks' goes out of scope.
}


// SNAPSHOT — Iterate all entries (for RDB serialisation)
# Stripe-by-stripe instead of one global lock to avoid stalling client operations.

void Store::snapshot(SnapshotFn fn) {
    using namespace std::chrono;

    for (size_t bucket = 0; bucket < num_buckets_; bucket++) {
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
