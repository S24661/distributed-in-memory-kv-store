
// rdb.cpp — RDB Snapshot Implementation

#include "rdb.h"
#include <fstream>
#include <iostream>
#include <cstring>
#include <chrono>

using namespace std::chrono;

// BINARY ENCODING HELPERS

void RDB::write_u32(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back((v      ) & 0xFF);
    buf.push_back((v >>  8) & 0xFF);
    buf.push_back((v >> 16) & 0xFF);
    buf.push_back((v >> 24) & 0xFF);
}

uint32_t RDB::read_u32(const std::vector<uint8_t>& data, size_t& pos) {
    if (pos + 4 > data.size()) return 0;
    uint32_t v = (uint32_t)data[pos]
               | ((uint32_t)data[pos+1] << 8)
               | ((uint32_t)data[pos+2] << 16)
               | ((uint32_t)data[pos+3] << 24);
    pos += 4;
    return v;
}

void RDB::write_u64(std::vector<uint8_t>& buf, uint64_t v) {
    for (int i = 0; i < 8; i++)
        buf.push_back((v >> (i * 8)) & 0xFF);
}

uint64_t RDB::read_u64(const std::vector<uint8_t>& data, size_t& pos) {
    if (pos + 8 > data.size()) return 0;
    uint64_t v = 0;
    for (int i = 0; i < 8; i++)
        v |= ((uint64_t)data[pos + i] << (i * 8));
    pos += 8;
    return v;
}

// Write: [4-byte length][N bytes of string]
void RDB::write_str(std::vector<uint8_t>& buf, const std::string& s) {
    write_u32(buf, (uint32_t)s.size());
    buf.insert(buf.end(), s.begin(), s.end());
}

// Read: [4-byte length][N bytes] - out
bool RDB::read_str(const std::vector<uint8_t>& data,
                   size_t& pos, std::string& out)
{
    if (pos + 4 > data.size()) return false;
    uint32_t len = read_u32(data, pos);
    if (pos + len > data.size()) return false;
    out.assign((char*)data.data() + pos, len);
    pos += len;
    return true;
}

// SERIALIZE — Dump entire store to binary blob
std::vector<uint8_t> RDB::serialize(Store& store) {
    std::vector<uint8_t> buf;
    buf.reserve(store.dbsize() * 64);  // rough estimate

    // Write magic header
    const std::string magic(MAGIC);
    buf.insert(buf.end(), magic.begin(), magic.end());

    // Visit every live key-value pair
    store.snapshot([&](const std::string& key,
                       const std::string& val,
                       int64_t ttl_ms)
    {
        write_str(buf, key);
        write_str(buf, val);
        write_u64(buf, (uint64_t)ttl_ms);
    });

    // Write sentinel to mark end
    write_u32(buf, SENTINEL);

    std::cout << "[RDB] Serialised " << store.dbsize()
              << " keys, " << buf.size() << " bytes\n";
    return buf;
}

// DESERIALIZE — Load binary blob into store
bool RDB::deserialize(const std::vector<uint8_t>& data, Store& store) {
    if (data.size() < strlen(MAGIC) + 4) {
        std::cerr << "[RDB] Too small to be valid\n";
        return false;
    }

    // Verify magic header
    std::string magic(MAGIC);
    if (memcmp(data.data(), magic.c_str(), magic.size()) != 0) {
        std::cerr << "[RDB] Invalid magic header\n";
        return false;
    }

    size_t pos = magic.size();
    size_t loaded = 0;
    auto now_ms = duration_cast<milliseconds>(
        steady_clock::now().time_since_epoch()).count();

    while (pos < data.size()) {
        // Check for sentinel
        if (pos + 4 <= data.size()) {
            uint32_t peek = (uint32_t)data[pos]
                          | ((uint32_t)data[pos+1] << 8)
                          | ((uint32_t)data[pos+2] << 16)
                          | ((uint32_t)data[pos+3] << 24);
            if (peek == SENTINEL) break;
        }

        std::string key, val;
        if (!read_str(data, pos, key)) break;
        if (!read_str(data, pos, val)) break;
        uint64_t ttl_ms = read_u64(data, pos);

        // Skip already-expired keys
        if (ttl_ms > 0 && (int64_t)ttl_ms < now_ms) continue;

        // Calculate remaining TTL in seconds
        std::optional<int> ttl_secs;
        if (ttl_ms > 0) {
            int64_t remaining_ms = (int64_t)ttl_ms - now_ms;
            ttl_secs = (int)(remaining_ms / 1000);
            if (*ttl_secs <= 0) continue;
        }

        store.set(key, val, ttl_secs);
        loaded++;
    }

    std::cout << "[RDB] Loaded " << loaded << " keys\n";
    return true;
}

// FILE I/O
bool RDB::save_to_file(const std::string& path, Store& store) {
    auto data = serialize(store);
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (!f.is_open()) {
        std::cerr << "[RDB] Cannot open " << path << " for writing\n";
        return false;
    }
    f.write((char*)data.data(), data.size());
    f.flush();
    std::cout << "[RDB] Saved to " << path << "\n";
    return true;
}

bool RDB::load_from_file(const std::string& path, Store& store) {
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) {
        std::cout << "[RDB] No snapshot at " << path << "\n";
        return false;
    }
    std::vector<uint8_t> data(
        (std::istreambuf_iterator<char>(f)),
        std::istreambuf_iterator<char>());
    return deserialize(data, store);
}
