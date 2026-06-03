#pragma once
// rdb.h — RDB Snapshot (Point-in-Time Binary Dump)

#include "store.h"
#include <string>
#include <vector>
#include <cstdint>

// One entry in an RDB snapshot
struct RDBEntry {
    std::string key;
    std::string value;
    int64_t     ttl_ms = 0;   // 0 = no expiry; else ms since epoch
};

class RDB {
public:
    // Serialise the entire store into a binary blob
    static std::vector<uint8_t> serialize(Store& store);

    // Deserialise a blob and load all entries into store
    static bool deserialize(const std::vector<uint8_t>& data, Store& store);

    // Write serialised RDB to a file 
    static bool save_to_file(const std::string& path, Store& store);

    // Load RDB from file into store
    static bool load_from_file(const std::string& path, Store& store);

private:
    static constexpr const char* MAGIC = "REDISLITE0001\n";
    static constexpr uint32_t    SENTINEL = 0xFFFFFFFF;

    // Encode a length-prefixed string into buf
    static void write_str(std::vector<uint8_t>& buf, const std::string& s);

    // Decode a length-prefixed string from data at pos
    static bool read_str(const std::vector<uint8_t>& data,
                         size_t& pos, std::string& out);

    // Write a uint64 in little-endian into buf
    static void write_u64(std::vector<uint8_t>& buf, uint64_t v);
    static uint64_t read_u64(const std::vector<uint8_t>& data, size_t& pos);

    // Write a uint32 in little-endian into buf
    static void write_u32(std::vector<uint8_t>& buf, uint32_t v);
    static uint32_t read_u32(const std::vector<uint8_t>& data, size_t& pos);
};
