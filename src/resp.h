#pragma once
// resp.h — Redis Serialization Protocol parser

#include <string>
#include <vector>
#include <cstdint>

class RespParser {
public:
    // Parse one complete command from a buffer.
    std::vector<std::string> parse(const std::string& buf, size_t& consumed);

    // Response builders 

    static std::string ok();                        // +OK\r\n
    static std::string pong();                      // +PONG\r\n
    static std::string nil();                       // $-1\r\n  (null = key not found)
    static std::string error(const std::string& msg);  // -ERR msg\r\n
    static std::string bulk(const std::string& s);     // $N\r\ndata\r\n
    static std::string integer(int64_t n);              // :N\r\n
    static std::string simple(const std::string& s);   // +s\r\n

private:
    // Read one \r\n-terminated line from buf starting at pos
    std::string read_line(const std::string& buf, size_t pos, size_t& end);
};
