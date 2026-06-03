
// resp.cpp — RESP Protocol Parser Implementation

#include "resp.h"
#include <stdexcept>

std::string RespParser::read_line(
    const std::string& buf, size_t pos, size_t& end)
{
    size_t crlf = buf.find("\r\n", pos);
    if (crlf == std::string::npos) {
        end = pos;
        return "";  // incomplete 
    }
    end = crlf + 2;  
    return buf.substr(pos, crlf - pos);
}


// Parses one complete RESP command from buf
std::vector<std::string> RespParser::parse(
    const std::string& buf, size_t& consumed)
{
    consumed = 0;
    if (buf.empty()) return {};

    size_t pos = 0;

    // All commands are RESP Arrays, which start with '*'
    if (buf[pos] != '*') {
        // Find \r\n and split by spaces
        size_t end;
        std::string line = read_line(buf, pos, end);
        if (line.empty()) return {};  // incomplete
        consumed = end;
        // Split by spaces
        std::vector<std::string> args;
        std::string token;
        for (char c : line) {
            if (c == ' ') { if (!token.empty()) args.push_back(token); token.clear(); }
            else token += c;
        }
        if (!token.empty()) args.push_back(token);
        return args;
    }
    pos++;  // skip '*'

    // Read number of arguments: "*3" 
    size_t end;
    std::string count_str = read_line(buf, pos, end);
    if (count_str.empty()) return {};  // incomplete
    int num_args;
    try { num_args = std::stoi(count_str); }
    catch (...) { return {"__ERROR__"}; }
    pos = end;

    std::vector<std::string> args;
    args.reserve(num_args);

    for (int i = 0; i < num_args; i++) {
        if (pos >= buf.size()) return {};  // incomplete
        if (buf[pos] != '$') return {"__ERROR__"};
        pos++;  // skip '$'

        // Read length
        std::string len_str = read_line(buf, pos, end);
        if (len_str.empty()) return {};  // incomplete
        int len;
        try { len = std::stoi(len_str); }
        catch (...) { return {"__ERROR__"}; }
        pos = end;

        // Read exactly 'len' bytes + \r\n
        if (pos + (size_t)len + 2 > buf.size()) return {};  // incomplete
        args.push_back(buf.substr(pos, len));
        pos += len + 2;  // skip data + \r\n
    }

    consumed = pos;  // tell caller how many bytes we consumed
    return args;
}

// Response builders 

std::string RespParser::ok() { return "+OK\r\n"; }

// Sent in response to PING. Used by clients as a health check.
std::string RespParser::pong() { return "+PONG\r\n"; }

// Null bulk string — means "key not found".
std::string RespParser::nil() { return "$-1\r\n"; }

// Error response. redis-cli displays this in red.
std::string RespParser::error(const std::string& msg) {
    return "-ERR " + msg + "\r\n";
}

// Bulk string — used to return a value.
std::string RespParser::bulk(const std::string& s) {
    return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}

// Integer response — used for counts, booleans (0/1), TTL values.
std::string RespParser::integer(int64_t n) {
    return ":" + std::to_string(n) + "\r\n";
}
// Simple string — for short status messages.
std::string RespParser::simple(const std::string& s) {
    return "+" + s + "\r\n";
}
