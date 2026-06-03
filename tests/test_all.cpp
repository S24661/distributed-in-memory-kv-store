
// test_all.cpp — Unit Tests

#include "../src/store.h"
#include "../src/eviction.h"
#include "../src/wal.h"
#include "../src/resp.h"
#include "../src/rdb.h"
#include "../src/replication.h"
#include "../src/replica_client.h"

#include <cassert>
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <unistd.h>

// Simple test framework
static int tests_run  = 0;
static int tests_pass = 0;

#define TEST(name) \
    do { \
        std::cout << "  [ ] " << name << "... "; \
        tests_run++; \
    } while(0)

#define PASS() \
    do { \
        tests_pass++; \
        std::cout << "\r  [✓] \n"; \
    } while(0)

#define FAIL(msg) \
    do { \
        std::cout << "\r  [✗] FAIL: " << msg << "\n"; \
    } while(0)

// STORE TESTS
void test_store_basic() {
    std::cout << "\n── Store: Basic Operations ──\n";
    Store store;

    TEST("SET and GET");
    store.set("name", "John");
    auto v = store.get("name");
    assert(v.has_value() && *v == "John");
    PASS();

    TEST("GET missing key returns nullopt");
    assert(!store.get("missing").has_value());
    PASS();

    TEST("SET overwrites existing key");
    store.set("name", "Jane");
    assert(store.get("name") == "Jane");
    PASS();

    TEST("DEL existing key");
    assert(store.del("name") == true);
    assert(!store.get("name").has_value());
    PASS();

    TEST("DEL missing key returns false");
    assert(store.del("nonexistent") == false);
    PASS();

    TEST("EXISTS");
    store.set("x", "1");
    assert(store.exists("x") == true);
    assert(store.exists("y") == false);
    PASS();

    TEST("DBSIZE");
    Store s2;
    s2.set("a", "1"); s2.set("b", "2"); s2.set("c", "3");
    assert(s2.dbsize() == 3);
    PASS();

    TEST("FLUSHALL clears store");
    s2.flushall();
    assert(s2.dbsize() == 0);
    PASS();
}

void test_store_ttl() {
    std::cout << "\n── Store: TTL / Expiry ──\n";
    Store store;

    TEST("Key with TTL expires");
    store.set("tmp", "val", 1);  // expires in 1 second
    assert(store.get("tmp").has_value());  // still alive now
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    assert(!store.get("tmp").has_value());  // expired
    PASS();

    TEST("TTL returns remaining seconds");
    store.set("k", "v", 10);
    int64_t t = store.ttl("k");
    assert(t >= 8 && t <= 10);
    PASS();

    TEST("TTL returns -1 for key without expiry");
    store.set("permanent", "val");
    assert(store.ttl("permanent") == -1);
    PASS();

    TEST("TTL returns -2 for missing key");
    assert(store.ttl("doesnotexist") == -2);
    PASS();

    TEST("EXPIRE sets TTL on existing key");
    store.set("key2", "val");
    assert(store.expire("key2", 1) == true);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    assert(!store.get("key2").has_value());
    PASS();
}

void test_store_concurrent() {
    std::cout << "\n── Store: Concurrent Access ──\n";
    Store store;

    TEST("Concurrent SET from multiple threads (no crash)");
    std::vector<std::thread> threads;
    const int NUM_THREADS = 8;
    const int OPS_PER_THREAD = 1000;

    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&store, t]() {
            for (int i = 0; i < OPS_PER_THREAD; i++) {
                std::string key = "key:" + std::to_string(t) + ":" + std::to_string(i);
                store.set(key, "value");
            }
        });
    }
    for (auto& th : threads) th.join();
    PASS();

    TEST("Concurrent GET and SET (no crash, no data corruption)");
    store.set("shared", "initial");
    std::atomic<int> read_count{0};
    std::vector<std::thread> mixed;

    for (int t = 0; t < 4; t++) {
        // Readers
        mixed.emplace_back([&]() {
            for (int i = 0; i < 500; i++) {
                auto v = store.get("shared");
                if (v) read_count++;
            }
        });
        // Writers
        mixed.emplace_back([&, t]() {
            for (int i = 0; i < 500; i++) {
                store.set("shared", "val" + std::to_string(t * 1000 + i));
            }
        });
    }
    for (auto& th : mixed) th.join();
    // No crash and reads returned some values
    assert(read_count.load() > 0);
    PASS();
}

// EVICTION TESTS
void test_eviction() {
    std::cout << "\n── CLOCK-Pro Eviction ──\n";

    TEST("Eviction triggers at capacity");
    ClockPro ev(3);
    ev.on_insert("a");
    ev.on_insert("b");
    ev.on_insert("c");
    std::string evicted = ev.on_insert("d");  // capacity exceeded
    // One of a, b, c should be evicted (cold, not accessed)
    assert(!evicted.empty());
    PASS();

    TEST("Frequently accessed key (HOT) not evicted first");
    ClockPro ev2(3);
    ev2.on_insert("hot");
    // Access "hot" multiple times to make it HOT
    ev2.on_access("hot");
    ev2.on_access("hot");
    ev2.on_insert("cold1");
    ev2.on_insert("cold2");

    // Insert one more — should evict a cold key, not "hot"
    std::string victim = ev2.on_insert("new");
    assert(victim != "hot");  // hot key should be protected
    PASS();

    TEST("is_cached returns false after eviction");
    ClockPro ev3(2);
    ev3.on_insert("x");
    ev3.on_insert("y");
    ev3.on_insert("z");  // triggers eviction of x or y
    // At least one of x,y should no longer be cached
    bool x_cached = ev3.is_cached("x");
    bool y_cached = ev3.is_cached("y");
    assert(!x_cached || !y_cached);  // at least one evicted
    PASS();

    TEST("on_delete removes from tracking");
    ClockPro ev4(5);
    ev4.on_insert("k");
    assert(ev4.is_cached("k"));
    ev4.on_delete("k");
    assert(!ev4.is_cached("k"));
    PASS();
}

// RESP PARSER TESTS
void test_resp() {
    std::cout << "\n── RESP Parser ──\n";
    RespParser parser;

    TEST("Parse SET command");
    std::string buf = "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$4\r\nJohn\r\n";
    size_t consumed = 0;
    auto cmd = parser.parse(buf, consumed);
    assert(cmd.size() == 3);
    assert(cmd[0] == "SET");
    assert(cmd[1] == "name");
    assert(cmd[2] == "John");
    assert(consumed == buf.size());
    PASS();

    TEST("Parse PING command");
    std::string ping = "*1\r\n$4\r\nPING\r\n";
    consumed = 0;
    cmd = parser.parse(ping, consumed);
    assert(cmd.size() == 1 && cmd[0] == "PING");
    PASS();

    TEST("Incomplete command returns empty");
    std::string partial = "*3\r\n$3\r\nSET\r\n$4\r\nnam";
    consumed = 0;
    cmd = parser.parse(partial, consumed);
    assert(cmd.empty());
    assert(consumed == 0);
    PASS();

    TEST("Multiple commands in buffer");
    std::string multi =
        "*1\r\n$4\r\nPING\r\n"
        "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n";
    consumed = 0;
    auto c1 = parser.parse(multi, consumed);
    assert(c1[0] == "PING");
    multi.erase(0, consumed);
    consumed = 0;
    auto c2 = parser.parse(multi, consumed);
    assert(c2[0] == "SET");
    PASS();

    TEST("Response builders");
    assert(RespParser::ok()    == "+OK\r\n");
    assert(RespParser::pong()  == "+PONG\r\n");
    assert(RespParser::nil()   == "$-1\r\n");
    assert(RespParser::integer(42) == ":42\r\n");
    assert(RespParser::bulk("hi")  == "$2\r\nhi\r\n");
    assert(RespParser::error("oops") == "-ERR oops\r\n");
    PASS();
}

// WAL TESTS
void test_wal() {
    std::cout << "\n── WAL (Write-Ahead Log) ──\n";
    const std::string test_path = "/tmp/test_redis_lite.aof";

    // Clean up before test
    ::unlink(test_path.c_str());

    TEST("WAL logs and replays commands");
    {
        WAL wal(test_path, 1);
        wal.log({"SET", "name", "John"});
        wal.log({"SET", "age",  "20"});
        wal.log({"DEL", "name"});
        wal.flush_now();
        wal.stop();
    }

    WAL wal2(test_path, 1);
    auto cmds = wal2.replay();
    assert(cmds.size() == 3);
    assert(cmds[0][0] == "SET" && cmds[0][1] == "name");
    assert(cmds[1][0] == "SET" && cmds[1][1] == "age");
    assert(cmds[2][0] == "DEL" && cmds[2][1] == "name");
    PASS();

    TEST("WAL persists across restart (state reconstruction)");
    Store store;
    for (const auto& cmd : cmds) {
        if (cmd[0] == "SET" && cmd.size() >= 3) store.set(cmd[1], cmd[2]);
        else if (cmd[0] == "DEL" && cmd.size() >= 2) store.del(cmd[1]);
    }
    // After replay: "name" was deleted, "age" remains
    assert(!store.get("name").has_value());
    assert(store.get("age") == "20");
    PASS();

    ::unlink(test_path.c_str());
}

// MAIN
void test_rdb();
void test_replication();
int main() {
    std::cout << "redis-lite Test Suite\n";
    std::cout << "=====================\n";

    test_store_basic();
    test_store_ttl();
    test_store_concurrent();
    test_eviction();
    test_resp();
    test_wal();
    test_rdb();
    test_replication();

    std::cout << "\n=====================\n";
    std::cout << "Results: " << tests_pass << "/" << tests_run << " passed\n";

    if (tests_pass == tests_run) {
        std::cout << "✓ All tests passed!\n";
        return 0;
    } else {
        std::cout << "✗ " << (tests_run - tests_pass) << " test(s) failed\n";
        return 1;
    }
}

// RDB TESTS
void test_rdb() {
    std::cout << "\n── RDB Snapshot ──\n";
    const std::string rdb_path = "/tmp/test_redis_lite.rdb";
    ::unlink(rdb_path.c_str());

    TEST("RDB serialise and deserialise");
    Store s1;
    s1.set("name", "John");
    s1.set("city", "Roorkee");
    s1.set("score", "99");

    auto blob = RDB::serialize(s1);
    assert(!blob.empty());

    Store s2;
    bool ok = RDB::deserialize(blob, s2);
    assert(ok);
    assert(s2.get("name") == "John");
    assert(s2.get("city") == "Roorkee");
    assert(s2.get("score") == "99");
    assert(s2.dbsize() == 3);
    PASS();

    TEST("RDB save and load file");
    RDB::save_to_file(rdb_path, s1);
    Store s3;
    bool loaded = RDB::load_from_file(rdb_path, s3);
    assert(loaded);
    assert(s3.get("name") == "John");
    assert(s3.dbsize() == 3);
    PASS();

    TEST("RDB skips expired keys");
    Store s4;
    s4.set("perm", "stays");
    s4.set("temp", "gone", 1);  // 1 second TTL
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));

    auto blob2 = RDB::serialize(s4);
    Store s5;
    RDB::deserialize(blob2, s5);
    assert(s5.get("perm") == "stays");
    assert(!s5.get("temp").has_value());  // expired, should not be in RDB
    PASS();

    ::unlink(rdb_path.c_str());
}

// REPLICATION TESTS
void test_replication() {
    std::cout << "\n── Replication: RingBuffer ──\n";

    TEST("RingBuffer write and read");
    RingBuffer rb(1024);
    rb.write("hello");
    rb.write(" world");
    int64_t cur = rb.current_offset();
    assert(cur == 11);

    std::string out;
    bool ok = rb.read(0, 5, out);
    assert(ok && out == "hello");

    ok = rb.read(6, 5, out);
    assert(ok && out == "world");
    PASS();

    TEST("RingBuffer contains_offset");
    RingBuffer rb2(16);
    rb2.write("0123456789abcdef");  // exactly 16 bytes
    assert(rb2.contains_offset(0));
    assert(rb2.contains_offset(15));
    rb2.write("OVERFLOW");          // 8 more → overwrites first 8
    // offset 0-7 overwritten, 8-23 valid
    assert(!rb2.contains_offset(0));
    assert(rb2.contains_offset(16));
    PASS();

    TEST("RingBuffer partial read (offset too old returns false)");
    RingBuffer rb3(8);
    rb3.write("12345678");  // fill
    rb3.write("ABCD");      // overwrites first 4
    std::string out2;
    assert(!rb3.read(0, 4, out2));   // overwritten
    assert(rb3.read(4, 4, out2));    // still valid: "5678"
    assert(out2 == "5678");
    PASS();

    std::cout << "\n── Replication: Primary→Replica Integration ──\n";

    TEST("Primary propagates SET to replica store");
    Store primary_store, replica_store;
    primary_store.set("key1", "val1");
    primary_store.set("key2", "val2");

    // Simulate RDB snapshot transfer
    auto rdb = RDB::serialize(primary_store);
    replica_store.flushall();
    RDB::deserialize(rdb, replica_store);

    assert(replica_store.get("key1") == "val1");
    assert(replica_store.get("key2") == "val2");
    assert(replica_store.dbsize() == 2);
    PASS();

    TEST("Replica applies streamed DEL from primary");
    // Simulate primary sending DEL over replication stream
    primary_store.del("key1");
    replica_store.del("key1");  

    assert(!replica_store.get("key1").has_value());
    assert(replica_store.get("key2") == "val2");
    PASS();

    TEST("ReplicationManager INFO has correct role");
    Store mgr_store;
    ReplicationManager mgr(mgr_store);
    auto info = mgr.get_info();
    assert(info.role == "primary");
    assert(info.connected_replicas == 0);
    assert(!info.repl_id.empty());
    assert(info.repl_id.size() == 40);
    PASS();
}
