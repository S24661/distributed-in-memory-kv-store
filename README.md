# distributed-in-memory-kv-store

A Redis-compatible distributed in-memory key-value store built from scratch in C++17.
Implements a hopscotch hash table, CLOCK-Pro eviction, WAL group-commit persistence,
and async primary-replica replication — the same core ideas behind Redis, PostgreSQL, and the Linux page cache.

```
╔══════════════════════════════════════════════╗
║      distributed-in-memory-kv-store v1.0     ║
║  Hash: Hopscotch  Eviction: CLOCK-Pro        ║
║  I/O: epoll ET    WAL: Group Commit          ║
║  Replication: Async Primary-Replica          ║
╚══════════════════════════════════════════════╝
```

---

## Features

- **RESP protocol** — works with `redis-cli`, `redis-py`, `ioredis`, and any standard Redis client out of the box
- **Hopscotch hash table** — open-addressed, 8-slot neighbourhood, cache-line-local lookups
- **16-stripe reader-writer locks** — concurrent GETs on different keys run in true parallel
- **CLOCK-Pro eviction** — hot/cold page classification; outperforms LRU on scan-heavy workloads
- **Write-ahead log (WAL)** — double-buffered group commit; state survives crashes and restarts
- **Async primary-replica replication** — PSYNC handshake, full RDB snapshot, ring-buffer backlog for partial resync
- **epoll (Linux) / kqueue (macOS)** — single-threaded, edge-triggered I/O; handles hundreds of concurrent clients
- **TTL / EXPIRE** — lazy expiry on GET + background purge thread every 100 ms
- **47 unit tests** — correctness, TTL, concurrent stress (1 M ops × 16 threads), RESP, WAL, RDB, replication

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLIENT LAYER                            │
│     redis-cli / redis-py / ioredis  (RESP protocol over TCP)    │
└──────────────────────────┬──────────────────────────────────────┘
                           │ TCP :6379
┌──────────────────────────▼──────────────────────────────────────┐
│                       SERVER LAYER                              │
│  EventLoop (epoll ET / kqueue)  ←  single thread               │
│         │                                                       │
│         ▼                                                       │
│  RespParser  →  CommandDispatch                                 │
│                      │              │                           │
│               ┌──────┘              └─────────────┐            │
│               ▼                                   ▼            │
│         Store (Hopscotch HT)          Eviction (CLOCK-Pro)      │
│         16-stripe RW locks                                      │
│               │                                                 │
│               ▼                                                 │
│         WAL (group commit)  ──►  appendonly.aof                 │
│               │                                                 │
│               ▼                                                 │
│         ReplicationManager  ──►  [Replica 1]  [Replica 2] …    │
│           ring-buffer backlog       PSYNC           PSYNC       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Source Map

| File | Responsibility |
|---|---|
| `src/server.h/cpp` | `EventLoop` — epoll (Linux) / kqueue (macOS), edge-triggered, single thread |
| `src/resp.h/cpp` | RESP protocol parser and response builders |
| `src/store.h/cpp` | Hopscotch hash table, 16-stripe `shared_mutex`, FNV-1a hash, background expiry purge |
| `src/eviction.h/cpp` | CLOCK-Pro algorithm — circular array, hot/cold classification, ghost tracking |
| `src/wal.h/cpp` | Write-ahead log — double-buffer, group commit, `fsync` every 2 ms |
| `src/rdb.h/cpp` | Binary point-in-time snapshot — used for initial replica sync |
| `src/replication.h/cpp` | Primary-side replication — `RingBuffer` backlog, per-replica sender threads, `PSYNC` handler |
| `src/replica_client.h/cpp` | Replica-side replication — PSYNC handshake, RDB load, streaming loop, auto-reconnect |
| `tests/test_all.cpp` | 47 unit tests — store, TTL, concurrency, eviction, RESP, WAL, RDB, replication |
| `bench/benchmark.sh` | `redis-benchmark` harness comparing kvstore vs real Redis |

---

## Design Decisions

### Hopscotch hashing instead of `std::unordered_map`

`std::unordered_map` uses chaining — each collision follows a pointer to a random heap address, causing a cache miss (~100 ns). Hopscotch keeps every key within 8 contiguous slots of its home bucket. Those 8 slots fit in a single CPU cache line — lookup is ~1 ns, 100× faster under contention.

**Paper:** Herlihy, Shavit, Tzafrir — *Hopscotch Hashing* (DISC 2008)

### 16-stripe `shared_mutex` instead of a global lock

A single global mutex serialises every GET and SET. With 16 independent stripe locks, operations on different keys run in parallel. `shared_mutex` allows multiple concurrent GETs on the same stripe; only SET/DEL take an exclusive lock.

### CLOCK-Pro eviction instead of LRU

LRU maintains a doubly-linked list — two pointer writes on every GET. CLOCK-Pro sweeps a flat array (cache-friendly). It distinguishes HOT pages (accessed ≥ 2 times) from COLD pages (accessed once), protecting the working set from scan-pattern evictions that destroy LRU hit rate. Used by the Linux kernel's page cache.

**Paper:** Jiang, Chen, Zhang — *CLOCK-Pro: An Effective Improvement of the CLOCK Replacement* (USENIX ATC 2005)

### WAL group commit instead of per-write `fsync`

`fsync()` costs ~10 ms on a spinning disk. Per-write fsync caps throughput at ~100 writes/sec. Double-buffered group commit batches 2 ms of writes into one fsync, achieving 20× higher write throughput. The lock is held only for the O(1) buffer swap, not the slow disk operation — the same technique PostgreSQL uses.

### Async replication

The primary returns `+OK` immediately after a write, then propagates the command to replicas in the background. Tradeoff: a crash between `+OK` and replica receipt loses that write on replicas. This is acceptable for a cache layer — the same tradeoff Redis makes by default. For financial data, use synchronous replication or Raft consensus.

### epoll / kqueue edge-triggered I/O

One thread monitors all connections. The OS wakes the thread only when a connection has data ready. No CPU is wasted polling idle clients. This is how real Redis serves 100 K clients on a single thread.

---

## Build

### Prerequisites

| Platform | Command |
|---|---|
| Linux | `sudo apt-get install cmake g++ redis-tools` |
| macOS | `brew install cmake git redis` |

### Compile

```bash
git clone <repo-url>
cd distributed-in-memory-kv-store
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)          # Linux
# make -j$(sysctl -n hw.ncpu)   # macOS
```

Produces two binaries inside `build/`:

| Binary | Purpose |
|---|---|
| `kvstore` | The server |
| `run-tests` | Unit test runner |

---

## Run

### Single node

```bash
./kvstore --port 6379
```

### Primary + replica

```bash
# Terminal 1 — primary
./kvstore --port 6379

# Terminal 2 — replica
./kvstore --port 6380 --replicaof 127.0.0.1 6379
```

### CLI flags

| Flag | Default | Description |
|---|---|---|
| `--port <n>` | `6379` | TCP port to listen on |
| `--replicaof <host> <port>` | — | Start as replica of given primary |
| `--role primary\|replica` | `primary` | Explicit role declaration |
| `--wal <path>` | `appendonly.aof` | WAL file path |

---

## Commands

```
PING [message]
SET  key value [EX seconds]
GET  key
DEL  key [key …]
EXISTS key
TTL  key
EXPIRE key seconds
MSET k1 v1 [k2 v2 …]
MGET key [key …]
DBSIZE
FLUSHALL
INFO [replication]
REPLICAOF host port
REPLICAOF NO ONE
PSYNC replid offset
```

---

## Usage Examples

### Basic operations

```bash
redis-cli -p 6379 PING
# PONG

redis-cli -p 6379 SET name "Alice"
# OK

redis-cli -p 6379 GET name
# "Alice"

redis-cli -p 6379 SET session "tok_abc" EX 10   # expires in 10 s
redis-cli -p 6379 TTL session
# (integer) 9

redis-cli -p 6379 MSET city "Bangalore" lang "C++"
redis-cli -p 6379 MGET city lang
# 1) "Bangalore"
# 2) "C++"

redis-cli -p 6379 DBSIZE
# (integer) 3
```

### Replication

```bash
# Write to primary
redis-cli -p 6379 SET university "IIT Roorkee"
# OK

# Read from replica — propagated asynchronously
redis-cli -p 6380 GET university
# "IIT Roorkee"

# Replica rejects writes
redis-cli -p 6380 SET x y
# (error) READONLY You can't write against a read only replica.

# Inspect replication state
redis-cli -p 6379 INFO replication
# role:primary
# connected_slaves:1
# slave0:ip=127.0.0.1,port=6380,offset=42,lag=0
```

### Dynamic failover

```bash
# Promote replica to standalone primary (no restart required)
redis-cli -p 6380 REPLICAOF NO ONE
# OK

redis-cli -p 6380 SET new_key "value"
# OK
```

### WAL persistence

```bash
# Start server, write some keys, then kill it (Ctrl-C)
./kvstore --port 6379
redis-cli -p 6379 SET counter 100

# Restart — keys are restored from appendonly.aof
./kvstore --port 6379
# [init] Restored 1 keys from WAL

redis-cli -p 6379 GET counter
# "100"
```

---

## Tests

```bash
cd build
./run-tests
```

Expected output:

```
── Store: Basic Operations ──
  [✓] SET and GET
  [✓] GET missing key returns nullopt
  [✓] SET overwrites existing key
  [✓] DEL existing key
  [✓] DEL missing key returns false
  [✓] EXISTS
  [✓] DBSIZE
  [✓] FLUSHALL clears store

── Store: TTL ──
  [✓] Key with TTL expires
  [✓] TTL returns remaining seconds
  [✓] TTL returns -1 for key without expiry
  [✓] TTL returns -2 for missing key
  [✓] EXPIRE sets TTL on existing key

── Store: Concurrency ──
  [✓] Concurrent SET from multiple threads (no crash)
  [✓] Concurrent GET and SET (no crash, no data corruption)

── Eviction: CLOCK-Pro ──
  [✓] Eviction triggers at capacity
  [✓] Frequently accessed key (HOT) not evicted first
  [✓] is_cached returns false after eviction
  [✓] on_delete removes from tracking

── RESP Parser ──
  [✓] Parse SET command
  [✓] Parse PING command
  [✓] Incomplete command returns empty
  [✓] Multiple commands in buffer
  [✓] Response builders

── WAL: Persistence ──
  [✓] WAL logs and replays commands
  [✓] WAL persists across restart (state reconstruction)

── Replication ──
  [✓] Primary-replica full sync
  ...

Results: 47/47 passed ✓
```

---

## Benchmarks

Run with the server live on port 6379:

```bash
cd bench
bash benchmark.sh
```

| Operation | kvstore | Redis 7.0 | Ratio |
|---|---|---|---|
| SET | ~580 K ops/sec | ~800 K ops/sec | ~72% |
| GET | ~620 K ops/sec | ~850 K ops/sec | ~73% |
| p50 GET latency | ~0.07 ms | ~0.06 ms | — |
| p99 GET latency | ~0.24 ms | ~0.18 ms | — |

*500 K requests, 50 concurrent clients, no pipelining, local loopback.*

The ~27% gap vs Redis comes from Redis's decades of micro-optimisations, jemalloc allocation, and a hand-tuned skip-list. For a project built from scratch these numbers are a strong baseline.

---

## Research References

1. **Hopscotch Hashing** — Herlihy, Shavit, Tzafrir (DISC 2008) — neighbourhood-bounded open addressing for cache-line locality
2. **CLOCK-Pro** — Jiang, Chen, Zhang (USENIX ATC 2005) — hot/cold page classification outperforming LRU on scan workloads
3. **PostgreSQL WAL** — group commit design amortising `fsync` cost across batched writes
4. **The C10K Problem** — Dan Kegel (1999) — foundation for event-driven single-threaded servers

---

