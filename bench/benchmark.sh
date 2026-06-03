#!/bin/bash

# Compares redis-lite vs real Redis performance


LITE_PORT=6379
REDIS_PORT=6380
NUM_REQUESTS=500000
CLIENTS=50
PIPELINE=1

echo "╔═══════════════════════════════════════════════════╗"
echo "║         redis-lite vs Redis Benchmark             ║"
echo "╚═══════════════════════════════════════════════════╝"
echo ""
echo "Requests: $NUM_REQUESTS | Clients: $CLIENTS | Pipeline: $PIPELINE"
echo ""

# Check if redis-benchmark is available
if ! command -v redis-benchmark &> /dev/null; then
    echo "redis-benchmark not found. Install with:"
    echo "  sudo apt-get install redis-tools"
    exit 1
fi

#  Benchmark redis-lite
echo " Benchmarking redis-lite (port $LITE_PORT)..."
echo "---------------------------------------------------"
redis-benchmark \
    -h 127.0.0.1 \
    -p $LITE_PORT \
    -n $NUM_REQUESTS \
    -c $CLIENTS \
    -P $PIPELINE \
    -t set,get,del,exists,mset \
    -q 2>/dev/null || echo "redis-lite not running on port $LITE_PORT"

echo ""

#  Benchmark real Redis 
if redis-cli -p $REDIS_PORT ping &>/dev/null; then
    echo " Benchmarking real Redis (port $REDIS_PORT)..."
    echo "---------------------------------------------------"
    redis-benchmark \
        -h 127.0.0.1 \
        -p $REDIS_PORT \
        -n $NUM_REQUESTS \
        -c $CLIENTS \
        -P $PIPELINE \
        -t set,get,del,exists,mset \
        -q
else
    echo "ℹ  Real Redis not found on port $REDIS_PORT."
    echo "   To compare, start Redis on port $REDIS_PORT:"
    echo "   redis-server --port $REDIS_PORT --daemonize yes"
fi

echo ""
echo "▶ Latency distribution (redis-lite)..."
echo "---------------------------------------------------"
redis-benchmark \
    -h 127.0.0.1 \
    -p $LITE_PORT \
    -n 100000 \
    -c 10 \
    -t get \
    --latency-history \
    -q 2>/dev/null | head -20 || true

echo ""
echo "▶ Memory usage (redis-lite process)..."
echo "---------------------------------------------------"
PID=$(pgrep redis-lite 2>/dev/null)
if [ -n "$PID" ]; then
    echo "PID: $PID"
    cat /proc/$PID/status | grep -E "VmRSS|VmPeak|Threads"
else
    echo "redis-lite process not found"
fi

echo ""
echo "╔═══════════════════════════════════════════════════╗"
echo "║  Benchmark complete. Add results to README.md!    ║"
echo "╚═══════════════════════════════════════════════════╝"
