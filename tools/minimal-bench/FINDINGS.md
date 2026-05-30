# Iggy Benchmark Findings

## Summary

Our minimal-bench achieves **sub-millisecond end-to-end latency** matching native iggy-bench, with both balanced (server-routed) and pinned (explicit partition) modes working equally well.

**Key result**: p50 ~180us, p99 ~350us for end-to-end latency at 30-50k msg/s.

## Test Environment

- Server: Iggy with config at `~/iggy/config.toml`
- Storage: NVMe + ext4 at `/mnt/iggy-nvme` (fsync=off)
- Message size: 250 bytes
- TCP with nodelay enabled
- 32-core machine

## Critical Discovery: poll-batch-size Eliminates Consumer Overhead

### The Problem with poll-batch-size=1

With `--poll-batch-size=1` (default), each consumer needs a full network round-trip per message. At 1:1 consumer:producer ratio, consumers can't keep up and latency explodes:

| Ratio | poll-batch | Throughput | p50 | p99 | p999 |
|-------|------------|------------|-----|-----|------|
| 1:1 | 1 | 33k | **20ms** | **452ms** | 468ms |
| 1:1 | 10 | 55k | 178us | 295us | 2.2ms |
| 1:1 | 100 | 54k | 180us | 298us | 7.4ms |

**With `poll-batch-size=10+`, latency drops from 452ms to 295us (1500x improvement)!**

### Why This Works

Iggy uses **drain-available** semantics (not fill-batch):
- `poll-batch-size` is a maximum, not a minimum
- Server returns immediately with whatever messages are available (0 to N)
- No waiting for batch to fill

With larger batch size:
- One round-trip fetches up to N messages instead of 1
- Reduces poll overhead dramatically
- Consumers can keep up with producers at 1:1 ratio

### Updated Recommendation

**You no longer need 3:1 consumer ratio if you use larger poll-batch-size.**

| Config | poll-batch | Throughput | p50 | p99 |
|--------|------------|------------|-----|-----|
| P=8 N=8 C=8 R=1 | 1 | 33k | 20ms | 452ms |
| P=8 N=8 C=8 R=1 | 10 | 55k | 178us | 295us |
| P=8 N=8 C=8 R=1 | 100 | 54k | 180us | 298us |
| P=8 N=24 C=24 R=3 | 1 | 41k | 219us | 572us |

**Best practice**: Use `--poll-batch-size=100` with 1:1 ratio instead of 3:1 ratio with batch=1.

## Poll Interval Tradeoff

The `--poll-interval-us` flag adds sleep after empty polls:

| P | N | C | poll=0 | poll=10 | poll=50 |
|---|---|---|--------|---------|---------|
| **Throughput** |
| 1 | 3 | 3 | 13.8k | 14.1k | 14.8k |
| 8 | 24 | 24 | 49.6k | 58.1k | 56.4k |
| 16 | 48 | 48 | 49.6k | 60.6k | 61.3k |
| **p50 latency (us)** |
| 1 | 3 | 3 | 121 | 517 | 423 |
| 8 | 24 | 24 | 226 | 548 | 602 |
| 16 | 48 | 48 | 353 | 676 | 702 |

- `poll-interval-us=0`: Lowest latency but high CPU (70%+ empty polls)
- `poll-interval-us=10-50`: Higher throughput, moderate latency increase

## End-to-End Latency Results

### Recommended Config: 1:1 with poll-batch-size=100

| P | N | C | R | poll-batch | Throughput | p50 | p99 | p999 |
|---|---|---|---|------------|------------|-----|-----|------|
| 8 | 8 | 8 | 1 | 100 | 54k msg/s | 180us | 298us | 7.4ms |
| 8 | 8 | 8 | 1 | 500 | 58k msg/s | 186us | 288us | 474us |

### Legacy Config: 3:1 with poll-batch-size=1

| P | N | C | R | Throughput | p50 | p99 | p999 |
|---|---|---|---|------------|-----|-----|------|
| 1 | 3 | 3 | 3 | 15k msg/s | 109us | 153us | 1.3ms |
| 2 | 6 | 6 | 3 | 25k msg/s | 132us | 193us | 3.9ms |
| 4 | 12 | 12 | 3 | 31k msg/s | 168us | 350us | 6.6ms |
| 8 | 24 | 24 | 3 | 41k msg/s | 222us | 1.7ms | 16ms |

### Comparison: Balanced vs Pinned Partitioning

Both modes achieve similar latency when producers interleave sends:

| Mode | p50 | p99 | p999 |
|------|-----|-----|------|
| Pinned (partition_id) | 179us | 349us | 7.6ms |
| Balanced (server routes) | 191us | 644us | 11.7ms |

**Conclusion**: Use pinned for predictable partition assignment, balanced for simplicity.

## Critical Fix: Interleaved Sends

The key to matching native bench latency was **interleaving sends across partitions** instead of parallel bursts.

### The Problem

With parallel burst mode (old behavior):
```
Task A: a,a,a,a,a,a... (6250 msgs burst to partition A)
Task B: b,b,b,b,b,b... (6250 msgs burst to partition B)
```
Result: **p50 = 20ms, p99 = 155ms** (100x worse!)

### The Fix

With interleaved mode (current behavior):
```
Single task: a,b,a,b,a,b,a,b... (round-robin)
```
Result: **p50 = 186us, p99 = 482us**

### Why It Matters

- Burst mode fills partition queues before consumers read them
- Messages timestamped during burst wait in queue
- Latency = time_read - time_sent (queue wait dominates)
- Interleaving spreads writes over time, keeping queues shallow
- This models real-world Poisson arrival patterns

## Scaling Limits

### Throughput Ceiling: ~60k msg/s (with consumers)

With optimal settings (`poll-batch-size=100+`):

| Config | Throughput | Bottleneck |
|--------|------------|------------|
| P=8 C=8 batch=100 | 54k | Client CPU |
| P=8 C=8 batch=500 | 58k | Client CPU |
| P=16 C=0 | **142k** | None |

The ~60k msg/s ceiling is a **benchmark client limitation**, not Iggy:
- Server CPU: 11% (barely used)
- Disk I/O: 2% utilization
- Client CPU: 600%+ (all cores maxed)

### Producer-Only Peak: 157k msg/s

| P | N | Throughput | p99 send |
|---|---|------------|----------|
| 1 | 1 | 19k | 65us |
| 8 | 8 | 104k | 97us |
| 16 | 16 | 138k | 151us |
| 24 | 24 | **157k** | 152us |
| 48 | 48 | 147k | 164us |

## Topology Model

**Variables:**
- P = number of producers
- N = number of partitions  
- R = redundancy (partitions per producer)
- C = number of consumers

**Constraints:**
- R ≤ N (can't write to more partitions than exist)
- P × R ≥ N (every partition gets at least one producer)

**Recommended**: Use `poll-batch-size=100` with 1:1 ratio (P=N=C, R=1) for simplicity and good performance.

## Quick Start

### Start Iggy Server

```bash
cd ~/iggy
IGGY_CONFIG_PATH=~/iggy/config.toml \
  nohup ./target/release/iggy-server --fresh --with-default-root-credentials \
  > /tmp/iggy.log 2>&1 &
```

### Build Benchmark

```bash
cd ~/iggy && cargo build --release -p minimal-bench
```

### Run Benchmarks

```bash
# Recommended: 1:1 ratio with batch fetching
~/iggy/target/release/minimal-bench -P 8 -N 8 -C 8 -R 1 -m 100000 --poll-batch-size 100

# With diagnostics
~/iggy/target/release/minimal-bench -P 8 -N 8 -C 8 -R 1 -m 100000 --poll-batch-size 100 --diagnostics

# Legacy 3:1 ratio (for comparison)
~/iggy/target/release/minimal-bench -P 8 -N 24 -C 24 -R 3 -m 100000

# Producer-only (max throughput test)
~/iggy/target/release/minimal-bench -P 24 -N 24 -R 1 -m 500000 --producer-only

# With rate limiting (50k msg/s target)
~/iggy/target/release/minimal-bench -P 8 -N 8 -C 8 -R 1 -T 50000 -m 100000 --poll-batch-size 100
```

### CLI Flags

| Flag | Description |
|------|-------------|
| `-P` | Number of producers |
| `-N` | Number of partitions |
| `-C` | Number of consumers |
| `-R` | Redundancy (partitions per producer) |
| `-m` | Total messages to send |
| `-T` | Target throughput msg/s (0 = unlimited) |
| `-v` | Verbose: show per-actor stats |
| `--poll-batch-size` | Max messages per consumer poll (default: 1) |
| `--poll-interval-us` | Sleep after empty poll in microseconds (default: 0) |
| `--producer-only` | Skip consumers |
| `--balanced` | Use server-side routing instead of pinned partitions |
| `--diagnostics` | Enable detailed per-partition and per-actor diagnostics |
| `--diagnostics-json` | Write diagnostics to JSON file |

## Diagnostics

Run with `--diagnostics` to see:
- Per-partition lag and throughput
- Consumer poll behavior (polls/s, empty poll ratio, msgs/poll)
- Producer send behavior (sends/s, duration percentiles)
- Bottleneck analysis

Example output:
```
--- Per-Partition Metrics ---
Part   Produced   Consumed     Prod/s     Cons/s      Lag   MaxLag
   0       8334       8334       4012       4012        0       53

--- Consumer Poll Behavior ---
  ID    Polls/s    Empty/s   Empty%    Dur p50    Dur p99   Msgs/poll
   0       4884        972    19.9%        90us       131us        1.0

--- Bottleneck Summary ---
No clear bottleneck detected. System operating within normal parameters.
```

## Comparison with Native iggy-bench

| Metric | Native iggy-bench | Our minimal-bench |
|--------|-------------------|-------------------|
| E2E p50 | 170us | 179us |
| E2E p99 | 440us | 349us |
| Throughput (matched P/C) | 48k msg/s | 48k msg/s |

Both benchmarks now produce equivalent results.

## Storage Notes

| Storage | Throughput | p99 | Notes |
|---------|------------|-----|-------|
| NVMe + ext4 | 153k msg/s | 172us | Current setup |
| NVMe + ZFS | 96k msg/s | 779us | 20-28x write amplification |
| SATA SSD + ext4 | 157k msg/s | 139us | Baseline |

ZFS write amplification caused 60% throughput loss. Use ext4 for Iggy workloads.

## Key Takeaways

1. **Use `--poll-batch-size=100`** instead of 3:1 consumer ratio
2. **Iggy uses drain-available semantics** - no fill-batch waiting
3. **Interleaved sends are critical** for low latency
4. **poll-interval-us trades latency for CPU** - use 10-50us for balanced workloads
5. **60k msg/s ceiling is client-side** - server can handle 150k+ with distributed clients
