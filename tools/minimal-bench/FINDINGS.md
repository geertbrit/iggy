# Iggy Producer Benchmark Findings

## Test Environment
- Server settings: `IGGY_SYSTEM_PARTITION_MESSAGES_REQUIRED_TO_SAVE=1`, `IGGY_SYSTEM_PARTITION_ENFORCE_FSYNC=true`
- Message size: 250 bytes
- TCP with nodelay enabled
- Rust benchmark tool with parallel sends per partition

## Topology Model

**Variables:**
- P = number of producers
- N = number of partitions  
- R = redundancy (partitions per producer)

**Constraints:**
- R ≤ N (can't write to more partitions than exist)
- P × R ≥ N (every partition gets at least one producer)

**Examples:**
- P=16, N=16, R=1: Each producer owns 1 partition (no overlap)
- P=16, N=16, R=4: Each producer writes to 4 partitions (overlap)
- P=16, N=16, R=16: Fully connected (every producer → every partition)

## Results: Scaling Producers (R=1, dedicated partitions)

| P | N | Throughput | p50 (us) | p99 (us) | p999 (us) |
|---|---|------------|----------|----------|-----------|
| 1 | 1 | 19k msg/s | 49 | 65 | 73 |
| 2 | 2 | 36k msg/s | 53 | 73 | 85 |
| 4 | 4 | 54k msg/s | 65 | 95 | 108 |
| 8 | 8 | 104k msg/s | 64 | 97 | 113 |
| 16 | 16 | 138k msg/s | 77 | 151 | 194 |
| 24 | 24 | **157k msg/s** | 76 | 152 | 201 |
| 32 | 32 | 153k msg/s | 78 | 154 | 269 |
| 48 | 48 | 147k msg/s | 79 | 164 | 292 |
| 64 | 64 | 148k msg/s | 80 | 172 | 306 |

**Observations:**
- Linear scaling up to P=8 (~13k msg/s per producer)
- Diminishing returns after P=16
- Peak throughput at P=24 (157k msg/s)
- Plateau at P≥32 (~150k msg/s)
- Send latency stays sub-200us p99 throughout

## Results: Effect of Redundancy (R) with Parallel Sends

### P=16, N=16

| R | Throughput | p50 (us) | p99 (us) | p999 (us) |
|---|------------|----------|----------|-----------|
| 1 | 138k msg/s | 77 | 151 | 194 |
| 2 | 137k msg/s | 83 | 244 | 317 |
| 4 | 133k msg/s | 85 | 342 | 489 |
| 8 | 129k msg/s | 78 | 438 | 520 |
| 16 | 84k msg/s | 81 | 169 | 226 |

### P=24, N=24

| R | Throughput | p50 (us) | p99 (us) | p999 (us) |
|---|------------|----------|----------|-----------|
| 1 | 157k msg/s | 76 | 152 | 201 |
| 2 | 153k msg/s | 79 | 166 | 254 |
| 4 | 153k msg/s | 81 | 186 | 681 |
| 24 | 39k msg/s | 78 | 169 | 367 |

### P=32, N=32

| R | Throughput | p50 (us) | p99 (us) | p999 (us) |
|---|------------|----------|----------|-----------|
| 1 | 153k msg/s | 78 | 154 | 269 |
| 2 | 150k msg/s | 80 | 165 | 315 |
| 4 | 150k msg/s | 80 | 165 | 286 |

**Observations:**
- R=1 optimal but R=2,4 nearly equivalent with parallel sends
- Fully connected (R=N) severely degrades at P≥16 due to partition contention
- p99 latency increases with R but stays under 500us for R≤8

## Results: Fewer Producers, More Partitions per Producer

| P | N | R | Topology | Throughput | p99 (us) |
|---|---|---|----------|------------|----------|
| 4 | 16 | 4 | 4 producers, 4 partitions each | 138k msg/s | 148 |
| 8 | 16 | 2 | 8 producers, 2 partitions each | 136k msg/s | 149 |

**Observation:** With parallel sends, fewer producers with higher R can match throughput of more producers with R=1.

## Comparison: Sequential vs Parallel Sends (R>1)

Before parallel sends (sequential round-robin):

| P | N | R | Throughput | p99 (us) |
|---|---|---|------------|----------|
| 16 | 16 | 2 | 116k msg/s | 177 |
| 16 | 16 | 4 | 112k msg/s | 209 |
| 16 | 16 | 8 | 81k msg/s | 420 |
| 16 | 16 | 16 | 65k msg/s | 447 |

After parallel sends (one TCP connection per partition):

| P | N | R | Throughput | p99 (us) |
|---|---|---|------------|----------|
| 16 | 16 | 2 | 137k msg/s | 244 |
| 16 | 16 | 4 | 133k msg/s | 342 |
| 16 | 16 | 8 | 129k msg/s | 438 |
| 16 | 16 | 16 | 84k msg/s | 169 |

**Improvement:** ~20-60% throughput gain with parallel sends for R>1.

## Key Takeaways

1. **Peak throughput:** ~157k msg/s at P=24, N=24, R=1
2. **Sweet spot:** P=16-32 producers with R=1-2
3. **Single producer limit:** ~19k msg/s (bottleneck is per-message fsync)
4. **Parallel sends essential:** When R>1, each producer needs separate TCP connection per partition
5. **Avoid fully connected:** R=N causes severe contention at scale
6. **Latency:** Send latency p99 < 200us for R≤2, < 500us for R≤8

## Comparison with Native iggy-bench

| Config | Native iggy-bench | Our minimal-bench | Diff |
|--------|-------------------|-------------------|------|
| P=16 N=16 R=1 throughput | 115k msg/s | 138k msg/s | +20% |
| P=24 N=24 R=1 throughput | 153k msg/s | 157k msg/s | +3% |
| P=16 p50 latency | 100us | 77us | -23% |
| P=16 p99 latency | 260us | 151us | -42% |

**Why we're slightly faster:**
1. Explicit `partition_id()` vs `balanced()` (avoids server-side routing)
2. One TCP connection per partition (true parallel sends)
3. Native bench uses high-level `IggyProducer` API with more overhead

**Conclusion:** Our benchmark matches or exceeds native iggy-bench performance. The producer-side implementation is solid.

## Benchmark Tool

Location: `/root/iggy/tools/minimal-bench`

```bash
# Basic usage
./target/release/minimal-bench -P 16 -N 16 -R 1 -m 300000 --producer-only

# With rate limiting
./target/release/minimal-bench -P 16 -N 16 -R 1 -T 100000 -m 300000 --producer-only

# With consumers
./target/release/minimal-bench -P 16 -N 16 -R 1 -C 16 -m 300000
```

Flags:
- `-P` / `--producers`: Number of producers
- `-N` / `--partitions`: Number of partitions
- `-R` / `--redundancy`: Partitions per producer
- `-C` / `--consumers`: Number of consumers
- `-T` / `--throughput`: Target total msg/s (0 = unlimited)
- `-m` / `--messages`: Total messages to send
- `--producer-only`: Skip consumers
