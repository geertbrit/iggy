# minimal-bench

Minimal benchmark for Iggy throughput and latency testing.

## Quick Start

```bash
# 1. Start the server (from repo root)
ulimit -n 1048576 && IGGY_CONFIG_PATH=config.toml ./target/release/iggy-server --with-default-root-credentials

# 2. Run the benchmark
./target/release/minimal-bench -P 8 -N 8 -C 8 -R 1 -m 100000 --poll-batch-size 100 --poll-interval-us 10
```

## Server Setup

The benchmark uses hardcoded credentials `iggy`/`iggy`. Start the server with:

```bash
ulimit -n 1048576 && IGGY_CONFIG_PATH=config.toml ./target/release/iggy-server --with-default-root-credentials
```

- `ulimit -n 1048576` - raises file descriptor limit (avoids "too many open files" errors)
- `IGGY_CONFIG_PATH=config.toml` - low-latency config at repo root (see below)
- `--with-default-root-credentials` - creates user `iggy` with password `iggy`

Without `--with-default-root-credentials`, the server generates a random password and the benchmark fails with `invalid_credentials`.

### Low-Latency Config

The repo includes `config.toml` at the root, tuned for benchmarking:

- TCP nodelay enabled, larger socket buffers (256KB)
- fsync disabled (state, partition, message_saver)
- Higher batch thresholds before disk write (10k msgs / 10MB)
- Index caching = all
- QUIC/WebSocket disabled
- Logging = warn, no file output
- Data path = `data/iggy`

For production, re-enable fsync and adjust paths as needed.

## Common Options

```bash
-m, --messages <N>          Total messages to send (default: 100000)
-P, --producers <N>         Number of producers (default: 1)
-N, --partitions <N>        Number of partitions (default: 1)
-R, --redundancy <N>        Partitions per producer (default: 1)
-C, --consumers <N>         Number of consumers (default: 1)
--poll-interval-us <N>      Microseconds between polls on empty (default: 0 = tight loop)
--poll-batch-size <N>       Max messages per poll (default: 1)
--producer-only             Skip consumers, measure producer throughput only
--manual-commit             Disable auto-commit, manually commit offset after each batch
--diagnostics               Show detailed stats (lag, poll behavior, etc.)
-v, --verbose               Per-producer and per-consumer stats
```

## Multi-Process Testing

For multi-process tests, use different `--stream` names:

```bash
# Terminal 1
./target/release/minimal-bench -m 100000 -P 1 -N 32 -R 32 -C 32 --stream s1

# Terminal 2
./target/release/minimal-bench -m 100000 -P 1 -N 32 -R 32 -C 32 --stream s2

# Terminal 3
./target/release/minimal-bench -m 100000 -P 1 -N 32 -R 32 -C 32 --stream s3
```

## Reducing CPU Burn on Consumers

If consumers are CPU-bound from empty polls, use `--poll-interval-us`:

```bash
./target/release/minimal-bench --poll-interval-us 50  # 50us delay on empty poll
```

## Findings

### poll-batch-size

The `--poll-batch-size` parameter has "up to N" semantics - the server returns up to N messages per poll. Setting this to 100 eliminates ~500x latency overhead vs batch=1:

| poll-batch-size | Latency p50 | Throughput |
|-----------------|-------------|------------|
| 1               | ~100ms      | ~1k msg/s  |
| 10-100          | ~200µs      | ~60k msg/s |

For any serious benchmarking, use `--poll-batch-size 100` or higher.

### CPU Pinning (AMD Ryzen 5950X)

Tested CCD affinity (server + bench on same L3 cache) vs cross-CCD (different L3 caches):

| Config | Producer Throughput | Latency avg |
|--------|---------------------|-------------|
| No pinning (baseline) | 54,977 msg/s | 239 µs |
| CCD-Affinity (same CCD) | 60,725 msg/s | 232 µs |
| CCD-Cross (diff CCD) | 49,907 msg/s | 451 µs |

**Conclusion:** CCD-affinity wins but the delta is modest (~10% throughput improvement). Cross-CCD is noticeably worse due to inter-CCD latency. For this workload, CPU pinning provides marginal gains. However, other servers or workloads may show larger deltas, so CPU pinning should be part of any serious optimization effort.

To pin server and benchmark to same CCD:

```bash
# Server on cores 0-7 (CCD0)
taskset -c 0-7 ./target/release/iggy-server --with-default-root-credentials

# Benchmark on same cores
taskset -c 0-7 ./target/release/minimal-bench -P 8 -N 8 -C 8 -R 1 -m 100000 --poll-batch-size 100
```
