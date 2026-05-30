# minimal-bench

Minimal benchmark for Iggy throughput and latency testing.

## Quick Start

```bash
# 1. Start the server (from repo root)
IGGY_CONFIG_PATH=config.toml ./target/release/iggy-server --with-default-root-credentials

# 2. Run the benchmark
./target/release/minimal-bench -P 8 -N 8 -C 8 -R 1 -m 100000 --poll-batch-size 100
```

## Server Setup

The benchmark uses hardcoded credentials `iggy`/`iggy`. Start the server with:

```bash
IGGY_CONFIG_PATH=config.toml ./target/release/iggy-server --with-default-root-credentials
```

- `IGGY_CONFIG_PATH=config.toml` - uses repo config (NVMe paths, TCP settings)
- `--with-default-root-credentials` - creates user `iggy` with password `iggy`

Without `--with-default-root-credentials`, the server generates a random password and the benchmark fails with `invalid_credentials`.

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
