# minimal-bench

Minimal benchmark for Iggy throughput and latency testing.

## Environment

AWS c8a.8xlarge (eu-west-1), AMD EPYC 9R45, 32 cores, 64 GB RAM.
CPU topology: 4 L3 domains (CCDs) of 8 cores — L3[0]=0-7, L3[1]=8-15, L3[2]=16-23, L3[3]=24-31.
Storage: 16 GB tmpfs at `/mnt/iggy-tmpfs`.

## Results

~125k msg/s, p999 consistently under 1.1ms. Validated across 8 runs with 1M messages each:

| Run | Throughput | p50 | p99 | p999 |
|-----|-----------|-----|-----|------|
| 1 | 124k msg/s | 485µs | 995µs | 1033µs |
| 2 | 127k msg/s | 501µs | 997µs | 1035µs |
| 3 | 125k msg/s | 474µs | 995µs | 1040µs |
| 4 | 125k msg/s | 492µs | 996µs | 1059µs |
| 5 | 125k msg/s | 487µs | 997µs | 1042µs |
| 6 | 127k msg/s | 473µs | 993µs | 1057µs |
| 7 | 123k msg/s | 480µs | 997µs | 1101µs |
| 8 | 124k msg/s | 495µs | 998µs | 1047µs |

The max outlier (~11ms) is the KVM hypervisor preempting a vCPU — not tunable in software.

## Prerequisites

```bash
# memlock limit for io_uring with many shards
echo "* soft memlock unlimited" | sudo tee -a /etc/security/limits.conf
echo "* hard memlock unlimited" | sudo tee -a /etc/security/limits.conf

# tmpfs mount (add to /etc/fstab for persistence)
sudo mkdir -p /mnt/iggy-tmpfs
sudo mount -t tmpfs -o size=16g tmpfs /mnt/iggy-tmpfs
```

## Build

```bash
cd ~/iggy
source ~/.cargo/env
cargo build --release --bin iggy-server --bin minimal-bench
```

## Server config

`config.toml` at the repo root. Key settings vs defaults:

| Setting | Default | Bench value | Why |
|---------|---------|-------------|-----|
| `system.path` | `local_data` | `/mnt/iggy-tmpfs` | tmpfs = zero I/O latency |
| `system.partition.messages_required_to_save` | 1024 | **500** | prevents periodic flush stall |
| `system.partition.size_of_messages_required_to_save` | 1 MiB | **256 KiB** | caps flush burst size |
| `system.segment.cache_indexes` | `open_segment` | `all` | keeps indexes hot |
| `message_saver.enforce_fsync` | `true` | `false` | no durability needed |
| `system.partition.enforce_fsync` | `false` | `false` | unchanged |
| `system.state.enforce_fsync` | `false` | `false` | unchanged |
| `tcp.socket.nodelay` | `false` | `true` | disable Nagle |
| `tcp.socket.{recv,send}_buffer_size` | 100 KB | 256 KB | larger socket buffers |
| `system.sharding.cpu_allocation` | `all` | `"0..8"` | pin shards to L3[0] |
| `quic.enabled` | `true` | `false` | reduce overhead |
| `websocket.enabled` | `true` | `false` | reduce overhead |
| `system.logging.level` | `info` | `warn` | less noise |
| `system.logging.file_enabled` | `true` | `false` | no log files |

## Start server

```bash
# Clear data (fresh run)
sudo rm -rf /mnt/iggy-tmpfs/*

# Start pinned to L3[0] (cores 0-7)
taskset -c 0-7 env IGGY_CONFIG_PATH=~/iggy/config.toml \
  ./target/release/iggy-server --with-default-root-credentials \
  > /tmp/iggy-server.log 2>&1 &
```

## Run benchmark

```bash
taskset -c 8-23 ./target/release/minimal-bench \
  -P 4 -N 8 -C 8 -R 2 -m 1000000 \
  --poll-batch-size 500 --poll-interval-us 1
```

## Key findings

### flush threshold is the p999 lever

`messages_required_to_save` is the single most impactful setting. Large values cause
periodic multi-MB write bursts that stall io_uring shards — consumers poll into the
stall, messages accumulate, p999 spikes. Smaller flushes also improve throughput by
removing backpressure on producers.

| `messages_required_to_save` | p999 | Throughput |
|-----------------------------|------|-----------|
| 10000 | ~8.7ms | ~48k msg/s |
| 1024 (default) | ~5ms | ~80k msg/s |
| **500** | **~1.05ms** | **~125k msg/s** |

### CPU pinning

Pinning the server and bench to separate CCDs prevents them competing for cores.
Without separation the bench lands on the server's cores and p999 degrades to 1–11ms.

| Config | Throughput | p999 |
|--------|-----------|------|
| Both unpinned | ~220k msg/s | 1–6ms (noisy) |
| Server pinned 0-7, bench free | ~215k msg/s | 1–11ms (worst) |
| **Server 0-7, bench 8-23** | **~125k msg/s** | **~1.05ms (stable)** |

Pinning costs ~35% throughput — the bench tokio runtime is restricted to 16 cores
instead of 32. Worth it if p999 stability matters.

### poll-batch-size and poll-interval-us

Use `--poll-batch-size 500` and `--poll-interval-us 1`.

Iggy uses drain-available semantics: the server returns immediately with whatever
messages are available (never waits to fill the batch). A large batch size removes
the need to round-trip per message during catch-up. In steady state, msgs/poll is
typically 2-5 regardless of batch size setting.

Tight loop (`--poll-interval-us 0`) burns all consumer CPU on empty polls, starves
the tokio runtime, and degrades both throughput and latency (p999 ~18ms, throughput
drops to 29k msg/s).

### server is not the bottleneck

At 125k msg/s, server shards run at < 5% CPU each. Producer-only runs reach ~220k
msg/s. The bench client is the ceiling.

### what does not help

- RT scheduling (`chrt -f 50`) — no effect. The ~11ms max is the KVM hypervisor;
  SCHED_FIFO cannot prevent hypervisor preemption.
- Rate limiting producers — worsens p50/p99 without improving p999.
- `TOKIO_WORKER_THREADS` tuning — no measurable effect.

## CLI flags

| Flag | Description |
|------|-------------|
| `-P` | Number of producers |
| `-N` | Number of partitions |
| `-C` | Number of consumers |
| `-R` | Redundancy — partitions per producer |
| `-m` | Total messages to send |
| `-T` | Target throughput msg/s (0 = unlimited) |
| `--poll-batch-size` | Max messages per consumer poll |
| `--poll-interval-us` | Sleep after empty poll in µs |
| `--producer-only` | Skip consumers, measure send throughput only |
| `--balanced` | Server-side routing instead of pinned partitions |
| `--diagnostics` | Per-partition lag, poll behavior, bottleneck summary |
| `--diagnostics-json` | Write diagnostics to JSON file |
| `-v` | Per-actor verbose stats |
| `--stream` | Stream name (use distinct names for multi-process runs) |

## Multi-process testing

```bash
taskset -c 8-15  ./target/release/minimal-bench -P 4 -N 8 -C 8 -R 2 -m 1000000 --poll-batch-size 500 --poll-interval-us 1 --stream s1 &
taskset -c 16-23 ./target/release/minimal-bench -P 4 -N 8 -C 8 -R 2 -m 1000000 --poll-batch-size 500 --poll-interval-us 1 --stream s2 &
```
