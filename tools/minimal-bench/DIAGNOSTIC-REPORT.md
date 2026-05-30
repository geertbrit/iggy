# Diagnostic Report: Iggy Multi-Process Throughput Plateau

## 1. Summary

Multi-process benchmark scaling shows a clear plateau:
- 1 process: 42-44k msg/s
- 2 processes: 62-68k msg/s (1.5x, not 2x)
- 3 processes: 48-62k msg/s (regression in some tests)

The bottleneck is **client-side poll overhead**, not server saturation.

## 2. Environment

- **CPU**: AMD Ryzen 9 5950X 16-Core (32 threads, 2 CCDs, 2x32MB L3)
- **Iggy server**: 32 shard threads (shard-0 to shard-31), each pinned to a core
- **Storage**: NVMe + ext4, fsync=off
- **Transport**: TCP with nodelay

## 3. Key Findings

### 3.1 Server Is NOT Saturated

During 3-process runs, server shard CPU usage:
```
shard-2:  1.4% avg
shard-27: 1.4% avg
shard-7:  1.3% avg
shard-5:  1.2% avg
... (all shards < 2%)
```

**Server is barely loaded.** The bottleneck is not server-side.

### 3.2 Client CPU Is The Constraint

Per benchmark process during 3-process run:
```
Process CPU: 133-153%
```

Each process uses ~1.5 cores. With 3 processes, that's ~4.5 cores for benchmarks + async runtime overhead. The benchmark is CPU-bound on the client side.

### 3.3 Msgs/Poll Reveals The Problem

**Single process (optimal):**
```
Consumer 0: Polls/s=11698, Empty%=10.1%, Msgs/poll=1.0
Consumer 1: Polls/s=11818, Empty%=11.1%, Msgs/poll=1.0
```
Low empty poll rate, consumers keeping up.

**3 processes (degraded):**
```
Consumer 0: Polls/s=12360, Empty%=69.9%, Msgs/poll=1.2
Consumer 1: Polls/s=12459, Empty%=68.2%, Msgs/poll=1.2
```
**70% empty polls!** Consumers are starving despite high poll rate.

### 3.4 TCP Connection Count

Each benchmark process creates ~8 TCP connections:
- 1 setup/admin connection
- 4 producer connections (one per partition)
- 4 consumer connections (one per partition, shared with consumer group)

With 3 processes: 24 connections total.

### 3.5 Producer-Only vs Full Benchmark

| Mode | Throughput |
|------|------------|
| Producer-only (P=8) | 98k msg/s |
| With consumers (P=4 C=4) | 43k msg/s |

Consumer polling path is the bottleneck, not producer writes.

## 4. Root Cause Analysis

### Primary Bottleneck: Client Poll Loop Inefficiency

The benchmark's consumer poll loop is:
```rust
loop {
    poll_start = Instant::now();
    polled = client.poll_messages(...);
    poll_duration = poll_start.elapsed();
    
    if polled.messages.is_empty() {
        // Empty poll - either yield or sleep
        if poll_interval_us > 0 {
            sleep(poll_interval_us);
        } else {
            yield_now();  // Still burns CPU
        }
    } else {
        // Process messages
    }
}
```

With `poll_interval_us=0`, empty polls still consume CPU via `yield_now()`. Under multi-process load:
1. Each process polls aggressively
2. Empty poll rate increases (contention for messages)
3. CPU burned on empty polls instead of processing
4. Throughput degrades despite server having capacity

### Secondary Factor: Poll Response Overhead

Each poll requires:
1. Client: serialize request, send over TCP
2. Server: deserialize, lookup partition, read messages, serialize response
3. Client: receive, deserialize, process

With `poll-batch-size=100` but only 1-2 messages returned per poll, the per-message overhead is high.

### Not A Factor: Server Shard Contention

- All 32 shards show < 2% CPU
- Separate streams map to different partitions
- No cross-shard forwarding observed
- io_uring workers also minimal CPU

## 5. Experimental Evidence

### 5.1 Scaling with Process Count

| Processes | Combined | Per-Process | Empty Poll % |
|-----------|----------|-------------|--------------|
| 1 | 44k | 44k | ~10% |
| 2 | 68k | 35k each | ~40-50% |
| 3 | 55k | 18k each | ~65-70% |

### 5.2 Poll Batch Size Effect

With `poll-batch-size=1`:
- p99 latency: 452ms (queue buildup)

With `poll-batch-size=100`:
- p99 latency: 298us (drain-available works)
- But msgs/poll still ~1.0 (no backlog to drain)

### 5.3 Partition Lag

All partitions show:
```
Produced=Consumed, Lag=0, MaxLag=1-200
```

No backlog accumulation. Consumer polling is keeping up but at high CPU cost.

## 6. Bottleneck Ranking

1. **Client poll loop CPU overhead** (HIGH CONFIDENCE)
   - Evidence: 150% CPU per process, 70% empty polls
   - Fix: Add smarter backoff, adaptive polling, or server push

2. **Per-poll network round-trip** (MEDIUM CONFIDENCE)
   - Evidence: 10k polls/s per consumer, ~85us per poll
   - Fix: Batch more aggressively, reduce poll frequency

3. **Async runtime scheduling** (MEDIUM CONFIDENCE)
   - Evidence: Many tasks per process competing for CPU
   - Fix: Pin tasks, reduce task count, use thread-per-partition

4. **Server shard contention** (LOW - RULED OUT)
   - Evidence: All shards < 2% CPU

5. **Storage/page-cache** (LOW - NOT TESTED)
   - Would need tmpfs comparison

## 7. Recommendations

### 7.1 Benchmark Improvements

1. **Adaptive poll interval**: Increase poll delay when empty polls are frequent
   ```rust
   if empty_polls > threshold {
       poll_interval_us *= 2;  // Exponential backoff
   } else {
       poll_interval_us = base_interval;
   }
   ```

2. **Reduce task count**: One consumer per partition is 4 tasks. Consider consolidating.

3. **Add poll interval in multi-process mode**: 
   ```bash
   # Instead of poll-interval-us=0
   --poll-interval-us 50  # Reduces CPU, allows batching
   ```

### 7.2 Configuration for Multi-Process

For 2+ processes, use:
```bash
--poll-interval-us 50 --poll-batch-size 100
```

This trades slight latency increase for better throughput scaling.

### 7.3 Iggy Server Enhancements (Future)

1. **Long polling**: Server waits up to N ms for messages before returning empty
2. **Push-based delivery**: Server pushes to consumers instead of pull
3. **Batch coalescing**: Server accumulates messages before responding

## 8. Conclusion

The 68k msg/s plateau at 2 processes is **client-side poll overhead**, not server capacity. The server handles the load with < 2% CPU per shard.

For production:
- Distribute clients across machines (network isolation)
- Use `poll-interval-us > 0` to reduce empty poll CPU burn
- Consider larger poll batches if latency tolerance allows

The current ~70k msg/s consumed ceiling is a benchmark artifact, not an Iggy server limit. Producer-only tests show 98-157k msg/s, indicating server write capacity is much higher than observed consumed throughput.
