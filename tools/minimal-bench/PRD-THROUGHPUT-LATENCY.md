# PRD: Iggy Throughput & Latency Optimization

## Goal
Achieve 50k+ msg/s with sub-1ms p99 end-to-end latency through the Iggy streaming layer, ultimately for the Node relay use case.

## Current State
- **Producer peak**: ~157k msg/s at P=24 N=24 R=1, p99=152us send latency
- **Native bench bpcg**: ~120k msg/s aggregate, producer p99=0.25ms, consumer p99=0.35-0.70ms
- **Node relay current**: 27k msg/s, p99=5.6ms (target: 50k msg/s, p99<1ms)

---

## Phase 1: Producer Throughput Ceiling Investigation

### 1a. Server Resource Analysis
**Question**: What's the bottleneck at 150k msg/s? CPU, disk I/O, network, locks?

**Steps**:
- [ ] Run producer benchmark at P=24 N=24 R=1 for 60s
- [ ] Monitor with `htop`, `iostat -x 1`, `pidstat -d 1`
- [ ] Check iggy server CPU utilization per core
- [ ] Check disk write throughput and queue depth
- [ ] Check network utilization

**Expected output**: Identify which resource hits ceiling first

### 1b. In-Memory Only Mode
**Question**: How much does fsync/storage cost us?

**Steps**:
- [ ] Run server with `IGGY_SYSTEM_PARTITION_ENFORCE_FSYNC=false`
- [ ] Run server with `IGGY_SYSTEM_PARTITION_MESSAGES_REQUIRED_TO_SAVE=1000` (batch fsync)
- [ ] Compare throughput and latency vs baseline
- [ ] Document the fsync tax

**Expected output**: Quantify storage overhead (likely 2-5x throughput difference)

### 1c. Multi-Process / Multi-Stream Scaling
**Question**: Can multiple processes with separate streams exceed 150k/s aggregate?

**Hypothesis**: If 150k/s is a single-stream bottleneck (partition locks, fsync serialization), multiple streams should scale linearly.

**Steps**:
- [ ] Create test script that spawns N minimal-bench processes
- [ ] Each process uses its own stream (stream-0, stream-1, etc.)
- [ ] Test N=2,4,8 processes, each doing P=8 N=8 R=1
- [ ] Measure aggregate throughput across all processes
- [ ] Compare latency per process vs single-process baseline

**Test configs**:
```bash
# Single process baseline
./minimal-bench -P 24 -N 24 -R 1 -m 500000 --producer-only

# Multi-process (run in parallel)
./minimal-bench -P 8 -N 8 -R 1 -m 200000 --producer-only --stream stream-0 &
./minimal-bench -P 8 -N 8 -R 1 -m 200000 --producer-only --stream stream-1 &
./minimal-bench -P 8 -N 8 -R 1 -m 200000 --producer-only --stream stream-2 &
./minimal-bench -P 8 -N 8 -R 1 -m 200000 --producer-only --stream stream-3 &
wait
```

**Expected output**: 
- If aggregate > 200k/s: bottleneck is per-stream, horizontal scaling works
- If aggregate ≈ 150k/s: bottleneck is server-wide (CPU/disk/network)

### Results (Completed)

**1a: Server Resources at Peak Load**
- CPU: 14% user, 25% system, **57% iowait** - disk I/O is bottleneck
- iggy-server: ~970% CPU (10 cores)
- Disk: 188 writes/s, 752 KB/s on md2

**TODO:** Move iggy data directory to NVMe drives (nvme0n1/nvme1n1 currently idle). Should eliminate iowait bottleneck and potentially 2-5x throughput.

**1c: Multi-Stream Scaling**

| Config | Per-stream | Aggregate |
|--------|------------|-----------|
| 1 stream, P=8 | 102k msg/s | 102k msg/s |
| 2 streams, P=12 each | 90k msg/s | **181k msg/s** |
| 4 streams, P=8 each | 55k msg/s | **220k msg/s** |
| 8 streams, P=4 each | 27k msg/s | **216k msg/s** |

**Conclusion:** Multiple streams break the single-stream ceiling! ~220k msg/s achievable with 4+ streams vs ~150k single-stream. Each stream has independent partition locks.

---

## Phase 2: Consumer Throughput & Latency Optimization

### 2a. Baseline Consumer Analysis
**Question**: What's our current consumer throughput and where's the bottleneck?

**Steps**:
- [ ] Run full benchmark with rate-limited producers (50k msg/s) and consumers
- [ ] Measure consumer throughput and end-to-end latency
- [ ] Identify if consumers keep up or queue builds

### 2b. Native Bench Consumer Code Review
**Question**: What optimizations does native iggy-bench use that we're missing?

**Steps**:
- [ ] Read `/root/iggy/core/bench/src/actors/consumer/client/high_level.rs`
- [ ] Read `/root/iggy/core/bench/src/actors/consumer/client/low_level.rs`
- [ ] Document key differences:
  - Polling strategy (next vs offset)
  - Batch size
  - Auto-commit behavior
  - Connection handling
  - Async patterns

### 2c. Polling Optimization
**Current issue**: 1ms sleep on empty poll is too slow

**Steps**:
- [ ] Test with no sleep (busy poll)
- [ ] Test with shorter sleep (100us, 500us)
- [ ] Test with adaptive backoff
- [ ] Measure latency impact of each approach

### 2d. Batch Size Tuning
**Question**: What's the optimal poll batch size for latency vs throughput?

**Steps**:
- [ ] Test batch sizes: 1, 10, 100, 1000
- [ ] Measure throughput and latency for each
- [ ] Find sweet spot where latency < 1ms and throughput > 50k

### 2e. Consumer Parallelism
**Question**: How many consumers needed to match producer rate?

**Steps**:
- [ ] Fix producer rate at 50k msg/s (P=8, T=50000)
- [ ] Test C=4, 8, 16, 32 consumers
- [ ] Find minimum C where consumers keep up (queue doesn't grow)
- [ ] Measure latency at that config

### 2f. Consumer Code Optimizations
**Potential improvements to test**:
- [ ] One TCP connection per consumer (current) vs connection pooling
- [ ] Histogram lock contention (use thread-local histograms, merge at end)
- [ ] Pre-allocated payload buffers
- [ ] Separate polling tasks from processing tasks

---

## Phase 3: End-to-End Validation

### 3a. Target Config Validation
**Goal**: 50k msg/s, p99 < 1ms end-to-end

**Steps**:
- [ ] Determine optimal config from Phase 1 & 2
- [ ] Run 5-minute sustained test
- [ ] Verify metrics:
  - Producer throughput ≥ 50k msg/s
  - Consumer throughput ≥ 50k msg/s (no queue growth)
  - End-to-end p99 < 1ms
  - p999 < 5ms

### 3b. Stress Test
**Steps**:
- [ ] Run at 75k msg/s (150% target)
- [ ] Verify graceful degradation
- [ ] Document backpressure behavior

---

## Implementation Tasks

### Immediate (minimal-bench changes needed)
1. [ ] Add `--stream` flag to allow custom stream name
2. [ ] Add multi-process test script
3. [ ] Fix consumer polling (remove/reduce 1ms sleep)
4. [ ] Add configurable poll batch size for consumers
5. [ ] Add thread-local histograms to reduce lock contention

### Code locations
- Producer: `/root/iggy/tools/minimal-bench/src/main.rs` lines 197-240
- Consumer: `/root/iggy/tools/minimal-bench/src/main.rs` lines 141-184
- Native consumer: `/root/iggy/core/bench/src/actors/consumer/`

---

## Success Criteria

| Metric | Current | Target |
|--------|---------|--------|
| Producer throughput | 157k msg/s | ≥150k msg/s |
| Consumer throughput | ~70k msg/s | ≥100k msg/s |
| Producer p99 | 152us | <500us |
| Consumer p99 | ~2ms | <500us |
| End-to-end p99 | ~2ms | <1ms |
| End-to-end p999 | ~6ms | <5ms |

---

## Next Steps (Priority Order)

1. **1a**: Check server resources at peak - understand current bottleneck
2. **1b**: Test no-fsync mode - quantify storage overhead  
3. **2b**: Review native consumer code - find what we're missing
4. **2c**: Fix polling sleep - likely biggest latency win
5. **1c**: Multi-stream test - understand scaling model
6. **2d-2f**: Remaining consumer optimizations
7. **3a-3b**: Validation and stress testing
