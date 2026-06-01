# TODO: things to try

## OS tuning (no reboot)

These can be applied before starting the server and bench. Goal: reduce
scheduler jitter and improve memory access latency.

```bash
# Hugepages — reduces TLB pressure on the 4GiB memory pool
echo 2048 | sudo tee /proc/sys/vm/nr_hugepages
echo always | sudo tee /sys/kernel/mm/transparent_hugepage/enabled

# Suppress deep C-states — keep fd open for the lifetime of the bench run
sudo bash -c 'printf "\x00\x00\x00\x00" > /dev/cpu_dma_latency; sleep infinity' &

# Scheduler
sudo sysctl -w kernel.sched_autogroup_enabled=0
sudo sysctl -w kernel.sched_rt_runtime_us=-1   # allow RT tasks 100% CPU

# Misc
sudo sysctl -w kernel.nmi_watchdog=0
sudo sysctl -w kernel.numa_balancing=0
sudo sysctl -w vm.swappiness=0
echo defer+madvise | sudo tee /sys/kernel/mm/transparent_hugepage/defrag
```

Notes from first attempt:
- `cpufreq` scaling_governor not exposed by hypervisor on c8a — skip
- `kernel.sched_energy_aware` not supported on this kernel — skip
- `sched_min_granularity_ns` / `sched_wakeup_granularity_ns` not in this kernel — skip

## OS tuning (requires reboot)

Add to kernel boot parameters in `/etc/default/grub`, then `update-grub` and reboot.
This is the gold standard for eliminating scheduler jitter on latency-sensitive cores.

```
isolcpus=8-23 nohz_full=8-23 rcu_nocbs=8-23
```

- `isolcpus` — removes cores 8-23 from the general scheduler pool
- `nohz_full` — disables the periodic timer tick on those cores (tickless)
- `rcu_nocbs` — offloads RCU callbacks off those cores

After reboot, launch bench normally with `taskset -c 8-23` — the isolation
means no kernel interference on those cores at all.

Expected impact: should eliminate or drastically reduce the ~11ms max outliers
which are currently caused by the KVM hypervisor / kernel tick preempting bench threads.

## Multi-process scaling

Run two bench instances on separate stream names and separate core ranges to
see if we can push past the ~125k msg/s single-process ceiling:

```bash
taskset -c 8-15  ./target/release/minimal-bench -P 4 -N 8 -C 8 -R 2 -m 1000000 --poll-batch-size 500 --poll-interval-us 1 --stream s1 &
taskset -c 16-23 ./target/release/minimal-bench -P 4 -N 8 -C 8 -R 2 -m 1000000 --poll-batch-size 500 --poll-interval-us 1 --stream s2 &
```

Expected: ~250k msg/s combined if scaling is linear. Watch per-process p999 —
should stay under 2ms if cores don't overlap.

## Ryzen / NVMe baseline

Run the same benchmark on the local Ryzen machine with NVMe + ext4 to establish
a baseline. Key things to compare:

- p999 on NVMe vs tmpfs (expect slightly higher, but how much?)
- Whether `messages_required_to_save = 500` is still the right threshold on NVMe
  (NVMe write latency is ~100µs vs ~1µs for tmpfs — may need lower value)
- CCD topology on Ryzen vs EPYC — different number of cores per CCD
