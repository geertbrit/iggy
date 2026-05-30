/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

mod diagnostics;

use anyhow::{bail, Result};
use bytes::Bytes;
use clap::Parser;
use diagnostics::{
    BenchmarkConfig, ConsumerPollSnapshot, ConsumerPollStats, DiagnosticPayload, DiagnosticReport,
    OverallMetrics, PartitionSnapshot, PartitionTrackers, ProducerSendSnapshot, ProducerSendStats,
};
use hdrhistogram::Histogram;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{warn, Level};
use tracing_subscriber::FmtSubscriber;

use iggy::prelude::*;

const DEFAULT_STREAM_NAME: &str = "minimal-bench";
const TOPIC_NAME: &str = "test";

#[derive(Parser, Debug, Clone)]
#[command(name = "minimal-bench", about = "Minimal Iggy benchmark with diagnostics")]
struct Args {
    /// Total messages to send
    #[arg(short = 'm', long, default_value = "100000")]
    messages: u64,

    /// Messages per send call (producer batching)
    #[arg(long, default_value = "1")]
    batch_size: u32,

    /// Message payload size in bytes
    #[arg(long, default_value = "250")]
    message_size: usize,

    /// Iggy server address
    #[arg(long, default_value = "127.0.0.1:8090")]
    server: String,

    /// Number of producers (P)
    #[arg(short = 'P', long, default_value = "1")]
    producers: usize,

    /// Number of partitions (N)
    #[arg(short = 'N', long, default_value = "1")]
    partitions: u32,

    /// Redundancy: partitions per producer (R). Constraints: R <= N, P * R >= N
    #[arg(short = 'R', long, default_value = "1")]
    redundancy: u32,

    /// Number of consumers
    #[arg(short = 'C', long, default_value = "1")]
    consumers: usize,

    /// Target total throughput msg/s (0 = unlimited)
    #[arg(short = 'T', long, default_value = "0")]
    throughput: u64,

    /// Producer only mode - no consumers
    #[arg(long)]
    producer_only: bool,

    /// Stream name (for multi-stream testing)
    #[arg(long, default_value = DEFAULT_STREAM_NAME)]
    stream: String,

    /// Show per-producer and per-consumer stats
    #[arg(short = 'v', long)]
    verbose: bool,

    /// Use balanced partitioning (server routes) instead of explicit partition_id
    #[arg(long)]
    balanced: bool,

    /// Consumer poll interval in microseconds on empty poll (0 = tight loop)
    #[arg(long, default_value = "0")]
    poll_interval_us: u64,

    /// Enable detailed diagnostics (per-partition lag, poll behavior, etc.)
    #[arg(long)]
    diagnostics: bool,

    /// Output diagnostics JSON to file
    #[arg(long)]
    diagnostics_json: Option<PathBuf>,
}

/// Per-actor stats returned from producer/consumer tasks
struct ProducerActorStats {
    id: usize,
    messages: u64,
    elapsed: Duration,
    histogram: Histogram<u64>,
    partitions_seen: Vec<u32>,
    send_stats: ProducerSendStats,
}

struct ConsumerActorStats {
    id: usize,
    messages: u64,
    elapsed: Duration,
    histogram: Histogram<u64>,
    partitions_seen: Vec<u32>,
    poll_stats: ConsumerPollStats,
}

/// Compute which partitions a producer writes to.
fn compute_producer_partitions(
    producer_id: usize,
    _num_producers: usize,
    num_partitions: u32,
    redundancy: u32,
) -> Vec<u32> {
    let mut partitions = Vec::with_capacity(redundancy as usize);
    let start = (producer_id * redundancy as usize) % num_partitions as usize;
    for i in 0..redundancy as usize {
        partitions.push(((start + i) % num_partitions as usize) as u32);
    }
    partitions
}

async fn create_client(server: &str) -> Result<IggyClient> {
    let config = TcpClientConfig {
        server_address: server.to_string(),
        nodelay: true,
        ..TcpClientConfig::default()
    };
    let tcp_client = TcpClient::create(Arc::new(config))?;
    Client::connect(&tcp_client).await?;
    let client = IggyClient::create(ClientWrapper::Tcp(tcp_client), None, None);
    client.login_user("iggy", "iggy").await?;
    Ok(client)
}

fn print_histogram(hist: &Histogram<u64>, label: &str) {
    if !hist.is_empty() {
        warn!(
            "{} (us): p50={} p99={} p999={} avg={:.0}",
            label,
            hist.value_at_quantile(0.50),
            hist.value_at_quantile(0.99),
            hist.value_at_quantile(0.999),
            hist.mean()
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::WARN)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();

    // Validate constraints (only in non-balanced mode)
    if !args.balanced {
        if args.redundancy > args.partitions {
            bail!("R ({}) must be <= N ({})", args.redundancy, args.partitions);
        }
        if (args.producers as u32 * args.redundancy) < args.partitions {
            bail!(
                "P * R ({} * {} = {}) must be >= N ({})",
                args.producers,
                args.redundancy,
                args.producers as u32 * args.redundancy,
                args.partitions
            );
        }
    }

    // Show configuration
    warn!("=== Minimal Rust Benchmark ===");
    warn!("Messages: {}", args.messages);
    warn!("Message size: {} bytes", args.message_size);
    warn!("Producers (P): {}", args.producers);
    warn!("Partitions (N): {}", args.partitions);
    warn!("Redundancy (R): {}", args.redundancy);
    warn!(
        "Consumers: {}",
        if args.producer_only {
            0
        } else {
            args.consumers
        }
    );
    warn!(
        "Target throughput: {} msg/s",
        if args.throughput == 0 {
            "unlimited".to_string()
        } else {
            args.throughput.to_string()
        }
    );
    warn!("Poll interval: {}us", args.poll_interval_us);
    warn!("Verbose: {}", args.verbose);
    warn!("Balanced partitioning: {}", args.balanced);
    warn!(
        "Diagnostics: {}",
        args.diagnostics || args.diagnostics_json.is_some()
    );
    warn!("");

    // Show topology
    if args.balanced {
        warn!(
            "Partition assignments: balanced (server routes to all {} partitions)",
            args.partitions
        );
    } else {
        warn!("Partition assignments:");
        for p in 0..args.producers {
            let parts =
                compute_producer_partitions(p, args.producers, args.partitions, args.redundancy);
            warn!("  Producer {} -> {:?}", p, parts);
        }
    }
    warn!("");

    // Setup stream
    let stream_name = Arc::new(args.stream.clone());
    let setup_client = create_client(&args.server).await?;
    let stream_id: Identifier = stream_name.as_str().try_into()?;
    let topic_id: Identifier = TOPIC_NAME.try_into()?;

    let _ = setup_client.delete_stream(&stream_id).await;
    setup_client.create_stream(&stream_name).await?;
    setup_client
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            args.partitions,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::Unlimited,
        )
        .await?;

    if !args.producer_only {
        setup_client
            .create_consumer_group(&stream_id, &topic_id, "bench-group")
            .await?;
    }
    drop(setup_client);

    warn!("Stream created with {} partitions", args.partitions);

    let producer_done = Arc::new(AtomicBool::new(false));
    let total_sent = Arc::new(AtomicU64::new(0));
    let total_received = Arc::new(AtomicU64::new(0));
    let global_consumer_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3)?));
    let global_producer_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3)?));

    // Diagnostics: per-partition trackers
    let enable_diagnostics = args.diagnostics || args.diagnostics_json.is_some();
    let partition_trackers = Arc::new(PartitionTrackers::new(args.partitions));

    let start = Instant::now();

    // Spawn consumers (if not producer-only)
    let mut consumer_handles: Vec<tokio::task::JoinHandle<ConsumerActorStats>> = Vec::new();
    if !args.producer_only {
        for consumer_id in 0..args.consumers {
            let client = create_client(&args.server).await?;
            let stream_id: Identifier = stream_name.as_str().try_into()?;
            let topic_id: Identifier = TOPIC_NAME.try_into()?;
            let group_id: Identifier = "bench-group".try_into()?;

            client
                .join_consumer_group(&stream_id, &topic_id, &group_id)
                .await?;

            let producer_done = producer_done.clone();
            let total_received = total_received.clone();
            let global_hist = global_consumer_hist.clone();
            let batch_size = args.batch_size;
            let poll_interval_us = args.poll_interval_us;
            let partition_trackers = partition_trackers.clone();
            let enable_diag = enable_diagnostics;
            let message_size = args.message_size;

            consumer_handles.push(tokio::spawn(async move {
                let consumer = Consumer::group(group_id);
                let mut empty_polls = 0;
                let mut local_hist = Histogram::<u64>::new(3).unwrap();
                let mut messages_received = 0u64;
                let mut partitions_seen: Vec<u32> = Vec::new();
                let consumer_start = Instant::now();
                let mut poll_stats = ConsumerPollStats::new(consumer_id);

                while !producer_done.load(Ordering::Relaxed) || empty_polls < 100 {
                    let poll_start = Instant::now();
                    let polled = client
                        .poll_messages(
                            &stream_id,
                            &topic_id,
                            None,
                            &consumer,
                            &PollingStrategy::next(),
                            batch_size,
                            true,
                        )
                        .await;
                    let poll_duration_us = poll_start.elapsed().as_micros() as u64;

                    match polled {
                        Ok(polled) if !polled.messages.is_empty() => {
                            empty_polls = 0;
                            let receive_ts = IggyTimestamp::now().as_micros();
                            let msg_count = polled.messages.len() as u64;

                            if enable_diag {
                                poll_stats.record_poll(poll_duration_us, msg_count);
                            }

                            let partition_id = polled.partition_id;
                            if !partitions_seen.contains(&partition_id) {
                                partitions_seen.push(partition_id);
                            }

                            for msg in &polled.messages {
                                let send_ts = msg.header.origin_timestamp;
                                if send_ts > 0 {
                                    let latency = receive_ts.saturating_sub(send_ts);
                                    local_hist.record(latency).ok();
                                } else if msg.payload.len() >= 8 {
                                    let send_ts =
                                        u64::from_le_bytes(msg.payload[0..8].try_into().unwrap());
                                    let latency = receive_ts.saturating_sub(send_ts);
                                    local_hist.record(latency).ok();
                                }

                                // Diagnostics: track per-partition consumption
                                if enable_diag {
                                    if let Some(dp) = DiagnosticPayload::from_bytes(&msg.payload) {
                                        if let Some(tracker) =
                                            partition_trackers.get(dp.partition_id)
                                        {
                                            tracker
                                                .record_consume(dp.sequence, message_size as u64);
                                        }
                                    }
                                }
                            }
                            messages_received += msg_count;
                            total_received.fetch_add(msg_count, Ordering::Relaxed);
                        }
                        _ => {
                            empty_polls += 1;
                            if enable_diag {
                                poll_stats.record_poll(poll_duration_us, 0);
                            }
                            if poll_interval_us > 0 {
                                sleep(Duration::from_micros(poll_interval_us)).await;
                            } else {
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                }

                let elapsed = consumer_start.elapsed();
                global_hist.lock().await.add(&local_hist).ok();
                partitions_seen.sort();

                ConsumerActorStats {
                    id: consumer_id,
                    messages: messages_received,
                    elapsed,
                    histogram: local_hist,
                    partitions_seen,
                    poll_stats,
                }
            }));
        }
        warn!("{} consumers started", args.consumers);
    }

    // Rate limiter state
    let rate_start = Instant::now();
    let throughput = args.throughput;

    // Spawn producers
    let msgs_per_producer = args.messages / args.producers as u64;
    let mut producer_handles: Vec<tokio::task::JoinHandle<ProducerActorStats>> = Vec::new();

    for producer_id in 0..args.producers {
        let my_partitions = compute_producer_partitions(
            producer_id,
            args.producers,
            args.partitions,
            args.redundancy,
        );
        let num_my_partitions = if args.balanced {
            1
        } else {
            my_partitions.len()
        };

        let mut clients = Vec::with_capacity(num_my_partitions);
        for _ in 0..num_my_partitions {
            clients.push(create_client(&args.server).await?);
        }

        let total_sent = total_sent.clone();
        let message_size = args.message_size;
        let global_hist = global_producer_hist.clone();
        let stream_name = stream_name.clone();
        let my_partitions_clone = my_partitions.clone();
        let use_balanced = args.balanced;
        let partition_trackers = partition_trackers.clone();
        let enable_diag = enable_diagnostics;

        producer_handles.push(tokio::spawn(async move {
            let payload_size = message_size.max(DiagnosticPayload::SIZE);
            let mut payload_template: Vec<u8> = vec![0u8; payload_size];
            let mut local_hist = Histogram::<u64>::new(3).unwrap();
            let producer_start = Instant::now();
            let mut send_stats = ProducerSendStats::new(producer_id);

            let stream_id: Identifier = stream_name.as_str().try_into().unwrap();
            let topic_id: Identifier = TOPIC_NAME.try_into().unwrap();
            let mut sent = 0u64;

            // Per-partition sequence counters
            let mut partition_seqs: Vec<u64> = vec![0; my_partitions_clone.len()];

            while sent < msgs_per_producer {
                let partition_idx = (sent as usize) % num_my_partitions;
                let partition_id = my_partitions_clone[partition_idx];
                let client = &clients[partition_idx];

                let partitioning = if use_balanced {
                    Partitioning::balanced()
                } else {
                    Partitioning::partition_id(partition_id)
                };

                // Rate limiting
                if throughput > 0 {
                    let total_global = total_sent.load(Ordering::Relaxed);
                    let elapsed = rate_start.elapsed().as_secs_f64();
                    let expected_time = total_global as f64 / throughput as f64;
                    if elapsed < expected_time {
                        sleep(Duration::from_secs_f64(expected_time - elapsed)).await;
                    }
                }

                // Build payload with diagnostic info
                let seq = partition_seqs[partition_idx];
                partition_seqs[partition_idx] += 1;

                let diag_payload = DiagnosticPayload::new(producer_id as u32, partition_id, seq);
                let diag_bytes = diag_payload.to_bytes();
                payload_template[..DiagnosticPayload::SIZE].copy_from_slice(&diag_bytes);

                let mut messages = vec![IggyMessage::builder()
                    .payload(Bytes::from(payload_template.clone()))
                    .build()
                    .unwrap()];

                let send_start = Instant::now();
                if client
                    .send_messages(&stream_id, &topic_id, &partitioning, &mut messages)
                    .await
                    .is_ok()
                {
                    let send_latency = send_start.elapsed().as_micros() as u64;
                    local_hist.record(send_latency).ok();

                    if enable_diag {
                        send_stats.record_send(send_latency, 1, payload_size as u64);
                        if let Some(tracker) = partition_trackers.get(partition_id) {
                            tracker.record_produce(seq, payload_size as u64);
                        }
                    }

                    sent += 1;
                    total_sent.fetch_add(1, Ordering::Relaxed);
                }
            }

            let elapsed = producer_start.elapsed();
            global_hist.lock().await.add(&local_hist).ok();

            ProducerActorStats {
                id: producer_id,
                messages: sent,
                elapsed,
                histogram: local_hist,
                partitions_seen: my_partitions_clone,
                send_stats,
            }
        }));
    }

    // Wait for producers and collect stats
    let mut producer_stats: Vec<ProducerActorStats> = Vec::new();
    for handle in producer_handles {
        if let Ok(stats) = handle.await {
            producer_stats.push(stats);
        }
    }
    let producer_elapsed = start.elapsed();
    producer_done.store(true, Ordering::SeqCst);

    let total_produced = total_sent.load(Ordering::Relaxed);
    let prod_hist = global_producer_hist.lock().await;

    warn!("");
    warn!("=== Producer Results ===");
    warn!(
        "Produced: {} msgs in {:?}",
        total_produced, producer_elapsed
    );
    warn!(
        "Producer throughput: {:.0} msg/s",
        total_produced as f64 / producer_elapsed.as_secs_f64()
    );
    print_histogram(&prod_hist, "Send latency");

    let producer_p50 = prod_hist.value_at_quantile(0.50);
    let producer_p99 = prod_hist.value_at_quantile(0.99);
    drop(prod_hist);

    // Per-producer stats
    if args.verbose && !producer_stats.is_empty() {
        warn!("");
        warn!("--- Per-Producer Stats ---");
        for stats in &producer_stats {
            let throughput = stats.messages as f64 / stats.elapsed.as_secs_f64();
            warn!(
                "  Producer {}: {} msgs, {:.0} msg/s, partitions {:?}",
                stats.id, stats.messages, throughput, stats.partitions_seen
            );
            if !stats.histogram.is_empty() {
                warn!(
                    "    latency (us): p50={} p99={} p999={}",
                    stats.histogram.value_at_quantile(0.50),
                    stats.histogram.value_at_quantile(0.99),
                    stats.histogram.value_at_quantile(0.999)
                );
            }
        }
    }

    // Wait for consumers and collect stats
    let mut consumer_stats: Vec<ConsumerActorStats> = Vec::new();
    let mut consumer_p50 = 0u64;
    let mut consumer_p99 = 0u64;
    let mut consumer_p999 = 0u64;
    let mut consumer_throughput = 0.0f64;

    if !args.producer_only {
        for handle in consumer_handles {
            if let Ok(stats) = handle.await {
                consumer_stats.push(stats);
            }
        }

        let elapsed = start.elapsed();
        let total_consumed = total_received.load(Ordering::Relaxed);
        let hist = global_consumer_hist.lock().await;

        warn!("");
        warn!("=== Consumer Results ===");
        warn!("Consumed: {} msgs in {:?}", total_consumed, elapsed);
        consumer_throughput = total_consumed as f64 / elapsed.as_secs_f64();
        warn!("Consumer throughput: {:.0} msg/s", consumer_throughput);

        if !hist.is_empty() {
            consumer_p50 = hist.value_at_quantile(0.50);
            consumer_p99 = hist.value_at_quantile(0.99);
            consumer_p999 = hist.value_at_quantile(0.999);

            warn!("");
            warn!("Latency (us) - {} samples:", hist.len());
            warn!("  p50:   {}", consumer_p50);
            warn!("  p90:   {}", hist.value_at_quantile(0.90));
            warn!("  p99:   {}", consumer_p99);
            warn!("  p999:  {}", consumer_p999);
            warn!("  max:   {}", hist.max());
            warn!("  avg:   {:.0}", hist.mean());
        }
        drop(hist);

        // Per-consumer stats
        if args.verbose && !consumer_stats.is_empty() {
            warn!("");
            warn!("--- Per-Consumer Stats ---");

            let mut idle_consumers = 0;
            for stats in &consumer_stats {
                let throughput = if stats.elapsed.as_secs_f64() > 0.0 {
                    stats.messages as f64 / stats.elapsed.as_secs_f64()
                } else {
                    0.0
                };

                if stats.messages == 0 {
                    idle_consumers += 1;
                    warn!(
                        "  Consumer {}: IDLE - 0 msgs, partitions seen: {:?}",
                        stats.id, stats.partitions_seen
                    );
                } else {
                    warn!(
                        "  Consumer {}: {} msgs, {:.0} msg/s, partitions {:?}",
                        stats.id, stats.messages, throughput, stats.partitions_seen
                    );
                    if !stats.histogram.is_empty() {
                        warn!(
                            "    latency (us): p50={} p99={} p999={}",
                            stats.histogram.value_at_quantile(0.50),
                            stats.histogram.value_at_quantile(0.99),
                            stats.histogram.value_at_quantile(0.999)
                        );
                    }
                }
            }

            if idle_consumers > 0 {
                warn!("");
                warn!(
                    "WARNING: {} of {} consumers were idle!",
                    idle_consumers,
                    consumer_stats.len()
                );
                warn!(
                    "This suggests consumer group partition assignment is not distributing load."
                );
            }
        }
    }

    // Diagnostics report
    if enable_diagnostics {
        let elapsed_secs = start.elapsed().as_secs_f64();

        let config = BenchmarkConfig {
            producers: args.producers,
            consumers: args.consumers,
            partitions: args.partitions,
            redundancy: args.redundancy,
            messages: args.messages,
            message_size: args.message_size,
            batch_size: args.batch_size,
            poll_interval_us: args.poll_interval_us,
            balanced: args.balanced,
            producer_only: args.producer_only,
        };

        let overall = OverallMetrics {
            elapsed_secs,
            producer_throughput: total_produced as f64 / producer_elapsed.as_secs_f64(),
            consumer_throughput,
            producer_p50_us: producer_p50,
            producer_p99_us: producer_p99,
            consumer_p50_us: consumer_p50,
            consumer_p99_us: consumer_p99,
            consumer_p999_us: consumer_p999,
        };

        let partition_snapshots: Vec<PartitionSnapshot> =
            partition_trackers.snapshots(elapsed_secs);

        let consumer_poll_snapshots: Vec<ConsumerPollSnapshot> = consumer_stats
            .iter()
            .map(|s| s.poll_stats.snapshot(s.elapsed.as_secs_f64()))
            .collect();

        let producer_send_snapshots: Vec<ProducerSendSnapshot> = producer_stats
            .iter()
            .map(|s| s.send_stats.snapshot(s.elapsed.as_secs_f64()))
            .collect();

        let report = DiagnosticReport::analyze(
            config,
            overall,
            partition_snapshots,
            consumer_poll_snapshots,
            producer_send_snapshots,
        );

        report.print_human_readable();

        if let Some(json_path) = &args.diagnostics_json {
            let json = report.to_json();
            std::fs::write(json_path, json)?;
            warn!("Diagnostics JSON written to: {}", json_path.display());
        }
    }

    Ok(())
}
