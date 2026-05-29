use anyhow::{bail, Result};
use bytes::Bytes;
use clap::Parser;
use hdrhistogram::Histogram;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use iggy::prelude::*;

const DEFAULT_STREAM_NAME: &str = "minimal-bench";
const TOPIC_NAME: &str = "test";

#[derive(Parser, Debug, Clone)]
#[command(name = "minimal-bench")]
struct Args {
    /// Total messages to send
    #[arg(short = 'm', long, default_value = "100000")]
    messages: u64,

    #[arg(long, default_value = "1")]
    batch_size: u32,

    #[arg(long, default_value = "250")]
    message_size: usize,

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
}

/// Compute which partitions a producer writes to.
/// Uses a rotating assignment to spread producers across partitions evenly.
fn compute_producer_partitions(producer_id: usize, num_producers: usize, num_partitions: u32, redundancy: u32) -> Vec<u32> {
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

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder().with_max_level(Level::INFO).finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();

    // Validate constraints
    if args.redundancy > args.partitions {
        bail!("R ({}) must be <= N ({})", args.redundancy, args.partitions);
    }
    if (args.producers as u32 * args.redundancy) < args.partitions {
        bail!("P * R ({} * {} = {}) must be >= N ({})",
            args.producers, args.redundancy, args.producers as u32 * args.redundancy, args.partitions);
    }

    // Show partition assignments
    info!("=== Minimal Rust Benchmark ===");
    info!("Messages: {}", args.messages);
    info!("Message size: {} bytes", args.message_size);
    info!("Producers (P): {}", args.producers);
    info!("Partitions (N): {}", args.partitions);
    info!("Redundancy (R): {}", args.redundancy);
    info!("Consumers: {}", if args.producer_only { 0 } else { args.consumers });
    info!("Target throughput: {} msg/s", if args.throughput == 0 { "unlimited".to_string() } else { args.throughput.to_string() });
    info!("");

    // Show topology
    info!("Partition assignments:");
    for p in 0..args.producers {
        let parts = compute_producer_partitions(p, args.producers, args.partitions, args.redundancy);
        info!("  Producer {} -> {:?}", p, parts);
    }
    info!("");

    // Setup stream
    let stream_name = Arc::new(args.stream.clone());
    let setup_client = create_client(&args.server).await?;
    let stream_id: Identifier = stream_name.as_str().try_into()?;
    let topic_id: Identifier = TOPIC_NAME.try_into()?;

    let _ = setup_client.delete_stream(&stream_id).await;
    setup_client.create_stream(&stream_name).await?;
    setup_client.create_topic(&stream_id, TOPIC_NAME, args.partitions, CompressionAlgorithm::None, None, IggyExpiry::NeverExpire, MaxTopicSize::Unlimited).await?;

    if !args.producer_only {
        setup_client.create_consumer_group(&stream_id, &topic_id, "bench-group").await?;
    }
    drop(setup_client);

    info!("Stream created with {} partitions", args.partitions);

    let producer_done = Arc::new(AtomicBool::new(false));
    let total_sent = Arc::new(AtomicU64::new(0));
    let total_received = Arc::new(AtomicU64::new(0));
    let latency_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3)?));
    let producer_latency_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3)?));

    let start = Instant::now();

    // Spawn consumers (if not producer-only)
    let mut consumer_handles = Vec::new();
    if !args.producer_only {
        for consumer_id in 0..args.consumers {
            let client = create_client(&args.server).await?;
            let stream_id: Identifier = stream_name.as_str().try_into()?;
            let topic_id: Identifier = TOPIC_NAME.try_into()?;
            let group_id: Identifier = "bench-group".try_into()?;

            client.join_consumer_group(&stream_id, &topic_id, &group_id).await?;

            let producer_done = producer_done.clone();
            let total_received = total_received.clone();
            let latency_hist = latency_hist.clone();
            let batch_size = args.batch_size;
            let stream_name = stream_name.clone();

            consumer_handles.push(tokio::spawn(async move {
                let consumer = Consumer::group(group_id);
                let mut empty_polls = 0;
                // Thread-local histogram to avoid lock contention
                let mut local_hist = Histogram::<u64>::new(3).unwrap();

                while !producer_done.load(Ordering::Relaxed) || empty_polls < 100 {
                    let polled = client.poll_messages(&stream_id, &topic_id, None, &consumer, &PollingStrategy::next(), batch_size, true).await;

                    match polled {
                        Ok(polled) if !polled.messages.is_empty() => {
                            empty_polls = 0;
                            let receive_ts = IggyTimestamp::now().as_micros();
                            for msg in &polled.messages {
                                if msg.payload.len() >= 8 {
                                    let send_ts = u64::from_le_bytes(msg.payload[0..8].try_into().unwrap());
                                    let latency = receive_ts.saturating_sub(send_ts);
                                    local_hist.record(latency).ok();
                                }
                            }
                            total_received.fetch_add(polled.messages.len() as u64, Ordering::Relaxed);
                        }
                        _ => {
                            empty_polls += 1;
                            // No sleep - tight poll loop for lowest latency
                            tokio::task::yield_now().await;
                        }
                    }
                }
                // Merge local histogram into global
                latency_hist.lock().await.add(&local_hist).ok();
                info!("Consumer {} done", consumer_id);
            }));
        }
        info!("{} consumers started", args.consumers);
    }

    // Rate limiter state
    let rate_start = Instant::now();
    let throughput = args.throughput;

    // Spawn producers
    let msgs_per_producer = args.messages / args.producers as u64;
    let mut producer_handles = Vec::new();

    for producer_id in 0..args.producers {
        let my_partitions = compute_producer_partitions(producer_id, args.producers, args.partitions, args.redundancy);
        let num_my_partitions = my_partitions.len();

        // Create one client per partition for parallel sends
        let mut clients = Vec::with_capacity(num_my_partitions);
        for _ in 0..num_my_partitions {
            clients.push(create_client(&args.server).await?);
        }

        let total_sent = total_sent.clone();
        let message_size = args.message_size;
        let rate_start = rate_start.clone();
        let producer_latency_hist = producer_latency_hist.clone();
        let stream_name = stream_name.clone();

        producer_handles.push(tokio::spawn(async move {
            let payload_template: Vec<u8> = vec![0u8; message_size];
            let msgs_per_partition = msgs_per_producer / num_my_partitions as u64;

            // Spawn a task per partition
            let mut partition_handles = Vec::with_capacity(num_my_partitions);
            for (i, client) in clients.into_iter().enumerate() {
                let partition_id = my_partitions[i];
                let total_sent = total_sent.clone();
                let producer_latency_hist = producer_latency_hist.clone();
                let payload_template = payload_template.clone();
                let rate_start = rate_start.clone();
                let stream_name = stream_name.clone();
                let stream_id: Identifier = stream_name.as_str().try_into().unwrap();
                let topic_id: Identifier = TOPIC_NAME.try_into().unwrap();

                partition_handles.push(tokio::spawn(async move {
                    let mut sent = 0u64;
                    while sent < msgs_per_partition {
                        // Rate limiting (global across all producers)
                        if throughput > 0 {
                            let total = total_sent.load(Ordering::Relaxed);
                            let elapsed = rate_start.elapsed().as_secs_f64();
                            let expected_time = total as f64 / throughput as f64;
                            if elapsed < expected_time {
                                sleep(Duration::from_secs_f64(expected_time - elapsed)).await;
                            }
                        }

                        let mut payload = payload_template.clone();
                        let ts = IggyTimestamp::now().as_micros();
                        payload[0..8].copy_from_slice(&ts.to_le_bytes());

                        let mut messages = vec![IggyMessage::builder().payload(Bytes::from(payload)).build().unwrap()];

                        let send_start = Instant::now();
                        if client.send_messages(&stream_id, &topic_id, &Partitioning::partition_id(partition_id), &mut messages).await.is_ok() {
                            let send_latency = send_start.elapsed().as_micros() as u64;
                            producer_latency_hist.lock().await.record(send_latency).ok();
                            sent += 1;
                            total_sent.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    sent
                }));
            }

            let mut total = 0u64;
            for h in partition_handles {
                total += h.await.unwrap_or(0);
            }
            info!("Producer {} done: {} msgs", producer_id, total);
        }));
    }

    // Wait for producers
    for handle in producer_handles {
        handle.await?;
    }
    let producer_elapsed = start.elapsed();
    producer_done.store(true, Ordering::SeqCst);

    let total_produced = total_sent.load(Ordering::Relaxed);
    let prod_hist = producer_latency_hist.lock().await;
    info!("");
    info!("=== Producer Results ===");
    info!("Produced: {} msgs in {:?}", total_produced, producer_elapsed);
    info!("Producer throughput: {:.0} msg/s", total_produced as f64 / producer_elapsed.as_secs_f64());
    if prod_hist.len() > 0 {
        info!("Send latency (us): p50={} p99={} p999={} avg={:.0}",
            prod_hist.value_at_quantile(0.50),
            prod_hist.value_at_quantile(0.99),
            prod_hist.value_at_quantile(0.999),
            prod_hist.mean());
    }
    drop(prod_hist);

    // Wait for consumers
    if !args.producer_only {
        for handle in consumer_handles {
            handle.await?;
        }

        let elapsed = start.elapsed();
        let total_consumed = total_received.load(Ordering::Relaxed);
        let hist = latency_hist.lock().await;

        info!("");
        info!("=== Consumer Results ===");
        info!("Consumed: {} msgs in {:?}", total_consumed, elapsed);
        info!("Consumer throughput: {:.0} msg/s", total_consumed as f64 / elapsed.as_secs_f64());

        if hist.len() > 0 {
            info!("");
            info!("Latency (us) - {} samples:", hist.len());
            info!("  p50:   {}", hist.value_at_quantile(0.50));
            info!("  p90:   {}", hist.value_at_quantile(0.90));
            info!("  p99:   {}", hist.value_at_quantile(0.99));
            info!("  p999:  {}", hist.value_at_quantile(0.999));
            info!("  max:   {}", hist.max());
            info!("  avg:   {:.0}", hist.mean());
        }
    }

    Ok(())
}
