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

use hdrhistogram::Histogram;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Per-partition tracking for lag and throughput diagnostics
#[derive(Default)]
pub struct PartitionTracker {
    pub partition_id: u32,
    pub produced_seq: AtomicU64,
    pub consumed_seq: AtomicU64,
    pub produced_count: AtomicU64,
    pub consumed_count: AtomicU64,
    pub produced_bytes: AtomicU64,
    pub consumed_bytes: AtomicU64,
    pub max_lag: AtomicU64,
    pub lag_sum: AtomicU64,
    pub lag_samples: AtomicU64,
}

impl PartitionTracker {
    pub fn new(partition_id: u32) -> Self {
        Self {
            partition_id,
            ..Default::default()
        }
    }

    pub fn record_produce(&self, seq: u64, bytes: u64) {
        self.produced_seq.fetch_max(seq, Ordering::Relaxed);
        self.produced_count.fetch_add(1, Ordering::Relaxed);
        self.produced_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_consume(&self, seq: u64, bytes: u64) {
        self.consumed_seq.fetch_max(seq, Ordering::Relaxed);
        self.consumed_count.fetch_add(1, Ordering::Relaxed);
        self.consumed_bytes.fetch_add(bytes, Ordering::Relaxed);

        let produced = self.produced_seq.load(Ordering::Relaxed);
        let consumed = seq;
        let lag = produced.saturating_sub(consumed);

        self.max_lag.fetch_max(lag, Ordering::Relaxed);
        self.lag_sum.fetch_add(lag, Ordering::Relaxed);
        self.lag_samples.fetch_add(1, Ordering::Relaxed);
    }

    pub fn current_lag(&self) -> u64 {
        let produced = self.produced_seq.load(Ordering::Relaxed);
        let consumed = self.consumed_seq.load(Ordering::Relaxed);
        produced.saturating_sub(consumed)
    }

    pub fn snapshot(&self, elapsed_secs: f64) -> PartitionSnapshot {
        let produced = self.produced_count.load(Ordering::Relaxed);
        let consumed = self.consumed_count.load(Ordering::Relaxed);
        let lag_samples = self.lag_samples.load(Ordering::Relaxed);
        let avg_lag = if lag_samples > 0 {
            self.lag_sum.load(Ordering::Relaxed) as f64 / lag_samples as f64
        } else {
            0.0
        };

        PartitionSnapshot {
            partition_id: self.partition_id,
            produced_count: produced,
            consumed_count: consumed,
            produced_bytes: self.produced_bytes.load(Ordering::Relaxed),
            consumed_bytes: self.consumed_bytes.load(Ordering::Relaxed),
            produced_per_sec: if elapsed_secs > 0.0 {
                produced as f64 / elapsed_secs
            } else {
                0.0
            },
            consumed_per_sec: if elapsed_secs > 0.0 {
                consumed as f64 / elapsed_secs
            } else {
                0.0
            },
            current_lag: self.current_lag(),
            max_lag: self.max_lag.load(Ordering::Relaxed),
            avg_lag,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PartitionSnapshot {
    pub partition_id: u32,
    pub produced_count: u64,
    pub consumed_count: u64,
    pub produced_bytes: u64,
    pub consumed_bytes: u64,
    pub produced_per_sec: f64,
    pub consumed_per_sec: f64,
    pub current_lag: u64,
    pub max_lag: u64,
    pub avg_lag: f64,
}

/// Consumer poll behavior tracking (thread-local, merged at end)
pub struct ConsumerPollStats {
    pub consumer_id: usize,
    pub poll_count: u64,
    pub empty_poll_count: u64,
    pub total_messages: u64,
    pub poll_duration_hist: Histogram<u64>,
    pub messages_per_poll_hist: Histogram<u64>,
    pub time_between_polls_hist: Histogram<u64>,
    last_poll_end: Option<Instant>,
}

impl ConsumerPollStats {
    pub fn new(consumer_id: usize) -> Self {
        Self {
            consumer_id,
            poll_count: 0,
            empty_poll_count: 0,
            total_messages: 0,
            poll_duration_hist: Histogram::new(3).unwrap(),
            messages_per_poll_hist: Histogram::new(3).unwrap(),
            time_between_polls_hist: Histogram::new(3).unwrap(),
            last_poll_end: None,
        }
    }

    pub fn record_poll(&mut self, duration_us: u64, message_count: u64) {
        if let Some(last_end) = self.last_poll_end {
            let between = last_end.elapsed().as_micros() as u64;
            self.time_between_polls_hist.record(between).ok();
        }

        self.poll_count += 1;
        self.poll_duration_hist.record(duration_us).ok();

        if message_count == 0 {
            self.empty_poll_count += 1;
        } else {
            self.messages_per_poll_hist.record(message_count).ok();
            self.total_messages += message_count;
        }

        self.last_poll_end = Some(Instant::now());
    }

    pub fn snapshot(&self, elapsed_secs: f64) -> ConsumerPollSnapshot {
        ConsumerPollSnapshot {
            consumer_id: self.consumer_id,
            polls_per_sec: if elapsed_secs > 0.0 {
                self.poll_count as f64 / elapsed_secs
            } else {
                0.0
            },
            empty_polls_per_sec: if elapsed_secs > 0.0 {
                self.empty_poll_count as f64 / elapsed_secs
            } else {
                0.0
            },
            empty_poll_ratio: if self.poll_count > 0 {
                self.empty_poll_count as f64 / self.poll_count as f64
            } else {
                0.0
            },
            poll_duration_p50_us: self.poll_duration_hist.value_at_quantile(0.50),
            poll_duration_p99_us: self.poll_duration_hist.value_at_quantile(0.99),
            poll_duration_p999_us: self.poll_duration_hist.value_at_quantile(0.999),
            messages_per_poll_avg: if self.poll_count > self.empty_poll_count {
                self.total_messages as f64 / (self.poll_count - self.empty_poll_count) as f64
            } else {
                0.0
            },
            messages_per_poll_p50: self.messages_per_poll_hist.value_at_quantile(0.50),
            messages_per_poll_p99: self.messages_per_poll_hist.value_at_quantile(0.99),
            messages_per_poll_p999: self.messages_per_poll_hist.value_at_quantile(0.999),
            time_between_polls_p50_us: self.time_between_polls_hist.value_at_quantile(0.50),
            time_between_polls_p99_us: self.time_between_polls_hist.value_at_quantile(0.99),
            time_between_polls_p999_us: self.time_between_polls_hist.value_at_quantile(0.999),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ConsumerPollSnapshot {
    pub consumer_id: usize,
    pub polls_per_sec: f64,
    pub empty_polls_per_sec: f64,
    pub empty_poll_ratio: f64,
    pub poll_duration_p50_us: u64,
    pub poll_duration_p99_us: u64,
    pub poll_duration_p999_us: u64,
    pub messages_per_poll_avg: f64,
    pub messages_per_poll_p50: u64,
    pub messages_per_poll_p99: u64,
    pub messages_per_poll_p999: u64,
    pub time_between_polls_p50_us: u64,
    pub time_between_polls_p99_us: u64,
    pub time_between_polls_p999_us: u64,
}

/// Producer send behavior tracking (thread-local)
pub struct ProducerSendStats {
    pub producer_id: usize,
    pub send_count: u64,
    pub total_messages: u64,
    pub total_bytes: u64,
    pub send_duration_hist: Histogram<u64>,
    pub messages_per_send_hist: Histogram<u64>,
}

impl ProducerSendStats {
    pub fn new(producer_id: usize) -> Self {
        Self {
            producer_id,
            send_count: 0,
            total_messages: 0,
            total_bytes: 0,
            send_duration_hist: Histogram::new(3).unwrap(),
            messages_per_send_hist: Histogram::new(3).unwrap(),
        }
    }

    pub fn record_send(&mut self, duration_us: u64, message_count: u64, bytes: u64) {
        self.send_count += 1;
        self.total_messages += message_count;
        self.total_bytes += bytes;
        self.send_duration_hist.record(duration_us).ok();
        self.messages_per_send_hist.record(message_count).ok();
    }

    pub fn snapshot(&self, elapsed_secs: f64) -> ProducerSendSnapshot {
        ProducerSendSnapshot {
            producer_id: self.producer_id,
            sends_per_sec: if elapsed_secs > 0.0 {
                self.send_count as f64 / elapsed_secs
            } else {
                0.0
            },
            send_duration_p50_us: self.send_duration_hist.value_at_quantile(0.50),
            send_duration_p99_us: self.send_duration_hist.value_at_quantile(0.99),
            send_duration_p999_us: self.send_duration_hist.value_at_quantile(0.999),
            messages_per_send_avg: if self.send_count > 0 {
                self.total_messages as f64 / self.send_count as f64
            } else {
                0.0
            },
            messages_per_send_p50: self.messages_per_send_hist.value_at_quantile(0.50),
            bytes_per_send_avg: if self.send_count > 0 {
                self.total_bytes as f64 / self.send_count as f64
            } else {
                0.0
            },
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProducerSendSnapshot {
    pub producer_id: usize,
    pub sends_per_sec: f64,
    pub send_duration_p50_us: u64,
    pub send_duration_p99_us: u64,
    pub send_duration_p999_us: u64,
    pub messages_per_send_avg: f64,
    pub messages_per_send_p50: u64,
    pub bytes_per_send_avg: f64,
}

/// Diagnostic observations derived from metrics
#[derive(Debug, Clone, Serialize)]
pub struct DiagnosticObservation {
    pub category: String,
    pub severity: Severity,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[allow(dead_code)]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

/// Full diagnostic report
#[derive(Debug, Clone, Serialize)]
pub struct DiagnosticReport {
    pub config: BenchmarkConfig,
    pub overall: OverallMetrics,
    pub partitions: Vec<PartitionSnapshot>,
    pub consumer_polls: Vec<ConsumerPollSnapshot>,
    pub producer_sends: Vec<ProducerSendSnapshot>,
    pub observations: Vec<DiagnosticObservation>,
    pub bottleneck_summary: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkConfig {
    pub producers: usize,
    pub consumers: usize,
    pub partitions: u32,
    pub redundancy: u32,
    pub messages: u64,
    pub message_size: usize,
    pub batch_size: u32,
    pub poll_interval_us: u64,
    pub balanced: bool,
    pub producer_only: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct OverallMetrics {
    pub elapsed_secs: f64,
    pub producer_throughput: f64,
    pub consumer_throughput: f64,
    pub producer_p50_us: u64,
    pub producer_p99_us: u64,
    pub consumer_p50_us: u64,
    pub consumer_p99_us: u64,
    pub consumer_p999_us: u64,
}

impl DiagnosticReport {
    pub fn analyze(
        config: BenchmarkConfig,
        overall: OverallMetrics,
        partitions: Vec<PartitionSnapshot>,
        consumer_polls: Vec<ConsumerPollSnapshot>,
        producer_sends: Vec<ProducerSendSnapshot>,
    ) -> Self {
        let mut observations = Vec::new();

        // A. Is latency caused by backlog?
        Self::analyze_backlog(&partitions, &overall, &mut observations);

        // B. Are producers or consumers imbalanced?
        Self::analyze_imbalance(
            &partitions,
            &consumer_polls,
            &producer_sends,
            &mut observations,
        );

        // C. Are consumers polling efficiently?
        Self::analyze_poll_efficiency(&consumer_polls, &partitions, &mut observations);

        // D. Is producer sending efficiently?
        Self::analyze_send_efficiency(&producer_sends, &config, &mut observations);

        let bottleneck_summary = Self::summarize_bottleneck(&observations, &partitions, &overall);

        Self {
            config,
            overall,
            partitions,
            consumer_polls,
            producer_sends,
            observations,
            bottleneck_summary,
        }
    }

    fn analyze_backlog(
        partitions: &[PartitionSnapshot],
        overall: &OverallMetrics,
        observations: &mut Vec<DiagnosticObservation>,
    ) {
        let total_lag: u64 = partitions.iter().map(|p| p.current_lag).sum();
        let max_lag = partitions.iter().map(|p| p.max_lag).max().unwrap_or(0);
        let max_lag_partition = partitions.iter().max_by_key(|p| p.max_lag);

        if max_lag > 1000 {
            observations.push(DiagnosticObservation {
                category: "backlog".to_string(),
                severity: Severity::Warning,
                message: format!(
                    "High max lag: {} msgs (partition {}). Latency likely includes queue wait time.",
                    max_lag,
                    max_lag_partition.map(|p| p.partition_id).unwrap_or(0)
                ),
            });
        }

        if total_lag > 100 && overall.consumer_p99_us > 1000 {
            let avg_consumer_rate: f64 = partitions.iter().map(|p| p.consumed_per_sec).sum::<f64>()
                / partitions.len().max(1) as f64;
            if avg_consumer_rate > 0.0 {
                let expected_queue_wait_us = (total_lag as f64 / avg_consumer_rate) * 1_000_000.0;
                if expected_queue_wait_us > overall.consumer_p50_us as f64 * 0.5 {
                    observations.push(DiagnosticObservation {
                        category: "backlog".to_string(),
                        severity: Severity::Info,
                        message: format!(
                            "Queue wait estimate: {:.0}us (lag={} / rate={:.0}/s). \
                             This explains ~{:.0}% of p50 latency.",
                            expected_queue_wait_us,
                            total_lag,
                            avg_consumer_rate,
                            (expected_queue_wait_us / overall.consumer_p50_us as f64) * 100.0
                        ),
                    });
                }
            }
        }

        // Check for growing lag (production > consumption)
        for p in partitions {
            if p.produced_per_sec > p.consumed_per_sec * 1.1 && p.produced_count > 100 {
                observations.push(DiagnosticObservation {
                    category: "backlog".to_string(),
                    severity: Severity::Warning,
                    message: format!(
                        "Partition {} falling behind: produce={:.0}/s, consume={:.0}/s",
                        p.partition_id, p.produced_per_sec, p.consumed_per_sec
                    ),
                });
            }
        }
    }

    fn analyze_imbalance(
        partitions: &[PartitionSnapshot],
        consumer_polls: &[ConsumerPollSnapshot],
        producer_sends: &[ProducerSendSnapshot],
        observations: &mut Vec<DiagnosticObservation>,
    ) {
        if partitions.len() > 1 {
            let rates: Vec<f64> = partitions.iter().map(|p| p.produced_per_sec).collect();
            let max_rate = rates.iter().cloned().fold(0.0_f64, f64::max);
            let min_rate = rates.iter().cloned().fold(f64::MAX, f64::min);
            if max_rate > 0.0 && min_rate < max_rate * 0.5 {
                observations.push(DiagnosticObservation {
                    category: "imbalance".to_string(),
                    severity: Severity::Warning,
                    message: format!(
                        "Partition imbalance: min={:.0}/s, max={:.0}/s (ratio {:.1}x)",
                        min_rate,
                        max_rate,
                        max_rate / min_rate.max(1.0)
                    ),
                });
            }
        }

        if consumer_polls.len() > 1 {
            let msg_counts: Vec<f64> = consumer_polls
                .iter()
                .map(|c| c.messages_per_poll_avg * c.polls_per_sec)
                .collect();
            let max_c = msg_counts.iter().cloned().fold(0.0_f64, f64::max);
            let min_c = msg_counts.iter().cloned().fold(f64::MAX, f64::min);
            if max_c > 0.0 && min_c < max_c * 0.3 {
                observations.push(DiagnosticObservation {
                    category: "imbalance".to_string(),
                    severity: Severity::Warning,
                    message: format!(
                        "Consumer imbalance: some consumers processing {:.1}x more than others",
                        max_c / min_c.max(1.0)
                    ),
                });
            }
        }

        if producer_sends.len() > 1 {
            let send_rates: Vec<f64> = producer_sends.iter().map(|p| p.sends_per_sec).collect();
            let max_p = send_rates.iter().cloned().fold(0.0_f64, f64::max);
            let min_p = send_rates.iter().cloned().fold(f64::MAX, f64::min);
            if max_p > 0.0 && min_p < max_p * 0.5 {
                observations.push(DiagnosticObservation {
                    category: "imbalance".to_string(),
                    severity: Severity::Info,
                    message: format!(
                        "Producer rate variance: min={:.0}/s, max={:.0}/s",
                        min_p, max_p
                    ),
                });
            }
        }
    }

    fn analyze_poll_efficiency(
        consumer_polls: &[ConsumerPollSnapshot],
        partitions: &[PartitionSnapshot],
        observations: &mut Vec<DiagnosticObservation>,
    ) {
        let total_lag: u64 = partitions.iter().map(|p| p.current_lag).sum();

        for poll in consumer_polls {
            if poll.empty_poll_ratio > 0.5 && poll.polls_per_sec > 100.0 {
                observations.push(DiagnosticObservation {
                    category: "polling".to_string(),
                    severity: Severity::Warning,
                    message: format!(
                        "Consumer {} has {:.0}% empty polls at {:.0} polls/s. Consider increasing poll_interval_us.",
                        poll.consumer_id,
                        poll.empty_poll_ratio * 100.0,
                        poll.polls_per_sec
                    ),
                });
            }

            if total_lag > 100
                && poll.messages_per_poll_avg < 5.0
                && poll.messages_per_poll_p99 < 10
            {
                observations.push(DiagnosticObservation {
                    category: "polling".to_string(),
                    severity: Severity::Info,
                    message: format!(
                        "Consumer {} getting small batches ({:.1} avg) despite lag={}. May indicate partition assignment issues.",
                        poll.consumer_id, poll.messages_per_poll_avg, total_lag
                    ),
                });
            }

            if poll.poll_duration_p99_us > 10_000 {
                observations.push(DiagnosticObservation {
                    category: "polling".to_string(),
                    severity: Severity::Warning,
                    message: format!(
                        "Consumer {} poll p99 latency is {}us. Network or server delay.",
                        poll.consumer_id, poll.poll_duration_p99_us
                    ),
                });
            }
        }
    }

    fn analyze_send_efficiency(
        producer_sends: &[ProducerSendSnapshot],
        config: &BenchmarkConfig,
        observations: &mut Vec<DiagnosticObservation>,
    ) {
        for send in producer_sends {
            if config.batch_size > 1 && send.messages_per_send_avg < 1.5 {
                observations.push(DiagnosticObservation {
                    category: "batching".to_string(),
                    severity: Severity::Info,
                    message: format!(
                        "Producer {} batch_size={} but avg msgs/send={:.1}. Batching not utilized.",
                        send.producer_id, config.batch_size, send.messages_per_send_avg
                    ),
                });
            }

            if send.send_duration_p99_us > 5_000 {
                observations.push(DiagnosticObservation {
                    category: "send".to_string(),
                    severity: Severity::Warning,
                    message: format!(
                        "Producer {} send p99={}us. Network or server pressure.",
                        send.producer_id, send.send_duration_p99_us
                    ),
                });
            }
        }
    }

    fn summarize_bottleneck(
        observations: &[DiagnosticObservation],
        partitions: &[PartitionSnapshot],
        overall: &OverallMetrics,
    ) -> String {
        let has_backlog = observations
            .iter()
            .any(|o| o.category == "backlog" && o.severity == Severity::Warning);
        let has_imbalance = observations
            .iter()
            .any(|o| o.category == "imbalance" && o.severity == Severity::Warning);
        let has_poll_issues = observations
            .iter()
            .any(|o| o.category == "polling" && o.severity == Severity::Warning);

        let total_lag: u64 = partitions.iter().map(|p| p.current_lag).sum();
        let high_latency = overall.consumer_p99_us > 1000;

        if has_backlog && high_latency {
            "FIFO queue residence time (backlog). Messages wait in partition queues before consumption.".to_string()
        } else if has_imbalance {
            "Partition or consumer imbalance. Work not evenly distributed.".to_string()
        } else if has_poll_issues {
            "Consumer polling inefficiency. High empty poll rate or slow poll responses."
                .to_string()
        } else if high_latency && total_lag < 10 {
            "Broker/client/network latency. Low lag suggests not a backlog issue.".to_string()
        } else {
            "No clear bottleneck detected. System operating within normal parameters.".to_string()
        }
    }

    pub fn print_human_readable(&self) {
        use tracing::warn;

        warn!("");
        warn!("=== Diagnostic Report ===");
        warn!("");

        // Per-partition lag and throughput
        if !self.partitions.is_empty() {
            warn!("--- Per-Partition Metrics ---");
            warn!(
                "{:>4} {:>10} {:>10} {:>10} {:>10} {:>8} {:>8}",
                "Part", "Produced", "Consumed", "Prod/s", "Cons/s", "Lag", "MaxLag"
            );
            for p in &self.partitions {
                warn!(
                    "{:>4} {:>10} {:>10} {:>10.0} {:>10.0} {:>8} {:>8}",
                    p.partition_id,
                    p.produced_count,
                    p.consumed_count,
                    p.produced_per_sec,
                    p.consumed_per_sec,
                    p.current_lag,
                    p.max_lag
                );
            }
            warn!("");
        }

        // Consumer poll behavior
        if !self.consumer_polls.is_empty() {
            warn!("--- Consumer Poll Behavior ---");
            warn!(
                "{:>4} {:>10} {:>10} {:>8} {:>10} {:>10} {:>10} {:>10}",
                "ID", "Polls/s", "Empty/s", "Empty%", "Dur p50", "Dur p99", "Dur p999", "Msgs/poll"
            );
            for c in &self.consumer_polls {
                warn!(
                    "{:>4} {:>10.0} {:>10.0} {:>7.1}% {:>9}us {:>9}us {:>9}us {:>10.1}",
                    c.consumer_id,
                    c.polls_per_sec,
                    c.empty_polls_per_sec,
                    c.empty_poll_ratio * 100.0,
                    c.poll_duration_p50_us,
                    c.poll_duration_p99_us,
                    c.poll_duration_p999_us,
                    c.messages_per_poll_avg
                );
            }
            warn!("");
        }

        // Producer send behavior
        if !self.producer_sends.is_empty() {
            warn!("--- Producer Send Behavior ---");
            warn!(
                "{:>4} {:>10} {:>10} {:>10} {:>10} {:>12}",
                "ID", "Sends/s", "Dur p50", "Dur p99", "Dur p999", "Msgs/send"
            );
            for p in &self.producer_sends {
                warn!(
                    "{:>4} {:>10.0} {:>9}us {:>9}us {:>9}us {:>12.1}",
                    p.producer_id,
                    p.sends_per_sec,
                    p.send_duration_p50_us,
                    p.send_duration_p99_us,
                    p.send_duration_p999_us,
                    p.messages_per_send_avg
                );
            }
            warn!("");
        }

        // Observations
        if !self.observations.is_empty() {
            warn!("--- Diagnostic Observations ---");
            for obs in &self.observations {
                let severity_str = match obs.severity {
                    Severity::Info => "INFO",
                    Severity::Warning => "WARN",
                    Severity::Critical => "CRIT",
                };
                warn!("[{}] {}: {}", severity_str, obs.category, obs.message);
            }
            warn!("");
        }

        warn!("--- Bottleneck Summary ---");
        warn!("{}", self.bottleneck_summary);
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_string())
    }
}

/// Message payload structure for diagnostic tracking
#[derive(Clone, Copy)]
#[repr(C, packed)]
pub struct DiagnosticPayload {
    pub timestamp_us: u64,
    pub producer_id: u32,
    pub partition_id: u32,
    pub sequence: u64,
}

impl DiagnosticPayload {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    pub fn new(producer_id: u32, partition_id: u32, sequence: u64) -> Self {
        Self {
            timestamp_us: iggy::prelude::IggyTimestamp::now().as_micros(),
            producer_id,
            partition_id,
            sequence,
        }
    }

    pub fn to_bytes(self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.timestamp_us.to_le_bytes());
        buf[8..12].copy_from_slice(&self.producer_id.to_le_bytes());
        buf[12..16].copy_from_slice(&self.partition_id.to_le_bytes());
        buf[16..24].copy_from_slice(&self.sequence.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }
        Some(Self {
            timestamp_us: u64::from_le_bytes(buf[0..8].try_into().ok()?),
            producer_id: u32::from_le_bytes(buf[8..12].try_into().ok()?),
            partition_id: u32::from_le_bytes(buf[12..16].try_into().ok()?),
            sequence: u64::from_le_bytes(buf[16..24].try_into().ok()?),
        })
    }
}

/// Global partition tracker registry
pub struct PartitionTrackers {
    trackers: HashMap<u32, PartitionTracker>,
}

impl PartitionTrackers {
    pub fn new(num_partitions: u32) -> Self {
        let mut trackers = HashMap::new();
        for i in 0..num_partitions {
            trackers.insert(i, PartitionTracker::new(i));
        }
        Self { trackers }
    }

    pub fn get(&self, partition_id: u32) -> Option<&PartitionTracker> {
        self.trackers.get(&partition_id)
    }

    pub fn snapshots(&self, elapsed_secs: f64) -> Vec<PartitionSnapshot> {
        let mut snapshots: Vec<_> = self
            .trackers
            .values()
            .map(|t| t.snapshot(elapsed_secs))
            .collect();
        snapshots.sort_by_key(|s| s.partition_id);
        snapshots
    }
}
