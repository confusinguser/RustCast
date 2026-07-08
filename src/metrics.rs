//! Live telemetry. Client devices measure their own buffers and sample flow into
//! lock-free [`DeviceMetrics`] and push periodic [`TelemetryReport`]s to the
//! server; the server records its own send path in [`ServerMetrics`] and keeps a
//! short per-source history in a [`TelemetryStore`] that the web UI graphs.
//!
//! Counters are cumulative since process start; gauges are the instantaneous
//! value at snapshot time. Recording is always lock-free so it never stalls the
//! audio threads.

use std::collections::{HashMap, VecDeque};
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use serde::Serialize;

use crate::wire::{TelemetryReport, now_epoch_ms};

/// Retained history per source: ~60 s at the client's 10 Hz report rate.
const HISTORY_LEN: usize = 600;
/// Forget a client's history if it stops reporting for this long (matches the
/// client-registry staleness window).
const STALE_SECS: f64 = 60.0;

// ---------------------------------------------------------------------------
// Client side
// ---------------------------------------------------------------------------

/// Client-side metrics, written from the playback loop and the network receiver
/// (different threads) and read by the telemetry sender thread.
#[derive(Debug, Default)]
pub struct DeviceMetrics {
    blocks_appended: AtomicU64,
    samples_appended: AtomicU64,
    packets_received: AtomicU64,
    overrun_drops: AtomicU64,
    late_drops: AtomicU64,
    lost_packets: AtomicU64,
    underruns: AtomicU64,
    output_queue_len: AtomicU32,
    jitter_buffer_len: AtomicU32,
    sample_rate: AtomicU32,
    channels: AtomicU32,
}

impl DeviceMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the stream format once it's known, so reports can carry it (lets
    /// the UI convert buffer depths to milliseconds).
    pub fn set_format(&self, sample_rate: u32, channels: u16) {
        self.sample_rate.store(sample_rate, Ordering::Relaxed);
        self.channels.store(channels as u32, Ordering::Relaxed);
    }

    /// One block of `samples` interleaved values handed to the player.
    pub fn record_append(&self, samples: usize) {
        self.blocks_appended.fetch_add(1, Ordering::Relaxed);
        self.samples_appended
            .fetch_add(samples as u64, Ordering::Relaxed);
    }

    pub fn record_packet_received(&self) {
        self.packets_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_overrun_drop(&self) {
        self.overrun_drops.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_late_drop(&self) {
        self.late_drops.fetch_add(1, Ordering::Relaxed);
    }

    /// `n` packets declared lost (a seq-gap skipped by the jitter timeout).
    pub fn record_lost(&self, n: u64) {
        self.lost_packets.fetch_add(n, Ordering::Relaxed);
    }

    pub fn record_underrun(&self) {
        self.underruns.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_output_queue_len(&self, n: usize) {
        self.output_queue_len.store(n as u32, Ordering::Relaxed);
    }

    pub fn set_jitter_buffer_len(&self, n: usize) {
        self.jitter_buffer_len.store(n as u32, Ordering::Relaxed);
    }

    /// A wire snapshot of all current values, stamped with the local clock.
    pub fn snapshot(&self) -> TelemetryReport {
        TelemetryReport {
            sent_ms: now_epoch_ms(),
            sample_rate: self.sample_rate.load(Ordering::Relaxed),
            channels: self.channels.load(Ordering::Relaxed) as u16,
            blocks_appended: self.blocks_appended.load(Ordering::Relaxed),
            samples_appended: self.samples_appended.load(Ordering::Relaxed),
            packets_received: self.packets_received.load(Ordering::Relaxed),
            overrun_drops: self.overrun_drops.load(Ordering::Relaxed),
            late_drops: self.late_drops.load(Ordering::Relaxed),
            lost_packets: self.lost_packets.load(Ordering::Relaxed),
            underruns: self.underruns.load(Ordering::Relaxed),
            output_queue_len: self.output_queue_len.load(Ordering::Relaxed),
            jitter_buffer_len: self.jitter_buffer_len.load(Ordering::Relaxed),
        }
    }
}

/// Client: every `interval_ms`, snapshot `metrics` and send it to the server.
/// Waits (polling `server_ip`) until the server address is known, since it's
/// learned from the first multicast datagram. Runs forever; intended for a
/// thread.
pub fn run_telemetry_sender(
    server_ip: Arc<Mutex<Option<Ipv4Addr>>>,
    port: u16,
    metrics: Arc<DeviceMetrics>,
    interval_ms: u64,
) {
    let sock = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("telemetry: could not bind socket: {e}");
            return;
        }
    };
    let interval = std::time::Duration::from_millis(interval_ms);
    loop {
        let dest = *server_ip.lock().unwrap();
        if let Some(ip) = dest {
            let report = metrics.snapshot();
            if let Ok(bytes) = bincode::serialize(&report) {
                let _ = sock.send_to(&bytes, (ip, port));
            }
        }
        std::thread::sleep(interval);
    }
}

// ---------------------------------------------------------------------------
// Server side
// ---------------------------------------------------------------------------

/// Server-side send-path metrics, written by the multicast send loop and sampled
/// into the telemetry store for the UI.
#[derive(Debug, Default)]
pub struct ServerMetrics {
    packets_sent: AtomicU64,
    frames_sent: AtomicU64,
    reanchors: AtomicU64,
    pending_len: AtomicU32,
    sample_rate: AtomicU32,
    channels: AtomicU32,
    lead_ms: AtomicU32,
}

impl ServerMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the fixed stream parameters, surfaced to the UI for context.
    pub fn set_static(&self, sample_rate: u32, channels: u16, lead_ms: u32) {
        self.sample_rate.store(sample_rate, Ordering::Relaxed);
        self.channels.store(channels as u32, Ordering::Relaxed);
        self.lead_ms.store(lead_ms, Ordering::Relaxed);
    }

    /// One packet of `frames` frames put on the wire.
    pub fn record_packet_sent(&self, frames: u64) {
        self.packets_sent.fetch_add(1, Ordering::Relaxed);
        self.frames_sent.fetch_add(frames, Ordering::Relaxed);
    }

    /// The timeline was re-anchored (source fell behind / paused / gap).
    pub fn record_reanchor(&self) {
        self.reanchors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_pending_len(&self, n: usize) {
        self.pending_len.store(n as u32, Ordering::Relaxed);
    }

    /// A timestamped sample of the current values, for the history ring.
    pub fn snapshot(&self) -> ServerSample {
        ServerSample {
            t: now_epoch_ms(),
            packets_sent: self.packets_sent.load(Ordering::Relaxed),
            frames_sent: self.frames_sent.load(Ordering::Relaxed),
            reanchors: self.reanchors.load(Ordering::Relaxed),
            pending_len: self.pending_len.load(Ordering::Relaxed),
        }
    }

    pub fn sample_rate(&self) -> u32 {
        self.sample_rate.load(Ordering::Relaxed)
    }
    pub fn channels(&self) -> u16 {
        self.channels.load(Ordering::Relaxed) as u16
    }
    pub fn lead_ms(&self) -> u32 {
        self.lead_ms.load(Ordering::Relaxed)
    }
}

/// One timestamped client telemetry sample kept in the server's history ring.
/// `t` is the server-receive time (epoch ms) — the graph's shared x-axis.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct ClientSample {
    pub t: u64,
    pub blocks_appended: u64,
    pub samples_appended: u64,
    pub packets_received: u64,
    pub overrun_drops: u64,
    pub late_drops: u64,
    pub lost_packets: u64,
    pub underruns: u64,
    pub output_queue_len: u32,
    pub jitter_buffer_len: u32,
}

/// One timestamped server send-path sample.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct ServerSample {
    pub t: u64,
    pub packets_sent: u64,
    pub frames_sent: u64,
    pub reanchors: u64,
    pub pending_len: u32,
}

struct ClientHistory {
    samples: VecDeque<ClientSample>,
    sample_rate: u32,
    channels: u16,
    last_report_ms: u64,
}

/// Per-device stats emitted to the UI: the recent history window plus context.
#[derive(Debug, Clone, Serialize)]
pub struct ClientStats {
    pub ip: String,
    pub sample_rate: u32,
    pub channels: u16,
    pub seconds_ago: f64,
    pub samples: Vec<ClientSample>,
}

/// The full `/api/stats` payload: server history + one entry per device.
#[derive(Debug, Clone, Serialize)]
pub struct StatsSnapshot {
    pub now_ms: u64,
    pub sample_rate: u32,
    pub channels: u16,
    pub lead_ms: u32,
    pub server: Vec<ServerSample>,
    pub clients: Vec<ClientStats>,
}

/// Server-side ring buffers of recent telemetry, keyed by client IP, plus the
/// server's own send-path history. Fed by the telemetry receiver and a periodic
/// server-metrics sampler; read by the HTTP `/api/stats` handler.
pub struct TelemetryStore {
    clients: Mutex<HashMap<Ipv4Addr, ClientHistory>>,
    server: Mutex<VecDeque<ServerSample>>,
}

impl TelemetryStore {
    pub fn new() -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            server: Mutex::new(VecDeque::new()),
        }
    }

    /// Record a report received from `ip` at server time `recv_ms`.
    pub fn push_client(&self, ip: Ipv4Addr, report: TelemetryReport, recv_ms: u64) {
        let mut map = self.clients.lock().unwrap();
        let hist = map.entry(ip).or_insert_with(|| ClientHistory {
            samples: VecDeque::new(),
            sample_rate: report.sample_rate,
            channels: report.channels,
            last_report_ms: recv_ms,
        });
        hist.sample_rate = report.sample_rate;
        hist.channels = report.channels;
        hist.last_report_ms = recv_ms;
        hist.samples.push_back(ClientSample {
            t: recv_ms,
            blocks_appended: report.blocks_appended,
            samples_appended: report.samples_appended,
            packets_received: report.packets_received,
            overrun_drops: report.overrun_drops,
            late_drops: report.late_drops,
            lost_packets: report.lost_packets,
            underruns: report.underruns,
            output_queue_len: report.output_queue_len,
            jitter_buffer_len: report.jitter_buffer_len,
        });
        while hist.samples.len() > HISTORY_LEN {
            hist.samples.pop_front();
        }
    }

    /// Record a server send-path sample.
    pub fn push_server(&self, sample: ServerSample) {
        let mut q = self.server.lock().unwrap();
        q.push_back(sample);
        while q.len() > HISTORY_LEN {
            q.pop_front();
        }
    }

    /// Build the `/api/stats` payload, dropping devices that have gone stale.
    pub fn snapshot(&self, meta: &ServerMetrics) -> StatsSnapshot {
        let now = now_epoch_ms();
        let server: Vec<ServerSample> = self.server.lock().unwrap().iter().copied().collect();

        let mut map = self.clients.lock().unwrap();
        map.retain(|_, h| (now.saturating_sub(h.last_report_ms) as f64) / 1000.0 < STALE_SECS);
        let mut clients: Vec<ClientStats> = map
            .iter()
            .map(|(ip, h)| ClientStats {
                ip: ip.to_string(),
                sample_rate: h.sample_rate,
                channels: h.channels,
                seconds_ago: (now.saturating_sub(h.last_report_ms) as f64) / 1000.0,
                samples: h.samples.iter().copied().collect(),
            })
            .collect();
        clients.sort_by(|a, b| a.ip.cmp(&b.ip));

        StatsSnapshot {
            now_ms: now,
            sample_rate: meta.sample_rate(),
            channels: meta.channels(),
            lead_ms: meta.lead_ms(),
            server,
            clients,
        }
    }
}

impl Default for TelemetryStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Server: receive client [`TelemetryReport`]s and record them in `store`,
/// keyed by the datagram's source IP. Runs forever; intended for a thread.
pub fn run_telemetry_receiver(port: u16, store: Arc<TelemetryStore>) {
    let sock = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("telemetry receiver: could not bind {port}: {e}");
            return;
        }
    };
    let mut buf = [0u8; 512];
    loop {
        match sock.recv_from(&mut buf) {
            Ok((n, SocketAddr::V4(v4))) => {
                if let Ok(report) = bincode::deserialize::<TelemetryReport>(&buf[..n]) {
                    store.push_client(*v4.ip(), report, now_epoch_ms());
                }
                // Malformed datagram or non-IPv4 source: ignore.
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("telemetry receiver: recv error: {e}");
                return;
            }
        }
    }
}
