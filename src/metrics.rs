//! Live telemetry. Client devices measure their own buffers and sample flow into
//! lock-free [`DeviceMetrics`] and stream periodic [`TelemetryReport`]s over TCP
//! to every server they know; each server records its per-source send path in
//! [`ServerMetrics`] and keeps a short history in a [`TelemetryStore`] that the
//! web UI subscribes to (SSE).
//!
//! Counters are cumulative since process start; gauges are the instantaneous
//! value at snapshot time. Recording is always lock-free so it never stalls the
//! audio threads.
//!
//! Source ids are 64-bit and are serialized to JSON as *strings*, because they
//! exceed JavaScript's safe-integer range (2^53).

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream, UdpSocket};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use serde::Serialize;

use crate::catalog::CatalogStore;
use crate::net::{bind_reuse, set_multicast_if};
use crate::sync::ClientSettings;
use crate::wire::{
    STATS_GROUP, STATS_PORT, SourceStat, StatsBroadcast, TELEMETRY_PORT, TelemetryReport,
    now_epoch_ms,
};

/// Retained history per source: ~60 s at the client's 10 Hz report rate.
const HISTORY_LEN: usize = 600;
/// Forget a client's history if it stops reporting for this long.
const STALE_SECS: f64 = 60.0;
/// A client is "connected" if a report arrived within this window.
const CONNECTED_SECS: f64 = 5.0;
/// Cap on a single framed telemetry report, to reject junk on the TCP stream.
const MAX_REPORT_BYTES: usize = 65536;

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
    /// Channels on the local output device (fixed at startup).
    output_channels: AtomicU32,
    // Clock-sync state (f64 stored as bits since there is no AtomicF64).
    clock_offset_bits: AtomicU64,
    clock_target_bits: AtomicU64,
    last_offset_bits: AtomicU64,
    last_rtt_bits: AtomicU64,
    rtt_bits: AtomicU64,
    sync_samples: AtomicU32,
}

impl DeviceMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the current stream format, so reports can carry it (lets the UI
    /// convert buffer depths to milliseconds). Updated on every source switch.
    pub fn set_format(&self, sample_rate: u32, channels: u16) {
        self.sample_rate.store(sample_rate, Ordering::Relaxed);
        self.channels.store(channels as u32, Ordering::Relaxed);
    }

    /// Record the output device's channel count (set once at startup).
    pub fn set_output_channels(&self, channels: u16) {
        self.output_channels
            .store(channels as u32, Ordering::Relaxed);
    }

    /// The output device's channel count, for building the default channel map.
    pub fn output_channels(&self) -> u16 {
        self.output_channels.load(Ordering::Relaxed) as u16
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

    /// Current jitter-buffer depth in packets (for the combined-budget cap).
    pub fn jitter_buffer_len(&self) -> u32 {
        self.jitter_buffer_len.load(Ordering::Relaxed)
    }

    /// Record the latest clock-sync state (offsets/RTT in ms, sample count).
    pub fn record_sync(
        &self,
        applied_ms: f64,
        target_ms: f64,
        last_offset_ms: f64,
        last_rtt_ms: f64,
        rtt_ms: f64,
        samples: u32,
    ) {
        self.clock_offset_bits
            .store(applied_ms.to_bits(), Ordering::Relaxed);
        self.clock_target_bits
            .store(target_ms.to_bits(), Ordering::Relaxed);
        self.last_offset_bits
            .store(last_offset_ms.to_bits(), Ordering::Relaxed);
        self.last_rtt_bits
            .store(last_rtt_ms.to_bits(), Ordering::Relaxed);
        self.rtt_bits.store(rtt_ms.to_bits(), Ordering::Relaxed);
        self.sync_samples.store(samples, Ordering::Relaxed);
    }

    /// A wire snapshot of all current values, stamped with the local clock. The
    /// client-owned settings fields (selected source / volume / delay) are left
    /// at defaults here and filled by the telemetry sender from [`ClientSettings`].
    pub fn snapshot(&self) -> TelemetryReport {
        TelemetryReport {
            sent_ms: now_epoch_ms(),
            device_id: String::new(),
            mac: [0; 6],
            hostname: String::new(),
            sample_rate: self.sample_rate.load(Ordering::Relaxed),
            channels: self.channels.load(Ordering::Relaxed) as u16,
            output_channels: self.output_channels.load(Ordering::Relaxed) as u16,
            selected_source_id: 0,
            volume: 0.0,
            delay_ms: 0,
            channel_map: Vec::new(),
            blocks_appended: self.blocks_appended.load(Ordering::Relaxed),
            samples_appended: self.samples_appended.load(Ordering::Relaxed),
            packets_received: self.packets_received.load(Ordering::Relaxed),
            overrun_drops: self.overrun_drops.load(Ordering::Relaxed),
            late_drops: self.late_drops.load(Ordering::Relaxed),
            lost_packets: self.lost_packets.load(Ordering::Relaxed),
            underruns: self.underruns.load(Ordering::Relaxed),
            output_queue_len: self.output_queue_len.load(Ordering::Relaxed),
            jitter_buffer_len: self.jitter_buffer_len.load(Ordering::Relaxed),
            clock_offset_ms: f64::from_bits(self.clock_offset_bits.load(Ordering::Relaxed)),
            clock_target_offset_ms: f64::from_bits(self.clock_target_bits.load(Ordering::Relaxed)),
            last_offset_ms: f64::from_bits(self.last_offset_bits.load(Ordering::Relaxed)),
            last_rtt_ms: f64::from_bits(self.last_rtt_bits.load(Ordering::Relaxed)),
            rtt_ms: f64::from_bits(self.rtt_bits.load(Ordering::Relaxed)),
            sync_samples: self.sync_samples.load(Ordering::Relaxed),
        }
    }
}

/// Frame a report for the length-prefixed TCP telemetry stream.
fn frame(bytes: &[u8]) -> Vec<u8> {
    let mut out = (bytes.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(bytes);
    out
}

/// Client: every `interval_ms`, snapshot `metrics` + the client-owned `settings`
/// and send the report over a persistent TCP connection to **every** server
/// currently in the catalog. TCP gives timely, reliable delivery (no multicast
/// loss); connections are opened lazily and reopened on error. Runs forever.
pub fn run_telemetry_sender(
    settings: Arc<ClientSettings>,
    metrics: Arc<DeviceMetrics>,
    device_id: String,
    mac: [u8; 6],
    hostname: String,
    catalog: Arc<CatalogStore>,
    interval_ms: u64,
) {
    let interval = Duration::from_millis(interval_ms);
    let mut conns: HashMap<Ipv4Addr, TcpStream> = HashMap::new();
    loop {
        let mut report = metrics.snapshot();
        let (sel, vol, delay) = settings.report_values();
        report.selected_source_id = sel;
        report.volume = vol;
        report.delay_ms = delay;
        report.channel_map = settings.channel_map();
        report.device_id = device_id.clone();
        report.mac = mac;
        report.hostname = hostname.clone();

        if let Ok(bytes) = bincode::serialize(&report) {
            let framed = frame(&bytes);
            let servers = catalog.server_ips();
            // Drop connections to servers that vanished from the catalog.
            conns.retain(|ip, _| servers.contains(ip));
            for ip in servers {
                if let std::collections::hash_map::Entry::Vacant(e) = conns.entry(ip) {
                    match TcpStream::connect_timeout(
                        &SocketAddr::from((ip, TELEMETRY_PORT)),
                        Duration::from_millis(500),
                    ) {
                        Ok(c) => {
                            let _ = c.set_nodelay(true);
                            e.insert(c);
                        }
                        Err(_) => continue, // retry next tick
                    }
                }
                if let Some(c) = conns.get_mut(&ip)
                    && c.write_all(&framed).is_err()
                {
                    conns.remove(&ip); // reconnect next tick
                }
            }
        }
        std::thread::sleep(interval);
    }
}

// ---------------------------------------------------------------------------
// Server side
// ---------------------------------------------------------------------------

/// Server-side send-path metrics for one source, written by that source's send
/// loop and sampled into the telemetry store for the UI.
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

    /// A redundant *copy* of an already-counted packet: bumps the wire packet
    /// count only (its frames are not new audio).
    pub fn record_copy_sent(&self) {
        self.packets_sent.fetch_add(1, Ordering::Relaxed);
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
    pub clock_offset_ms: f64,
    pub clock_target_offset_ms: f64,
    pub last_offset_ms: f64,
    pub last_rtt_ms: f64,
    pub rtt_ms: f64,
    pub sync_samples: u32,
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
    output_channels: u16,
    last_report_ms: u64,
    /// Highest client-clock send time seen, to drop out-of-order/duplicate
    /// reports (multicast can deliver in bursts / reorder).
    last_sent_ms: u64,
    /// Current source IP of the client (for targeting control commands).
    ip: Ipv4Addr,
    /// Device hostname reported by the client (the default display name).
    hostname: String,
    // Latest reported client-owned settings, for the /api/clients list.
    volume: f32,
    delay_ms: u32,
    selected_source_id: u64,
    channel_map: Vec<i16>,
}

/// Per-device stats emitted to the UI: the recent history window plus context.
/// Keyed by device id so the UI can match it to the client list.
#[derive(Debug, Clone, Serialize)]
pub struct ClientStats {
    pub id: String,
    pub sample_rate: u32,
    pub channels: u16,
    pub seconds_ago: f64,
    pub samples: Vec<ClientSample>,
}

/// One server source's send-path stats (one UI "server" card per source).
#[derive(Debug, Clone, Serialize)]
pub struct ServerSourceStats {
    /// Source id as a string (see module note on JS integer range).
    pub source_id: String,
    pub name: String,
    pub sample_rate: u32,
    pub channels: u16,
    pub lead_ms: u32,
    /// Redundant copies per packet and the last-copy lead (send-timing sliders).
    /// Meaningful for local sources; 0 for remote (not controllable from here).
    pub redundancy: u32,
    pub last_lead_ms: u64,
    /// Whether this source streams by unicast to listeners (local sources only).
    pub unicast: bool,
    /// True if hosted by a different server (learned via the stats broadcast).
    pub remote: bool,
    pub samples: Vec<ServerSample>,
}

/// Metadata for one source the UI should show a send-path card for. Built by the
/// caller from the catalog, so it spans local and remote servers.
pub struct SourceMeta {
    pub id: u64,
    pub name: String,
    pub sample_rate: u32,
    pub channels: u16,
    pub lead_ms: u32,
    pub redundancy: u32,
    pub last_lead_ms: u64,
    pub unicast: bool,
    pub remote: bool,
}

/// The full `/api/stats` payload: per-source server history + one entry per device.
#[derive(Debug, Clone, Serialize)]
pub struct StatsSnapshot {
    pub now_ms: u64,
    pub server: Vec<ServerSourceStats>,
    pub clients: Vec<ClientStats>,
}

/// A client's latest state for the `/api/clients` list (not the graph history).
pub struct ClientSummary {
    pub id: String,
    pub ip: Ipv4Addr,
    pub hostname: String,
    pub seconds_ago: f64,
    pub connected: bool,
    pub volume: f32,
    pub delay_ms: u32,
    pub selected_source_id: u64,
    pub output_channels: u16,
    pub channel_map: Vec<i16>,
}

/// Server-side ring buffers of recent telemetry, keyed by client IP, plus the
/// per-source server send-path history. Fed by the telemetry receiver and a
/// periodic server-metrics sampler; read by the HTTP `/api/*` handlers.
pub struct TelemetryStore {
    /// Keyed by client device id (the `--id` value, else MAC hex).
    clients: Mutex<HashMap<String, ClientHistory>>,
    server_hist: Mutex<HashMap<u64, VecDeque<ServerSample>>>,
}

impl TelemetryStore {
    pub fn new() -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            server_hist: Mutex::new(HashMap::new()),
        }
    }

    /// Record a report received from `ip` at server time `recv_ms`, keyed by the
    /// client's MAC.
    pub fn push_client(&self, ip: Ipv4Addr, report: TelemetryReport, recv_ms: u64) {
        let mut map = self.clients.lock().unwrap();
        let hist = map
            .entry(report.device_id.clone())
            .or_insert_with(|| ClientHistory {
                samples: VecDeque::new(),
                sample_rate: report.sample_rate,
                channels: report.channels,
                output_channels: report.output_channels,
                last_report_ms: recv_ms,
                last_sent_ms: 0,
                ip,
                hostname: report.hostname.clone(),
                volume: report.volume,
                delay_ms: report.delay_ms,
                selected_source_id: report.selected_source_id,
                channel_map: report.channel_map.clone(),
            });
        // Drop reordered/duplicate reports so the graph x-axis stays monotonic.
        if report.sent_ms <= hist.last_sent_ms {
            return;
        }
        hist.last_sent_ms = report.sent_ms;
        // Plot each sample at the client's *send* time, mapped onto the server
        // clock via the reported offset. The client sends at an even 10 Hz, so
        // this spaces samples evenly regardless of multicast delivery jitter
        // (receive-time spacing is bursty over a real network → choppy graphs).
        let disp_t = (report.sent_ms as f64 + report.clock_offset_ms).max(0.0) as u64;
        hist.sample_rate = report.sample_rate;
        hist.channels = report.channels;
        hist.output_channels = report.output_channels;
        hist.last_report_ms = recv_ms;
        hist.ip = ip;
        hist.hostname = report.hostname.clone();
        hist.volume = report.volume;
        hist.delay_ms = report.delay_ms;
        hist.selected_source_id = report.selected_source_id;
        hist.channel_map = report.channel_map.clone();
        hist.samples.push_back(ClientSample {
            t: disp_t,
            blocks_appended: report.blocks_appended,
            samples_appended: report.samples_appended,
            packets_received: report.packets_received,
            overrun_drops: report.overrun_drops,
            late_drops: report.late_drops,
            lost_packets: report.lost_packets,
            underruns: report.underruns,
            output_queue_len: report.output_queue_len,
            jitter_buffer_len: report.jitter_buffer_len,
            clock_offset_ms: report.clock_offset_ms,
            clock_target_offset_ms: report.clock_target_offset_ms,
            last_offset_ms: report.last_offset_ms,
            last_rtt_ms: report.last_rtt_ms,
            rtt_ms: report.rtt_ms,
            sync_samples: report.sync_samples,
        });
        while hist.samples.len() > HISTORY_LEN {
            hist.samples.pop_front();
        }
    }

    /// Record a server send-path sample for one source.
    pub fn push_server(&self, source_id: u64, sample: ServerSample) {
        let mut map = self.server_hist.lock().unwrap();
        let q = map.entry(source_id).or_default();
        q.push_back(sample);
        while q.len() > HISTORY_LEN {
            q.pop_front();
        }
    }

    /// Each currently-known client's latest state, for the `/api/clients` list.
    pub fn clients_summary(&self) -> Vec<ClientSummary> {
        let now = now_epoch_ms();
        let mut map = self.clients.lock().unwrap();
        map.retain(|_, h| (now.saturating_sub(h.last_report_ms) as f64) / 1000.0 < STALE_SECS);
        let mut out: Vec<ClientSummary> = map
            .iter()
            .map(|(id, h)| {
                let secs = (now.saturating_sub(h.last_report_ms) as f64) / 1000.0;
                ClientSummary {
                    id: id.clone(),
                    ip: h.ip,
                    hostname: h.hostname.clone(),
                    seconds_ago: secs,
                    connected: secs < CONNECTED_SECS,
                    volume: h.volume,
                    delay_ms: h.delay_ms,
                    selected_source_id: h.selected_source_id,
                    output_channels: h.output_channels,
                    channel_map: h.channel_map.clone(),
                }
            })
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    /// The current IP of the client with this device id, if known — used to
    /// target control commands (which address clients by IP).
    pub fn ip_for_id(&self, id: &str) -> Option<Ipv4Addr> {
        self.clients.lock().unwrap().get(id).map(|h| h.ip)
    }

    /// Build the `/api/stats` payload for the given sources (local + remote,
    /// from the catalog), dropping devices that have gone stale.
    pub fn snapshot(&self, sources: &[SourceMeta]) -> StatsSnapshot {
        let now = now_epoch_ms();

        let hist = self.server_hist.lock().unwrap();
        let mut server: Vec<ServerSourceStats> = sources
            .iter()
            .map(|m| ServerSourceStats {
                source_id: m.id.to_string(),
                name: m.name.clone(),
                sample_rate: m.sample_rate,
                channels: m.channels,
                lead_ms: m.lead_ms,
                redundancy: m.redundancy,
                last_lead_ms: m.last_lead_ms,
                unicast: m.unicast,
                remote: m.remote,
                samples: hist
                    .get(&m.id)
                    .map(|q| q.iter().copied().collect())
                    .unwrap_or_default(),
            })
            .collect();
        server.sort_by(|a, b| a.name.cmp(&b.name));
        drop(hist);

        let mut map = self.clients.lock().unwrap();
        map.retain(|_, h| (now.saturating_sub(h.last_report_ms) as f64) / 1000.0 < STALE_SECS);
        let mut clients: Vec<ClientStats> = map
            .iter()
            .map(|(id, h)| ClientStats {
                id: id.clone(),
                sample_rate: h.sample_rate,
                channels: h.channels,
                seconds_ago: (now.saturating_sub(h.last_report_ms) as f64) / 1000.0,
                samples: h.samples.iter().copied().collect(),
            })
            .collect();
        clients.sort_by(|a, b| a.id.cmp(&b.id));

        StatsSnapshot {
            now_ms: now,
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

/// Server: accept client telemetry over TCP on [`TELEMETRY_PORT`] and record it
/// into `store`, keyed by the connection's source IP. Each client keeps a
/// persistent connection and streams length-prefixed [`TelemetryReport`]s.
/// Runs forever; intended for a thread.
pub fn run_telemetry_receiver(store: Arc<TelemetryStore>) {
    let listener = match TcpListener::bind((Ipv4Addr::UNSPECIFIED, TELEMETRY_PORT)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("telemetry receiver: could not bind {TELEMETRY_PORT}: {e}");
            return;
        }
    };
    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                let store = store.clone();
                std::thread::spawn(move || handle_telemetry_conn(stream, store));
            }
            Err(e) => eprintln!("telemetry receiver: accept error: {e}"),
        }
    }
}

/// Read length-prefixed reports from one client connection until it closes.
fn handle_telemetry_conn(mut stream: TcpStream, store: Arc<TelemetryStore>) {
    let ip = match stream.peer_addr() {
        Ok(SocketAddr::V4(v4)) => *v4.ip(),
        _ => return,
    };
    let mut len_buf = [0u8; 4];
    loop {
        if stream.read_exact(&mut len_buf).is_err() {
            return; // connection closed / error
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 || len > MAX_REPORT_BYTES {
            return; // framing junk
        }
        let mut data = vec![0u8; len];
        if stream.read_exact(&mut data).is_err() {
            return;
        }
        if let Ok(report) = bincode::deserialize::<TelemetryReport>(&data) {
            store.push_client(ip, report, now_epoch_ms());
        }
    }
}

/// How often a server broadcasts its send-path stats to other servers.
const STATS_BROADCAST_MS: u64 = 250;

/// Supplies the current `(source_id, metrics)` pairs, read fresh each tick so a
/// hot-added/removed source is reflected without a restart.
pub type SamplerProvider = Arc<dyn Fn() -> Vec<(u64, Arc<ServerMetrics>)> + Send + Sync>;

/// Server: broadcast this server's per-source send-path stats on [`STATS_GROUP`]
/// at ~4 Hz, so any server's UI can graph these streams. Runs forever.
pub fn run_stats_broadcaster(server_id: u64, sources: SamplerProvider, iface: Ipv4Addr) {
    let sock = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("stats broadcaster: could not bind: {e}");
            return;
        }
    };
    sock.set_multicast_ttl_v4(1).ok();
    sock.set_multicast_loop_v4(true).ok();
    if iface != Ipv4Addr::UNSPECIFIED {
        let _ = set_multicast_if(&sock, iface);
    }
    let dest = (STATS_GROUP, STATS_PORT);
    loop {
        let stats: Vec<SourceStat> = sources()
            .iter()
            .map(|(id, m)| {
                let s = m.snapshot();
                SourceStat {
                    source_id: *id,
                    packets_sent: s.packets_sent,
                    frames_sent: s.frames_sent,
                    reanchors: s.reanchors,
                    pending_len: s.pending_len,
                }
            })
            .collect();
        let msg = StatsBroadcast {
            server_id,
            sent_ms: now_epoch_ms(),
            sources: stats,
        };
        if let Ok(bytes) = bincode::serialize(&msg) {
            let _ = sock.send_to(&bytes, dest);
        }
        std::thread::sleep(Duration::from_millis(STATS_BROADCAST_MS));
    }
}

/// Server: receive other servers' stats broadcasts into `store` so their streams
/// graph alongside the local ones. Own broadcasts (matching `my_server_id`) are
/// ignored — the local sampler already records them at full rate. Runs forever.
pub fn run_stats_receiver(store: Arc<TelemetryStore>, my_server_id: u64, iface: Ipv4Addr) {
    let sock = match bind_reuse(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, STATS_PORT)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("stats receiver: could not bind {STATS_PORT}: {e}");
            return;
        }
    };
    if let Err(e) = sock.join_multicast_v4(&STATS_GROUP, &iface) {
        eprintln!("stats receiver: could not join {STATS_GROUP}: {e}");
    }
    let mut buf = [0u8; 65536];
    loop {
        match sock.recv_from(&mut buf) {
            Ok((n, _)) => {
                if let Ok(msg) = bincode::deserialize::<StatsBroadcast>(&buf[..n]) {
                    if msg.server_id == my_server_id {
                        continue; // our own — already sampled locally at full rate
                    }
                    // Stamp with our receive time so remote streams share this
                    // server's timeline (and right edge) on the graphs.
                    let t = now_epoch_ms();
                    for st in msg.sources {
                        store.push_server(
                            st.source_id,
                            ServerSample {
                                t,
                                packets_sent: st.packets_sent,
                                frames_sent: st.frames_sent,
                                reanchors: st.reanchors,
                                pending_len: st.pending_len,
                            },
                        );
                    }
                }
            }
            Err(e) => {
                eprintln!("stats receiver: recv error: {e}");
                return;
            }
        }
    }
}
