//! NTP-like clock synchronization.
//!
//! Multicast is one-way (server → clients), so to estimate the server's clock a
//! client opens a *unicast* side-channel to the server and does an NTP-style
//! round-trip exchange. [`SyncedClock`] turns those measurements into a smooth,
//! continuously-corrected estimate of the server's clock that playback
//! schedules against — no external NTP daemon required.

use std::collections::{HashMap, VecDeque};
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::metrics::DeviceMetrics;
use crate::wire::{SettingsRequest, SettingsResponse, TimeRequest, TimeResponse, now_epoch_ms};

/// How many recent measurements to keep when choosing the best offset.
const SAMPLE_WINDOW: usize = 64;
/// Maximum rate at which the *applied* offset may move toward the target, as a
/// fraction of real time (0.005 = 0.5%). Keeps the induced tempo/pitch change
/// well below the threshold of perception while still tracking drift.
const MAX_SLEW_FRACTION: f64 = 0.005;

/// Number of quick exchanges to do before settling into the steady interval.
const WARMUP_SAMPLES: usize = 8;
const WARMUP_INTERVAL_MS: u64 = 150;
/// Steady sync cadence
const STEADY_INTERVAL_MS: u64 = 500;
/// Fallback poll / keepalive interval. The server pushes changes instantly, so
/// this only recovers a missed push and refreshes the client's address.
const SETTINGS_INTERVAL_MS: u64 = 1000;

#[derive(Clone, Copy)]
struct Sample {
    offset_ms: f64,
    rtt_ms: f64,
}

struct State {
    samples: VecDeque<Sample>,
    /// Best current estimate of (server_clock − local_clock), in ms.
    target_offset_ms: f64,
    /// The offset actually in effect; slewed toward `target_offset_ms`.
    applied_offset_ms: f64,
    last_local_ms: f64,
    initialized: bool,
}

/// A snapshot of the clock-sync state, for telemetry.
#[derive(Clone, Copy)]
pub struct SyncStats {
    /// Applied offset (server_clock - client_clock), ms; what playback uses.
    pub applied_offset_ms: f64,
    /// Best offset estimate, ms (lowest-RTT sample in the window).
    pub target_offset_ms: f64,
    /// Lowest RTT in the window, ms.
    pub best_rtt_ms: f64,
    /// Raw offset from the most recent NTP exchange, ms (before lowest-RTT pick).
    pub last_offset_ms: f64,
    /// RTT of the most recent NTP exchange, ms.
    pub last_rtt_ms: f64,
    /// Number of samples currently held.
    pub samples: usize,
}

/// A continuously-corrected estimate of the server's clock.
pub struct SyncedClock {
    state: Mutex<State>,
}

impl SyncedClock {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State {
                samples: VecDeque::new(),
                target_offset_ms: 0.0,
                applied_offset_ms: 0.0,
                last_local_ms: now_epoch_ms() as f64,
                initialized: false,
            }),
        }
    }

    /// Feed a raw round-trip measurement.
    pub fn add_sample(&self, offset_ms: f64, rtt_ms: f64) {
        let mut s = self.state.lock().unwrap();
        s.samples.push_back(Sample { offset_ms, rtt_ms });
        while s.samples.len() > SAMPLE_WINDOW {
            s.samples.pop_front();
        }

        // Use the offset from the lowest-RTT sample in the window: it is the
        // one least distorted by transient network/queuing delay (a standard
        // NTP heuristic), which keeps the target stable against jitter.
        if let Some(best) = s
            .samples
            .iter()
            .min_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms))
            .copied()
        {
            s.target_offset_ms = best.offset_ms;
        }

        if !s.initialized {
            // First estimate: adopt it directly. Playback hasn't started relying
            // on the clock yet, so there is nothing to slew smoothly from.
            s.applied_offset_ms = s.target_offset_ms;
            s.last_local_ms = now_epoch_ms() as f64;
            s.initialized = true;
        }
    }

    pub fn is_initialized(&self) -> bool {
        self.state.lock().unwrap().initialized
    }

    pub fn sample_count(&self) -> usize {
        self.state.lock().unwrap().samples.len()
    }

    /// Current sync state for telemetry.
    pub fn stats(&self) -> SyncStats {
        let s = self.state.lock().unwrap();
        let best_rtt = s
            .samples
            .iter()
            .map(|x| x.rtt_ms)
            .fold(f64::INFINITY, f64::min);
        let last = s.samples.back();
        let last_offset_ms = last.map(|x| x.offset_ms).unwrap_or(0.0);
        let last_rtt_ms = last.map(|x| x.rtt_ms).unwrap_or(0.0);
        SyncStats {
            applied_offset_ms: s.applied_offset_ms,
            target_offset_ms: s.target_offset_ms,
            best_rtt_ms: if best_rtt.is_finite() { best_rtt } else { 0.0 },
            last_offset_ms,
            last_rtt_ms,
            samples: s.samples.len(),
        }
    }

    /// Best estimate of the server's current clock (epoch ms, fractional). Each
    /// call advances the applied offset toward the target by at most
    /// `MAX_SLEW_FRACTION` of the elapsed time, so the correction is spread out
    /// and never causes an audible jump.
    pub fn server_now_ms(&self) -> f64 {
        let mut s = self.state.lock().unwrap();
        let local = now_epoch_ms() as f64;
        let elapsed = (local - s.last_local_ms).max(0.0);
        s.last_local_ms = local;

        let max_step = MAX_SLEW_FRACTION * elapsed;
        let diff = s.target_offset_ms - s.applied_offset_ms;
        s.applied_offset_ms += diff.clamp(-max_step, max_step);

        local + s.applied_offset_ms
    }
}

impl Default for SyncedClock {
    fn default() -> Self {
        Self::new()
    }
}

/// A latch holding the latest server-assigned volume, written by the sync
/// thread and consumed by the playback loop.
pub struct VolumeCell {
    inner: Mutex<VolumeState>,
}

struct VolumeState {
    value: f32,
    dirty: bool,
}

impl VolumeCell {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(VolumeState {
                value: 1.0,
                dirty: false,
            }),
        }
    }

    /// Record a new target volume; marks it dirty only if it actually changed.
    pub fn set(&self, value: f32) {
        let mut s = self.inner.lock().unwrap();
        if (value - s.value).abs() > f32::EPSILON {
            s.value = value;
            s.dirty = true;
        }
    }

    /// Return the volume if it changed since the last call, clearing the flag.
    pub fn take_update(&self) -> Option<f32> {
        let mut s = self.inner.lock().unwrap();
        if s.dirty {
            s.dirty = false;
            Some(s.value)
        } else {
            None
        }
    }
}

impl Default for VolumeCell {
    fn default() -> Self {
        Self::new()
    }
}

/// A client that has recently contacted the server for time-sync.
struct ClientEntry {
    last_seen: Instant,
    volume: f32,
    delay_ms: u32,
    /// Address this client sends settings requests from; where we push changes.
    settings_addr: Option<SocketAddr>,
}

/// The per-client settings returned to a client on each sync.
#[derive(Clone, Copy)]
pub struct ClientSettings {
    pub volume: f32,
    pub delay_ms: u32,
}

/// Snapshot of one client for the HTTP API.
pub struct ClientStatus {
    pub ip: Ipv4Addr,
    pub seconds_ago: f64,
    pub volume: f32,
    pub delay_ms: u32,
    pub connected: bool,
}

/// Server-side registry of connected clients and their assigned volumes.
/// Populated from time-sync traffic and read/written by the HTTP API.
pub struct ClientRegistry {
    clients: Mutex<HashMap<Ipv4Addr, ClientEntry>>,
    /// Socket used to push settings changes to clients (set by the server).
    push_sock: Mutex<Option<Arc<UdpSocket>>>,
}

impl ClientRegistry {
    /// A client is "connected" if seen within this window.
    const CONNECTED_SECS: f64 = 5.0;
    /// Forget clients not seen for this long, so the list doesn't grow forever.
    const STALE_SECS: f64 = 60.0;
    /// Max playback advance. Must stay comfortably below the server's send lead
    /// (SEND_LEAD_MS): a client can only skip forward into audio it has already
    /// buffered, and needs headroom left over to absorb jitter.
    pub const MAX_DELAY_MS: u32 = 150;

    pub fn new() -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            push_sock: Mutex::new(None),
        }
    }

    /// Provide the UDP socket used to push settings changes to clients.
    pub fn set_push_socket(&self, sock: Arc<UdpSocket>) {
        *self.push_sock.lock().unwrap() = Some(sock);
    }

    /// Mark `ip` as seen now, creating it (defaults) if new. Returns its current
    /// assigned settings, for the sync response.
    pub fn touch(&self, ip: Ipv4Addr) -> ClientSettings {
        let mut map = self.clients.lock().unwrap();
        let entry = map.entry(ip).or_insert(ClientEntry {
            last_seen: Instant::now(),
            volume: 1.0,
            delay_ms: 0,
            settings_addr: None,
        });
        entry.last_seen = Instant::now();
        ClientSettings {
            volume: entry.volume,
            delay_ms: entry.delay_ms,
        }
    }

    /// Register/refresh a client from a settings request, recording the address
    /// to push future changes to. Returns its current settings.
    pub fn record_settings_addr(&self, ip: Ipv4Addr, addr: SocketAddr) -> ClientSettings {
        let mut map = self.clients.lock().unwrap();
        let entry = map.entry(ip).or_insert(ClientEntry {
            last_seen: Instant::now(),
            volume: 1.0,
            delay_ms: 0,
            settings_addr: None,
        });
        entry.last_seen = Instant::now();
        entry.settings_addr = Some(addr);
        ClientSettings {
            volume: entry.volume,
            delay_ms: entry.delay_ms,
        }
    }

    /// Set a client's volume and immediately push it. No-op if unknown.
    pub fn set_volume(&self, ip: Ipv4Addr, volume: f32) -> bool {
        let push = {
            let mut map = self.clients.lock().unwrap();
            match map.get_mut(&ip) {
                Some(entry) => {
                    entry.volume = volume.clamp(0.0, 1.0);
                    Some(entry.settings_addr.map(|a| (a, entry.volume, entry.delay_ms)))
                }
                None => None,
            }
        };
        match push {
            Some(target) => {
                if let Some((addr, vol, delay)) = target {
                    self.push_settings(addr, vol, delay);
                }
                true
            }
            None => false,
        }
    }

    /// Set a client's playback advance (ms earlier than play_at) and push it.
    /// No-op if the client isn't known.
    pub fn set_delay(&self, ip: Ipv4Addr, delay_ms: u32) -> bool {
        let push = {
            let mut map = self.clients.lock().unwrap();
            match map.get_mut(&ip) {
                Some(entry) => {
                    entry.delay_ms = delay_ms.min(Self::MAX_DELAY_MS);
                    Some(entry.settings_addr.map(|a| (a, entry.volume, entry.delay_ms)))
                }
                None => None,
            }
        };
        match push {
            Some(target) => {
                if let Some((addr, vol, delay)) = target {
                    self.push_settings(addr, vol, delay);
                }
                true
            }
            None => false,
        }
    }

    /// Push a client's current settings to its last-known address, so a change
    /// reaches the device at once instead of waiting for its next poll.
    fn push_settings(&self, addr: SocketAddr, volume: f32, delay_ms: u32) {
        let sock = self.push_sock.lock().unwrap().clone();
        if let Some(sock) = sock {
            let resp = SettingsResponse { nonce: 0, volume, delay_ms };
            if let Ok(bytes) = bincode::serialize(&resp) {
                let _ = sock.send_to(&bytes, addr);
            }
        }
    }

    /// Current clients, dropping any that have gone stale.
    pub fn snapshot(&self) -> Vec<ClientStatus> {
        let mut map = self.clients.lock().unwrap();
        let now = Instant::now();
        map.retain(|_, e| now.duration_since(e.last_seen).as_secs_f64() < Self::STALE_SECS);
        let mut out: Vec<ClientStatus> = map
            .iter()
            .map(|(ip, e)| {
                let secs = now.duration_since(e.last_seen).as_secs_f64();
                ClientStatus {
                    ip: *ip,
                    seconds_ago: secs,
                    volume: e.volume,
                    delay_ms: e.delay_ms,
                    connected: secs < Self::CONNECTED_SECS,
                }
            })
            .collect();
        out.sort_by_key(|c| c.ip.octets());
        out
    }
}

impl Default for ClientRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Client side: repeatedly exchange timestamps with the server and feed the
/// results into `clock`, applying any volume the server sends back to `volume`.
/// Runs forever; intended to live on its own thread.
pub fn run_client_sync(
    server_ip: Ipv4Addr,
    sync_port: u16,
    clock: Arc<SyncedClock>,
    metrics: Arc<DeviceMetrics>,
) {
    let sock = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("time-sync: could not bind socket: {e}");
            return;
        }
    };
    let _ = sock.set_read_timeout(Some(Duration::from_millis(500)));
    let dest = (server_ip, sync_port);

    let mut nonce: u64 = 0;
    let mut buf = [0u8; 256];

    loop {
        nonce = nonce.wrapping_add(1);
        let t1 = now_epoch_ms();
        let req = TimeRequest {
            client_send_ms: t1,
            nonce,
        };
        if let Ok(bytes) = bincode::serialize(&req) {
            let _ = sock.send_to(&bytes, dest);
        }

        // Wait for the matching reply (drop stale/mismatched ones).
        if let Ok((n, _)) = sock.recv_from(&mut buf) {
            if let Ok(resp) = bincode::deserialize::<TimeResponse>(&buf[..n]) {
                if resp.nonce == nonce {
                    let t4 = now_epoch_ms();
                    let rtt = t4.saturating_sub(t1) as f64;
                    // offset = server_clock − midpoint of the client send/recv.
                    let offset = resp.server_ms as f64 - (t1 as f64 + t4 as f64) / 2.0;
                    clock.add_sample(offset, rtt);
                    let st = clock.stats();
                    metrics.record_sync(
                        st.applied_offset_ms,
                        st.target_offset_ms,
                        st.last_offset_ms,
                        st.last_rtt_ms,
                        st.best_rtt_ms,
                        st.samples as u32,
                    );
                }
            }
        }

        let interval = if clock.sample_count() < WARMUP_SAMPLES {
            WARMUP_INTERVAL_MS
        } else {
            STEADY_INTERVAL_MS
        };
        std::thread::sleep(Duration::from_millis(interval));
    }
}

/// Server side: answer time-sync requests with the current server clock, and
/// register the requesting client / return its assigned volume. Runs forever;
/// intended to live on its own thread.
pub fn run_server_responder(sync_port: u16, registry: Arc<ClientRegistry>) {
    let sock = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, sync_port)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("time-sync responder: could not bind {sync_port}: {e}");
            return;
        }
    };
    let mut buf = [0u8; 256];
    loop {
        match sock.recv_from(&mut buf) {
            Ok((n, from)) => {
                if let Ok(req) = bincode::deserialize::<TimeRequest>(&buf[..n]) {
                    // Register the client (by IP) for liveness / the web UI list.
                    if let std::net::SocketAddr::V4(v4) = from {
                        registry.touch(*v4.ip());
                    }
                    let resp = TimeResponse {
                        client_send_ms: req.client_send_ms,
                        server_ms: now_epoch_ms(),
                        nonce: req.nonce,
                    };
                    if let Ok(bytes) = bincode::serialize(&resp) {
                        let _ = sock.send_to(&bytes, from);
                    }
                }
            }
            Err(e) => {
                eprintln!("time-sync responder: recv error: {e}");
                return;
            }
        }
    }
}

/// Client side: periodically ask the server for this client's settings
/// (volume + delay) on the dedicated settings port and apply them. Kept
/// separate from the time-sync exchange. Runs forever; intended for a thread.
pub fn run_settings_client(
    server_ip: Ipv4Addr,
    settings_port: u16,
    volume: Arc<VolumeCell>,
    delay_ms: Arc<AtomicU32>,
) {
    let sock = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("settings: could not bind socket: {e}");
            return;
        }
    };
    // Block up to the fallback interval per recv; a server push wakes us at once.
    let _ = sock.set_read_timeout(Some(Duration::from_millis(SETTINGS_INTERVAL_MS)));
    let dest = (server_ip, settings_port);
    let mut buf = [0u8; 256];
    loop {
        // Fallback poll / keepalive: lets the server learn our address (so it
        // can push) and recovers any missed push.
        let req = SettingsRequest { nonce: 0 };
        if let Ok(bytes) = bincode::serialize(&req) {
            let _ = sock.send_to(&bytes, dest);
        }
        // Apply every settings message the instant it lands (server pushes and
        // the poll reply), until the socket is quiet for the interval.
        loop {
            match sock.recv_from(&mut buf) {
                Ok((n, _)) => {
                    if let Ok(resp) = bincode::deserialize::<SettingsResponse>(&buf[..n]) {
                        volume.set(resp.volume);
                        delay_ms.store(resp.delay_ms, Ordering::Relaxed);
                    }
                }
                Err(_) => break,
            }
        }
    }
}

/// Server side: answer settings requests, recording each client's address so
/// changes can be pushed to it, and reply with its current settings. Runs
/// forever; intended for its own thread. `sock` is the shared settings socket.
pub fn run_settings_responder(sock: Arc<UdpSocket>, registry: Arc<ClientRegistry>) {
    let mut buf = [0u8; 256];
    loop {
        match sock.recv_from(&mut buf) {
            Ok((n, from)) => {
                if bincode::deserialize::<SettingsRequest>(&buf[..n]).is_ok() {
                    let settings = if let std::net::SocketAddr::V4(v4) = from {
                        registry.record_settings_addr(*v4.ip(), from)
                    } else {
                        ClientSettings { volume: 1.0, delay_ms: 0 }
                    };
                    let resp = SettingsResponse {
                        nonce: 0,
                        volume: settings.volume,
                        delay_ms: settings.delay_ms,
                    };
                    if let Ok(bytes) = bincode::serialize(&resp) {
                        let _ = sock.send_to(&bytes, from);
                    }
                }
            }
            Err(e) => {
                eprintln!("settings responder: recv error: {e}");
                return;
            }
        }
    }
}
