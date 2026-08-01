//! Client clock synchronization and client-owned settings.
//!
//! Multicast audio is one-way (server → clients), so to estimate the owning
//! server's clock a client opens a *unicast* side-channel and does an NTP-style
//! round-trip exchange. [`SyncedClock`] turns those measurements into a smooth,
//! continuously-corrected estimate that playback schedules against.
//!
//! With many servers, no single server owns a client's settings, so the client
//! owns them itself in [`ClientSettings`] (selected source, volume, delay). The
//! web UI mutates them by multicasting a [`ControlCommand`]; the client applies
//! it and reflects the new value in its telemetry. The sync exchange re-points
//! to whichever server owns the currently-selected source via [`SyncTarget`].

use std::collections::{HashMap, VecDeque};
use std::net::{Ipv4Addr, UdpSocket};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::metrics::DeviceMetrics;
use crate::wire::{ControlCommand, MAX_DELAY_MS, TimeRequest, TimeResponse, now_epoch_ms};

/// How many recent measurements to keep when choosing the best offset.
const SAMPLE_WINDOW: usize = 64;
/// Maximum rate at which the *applied* offset may move toward the target, as a
/// fraction of real time (0.005 = 0.5%). Keeps the induced tempo/pitch change
/// well below the threshold of perception while still tracking drift.
const MAX_SLEW_FRACTION: f64 = 0.005;

/// Number of quick exchanges to do before settling into the steady interval.
const WARMUP_SAMPLES: usize = 8;
const WARMUP_INTERVAL_MS: u64 = 50;
/// Steady sync cadence.
const STEADY_INTERVAL_MS: u64 = 500;
/// How often the sync thread wakes to re-check its target while nothing is
/// selected (Off), so it re-points promptly once a source is chosen.
const IDLE_POLL_MS: u64 = 200;

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
    pub applied_offset_ms: f64,
    pub target_offset_ms: f64,
    pub best_rtt_ms: f64,
    pub last_offset_ms: f64,
    pub last_rtt_ms: f64,
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

    /// Discard all samples and offsets. Called when the sync target switches to
    /// a *different server*, whose clock offset is unrelated to the old one's.
    pub fn reset(&self) {
        let mut s = self.state.lock().unwrap();
        s.samples.clear();
        s.target_offset_ms = 0.0;
        s.applied_offset_ms = 0.0;
        s.last_local_ms = now_epoch_ms() as f64;
        s.initialized = false;
    }

    /// Feed a raw round-trip measurement.
    pub fn add_sample(&self, offset_ms: f64, rtt_ms: f64) {
        let mut s = self.state.lock().unwrap();
        s.samples.push_back(Sample { offset_ms, rtt_ms });
        while s.samples.len() > SAMPLE_WINDOW {
            s.samples.pop_front();
        }

        // Use the offset from the lowest-RTT sample in the window: it is the one
        // least distorted by transient network/queuing delay (a standard NTP
        // heuristic), which keeps the target stable against jitter.
        if let Some(best) = s
            .samples
            .iter()
            .min_by(|a, b| a.rtt_ms.total_cmp(&b.rtt_ms))
            .copied()
        {
            s.target_offset_ms = best.offset_ms;
        }

        // Don't apply an offset (or let playback start) until the full warmup
        // window is collected: a single early sample can be badly skewed, and
        // adopting it would jump playback. Once WARMUP_SAMPLES are in, adopt the
        // best (lowest-RTT) estimate directly, then slew from there.
        if !s.initialized && s.samples.len() >= WARMUP_SAMPLES {
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

/// The unicast address the client currently time-syncs against: the server that
/// owns the selected source. Updated by the selection logic; read by the sync
/// thread. `None` when nothing is selected.
pub struct SyncTarget {
    inner: Mutex<Option<(Ipv4Addr, u16)>>,
}

impl SyncTarget {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(None),
        }
    }
    pub fn set(&self, ip: Ipv4Addr, port: u16) {
        *self.inner.lock().unwrap() = Some((ip, port));
    }
    pub fn clear(&self) {
        *self.inner.lock().unwrap() = None;
    }
    pub fn get(&self) -> Option<(Ipv4Addr, u16)> {
        *self.inner.lock().unwrap()
    }
}

impl Default for SyncTarget {
    fn default() -> Self {
        Self::new()
    }
}

/// Client-owned settings: the single source of truth for this client's selected
/// source, volume, and delay. Written by the control listener (applying UI
/// commands), read by playback, the sync re-pointer, and the telemetry sender.
pub struct ClientSettings {
    inner: Mutex<SettingsInner>,
    /// Signalled when the selected source changes, so the network source's
    /// watcher can re-join the appropriate multicast group.
    cv: Condvar,
}

struct SettingsInner {
    /// 0 = off (play nothing).
    selected_source_id: u64,
    volume: f32,
    delay_ms: u32,
    volume_dirty: bool,
    /// Bumped on every actual selection change; the watcher keys off it.
    selection_epoch: u64,
    /// Send lead (ms) of the currently-selected source: the cap on `delay_ms`,
    /// since a device can't play earlier than the buffered lead. Kept current by
    /// the selection watcher from the catalog.
    active_lead_ms: u32,
    /// Output channel map: one source-channel index per output channel (`-1` =
    /// silence). Empty means the default identity mapping (out i ← src i).
    channel_map: Vec<i16>,
}

impl ClientSettings {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(SettingsInner {
                selected_source_id: 0,
                volume: 1.0,
                delay_ms: 0,
                volume_dirty: true, // apply the initial volume once at startup
                selection_epoch: 0,
                active_lead_ms: MAX_DELAY_MS,
                channel_map: Vec::new(),
            }),
            cv: Condvar::new(),
        }
    }

    /// (selected_source_id, volume, delay_ms) for a telemetry report.
    pub fn report_values(&self) -> (u64, f32, u32) {
        let s = self.inner.lock().unwrap();
        (s.selected_source_id, s.volume, s.delay_ms)
    }

    pub fn selected(&self) -> u64 {
        self.inner.lock().unwrap().selected_source_id
    }

    pub fn delay_ms(&self) -> u32 {
        self.inner.lock().unwrap().delay_ms
    }

    /// Send lead (ms) of the selected source — the total-buffer budget basis.
    pub fn active_lead_ms(&self) -> u32 {
        self.inner.lock().unwrap().active_lead_ms
    }

    /// The current output channel map (empty = default identity mapping).
    pub fn channel_map(&self) -> Vec<i16> {
        self.inner.lock().unwrap().channel_map.clone()
    }

    fn set_channel_map(&self, map: Vec<i16>) {
        self.inner.lock().unwrap().channel_map = map;
    }

    /// The current volume if it changed since the last call, clearing the flag.
    pub fn take_volume_update(&self) -> Option<f32> {
        let mut s = self.inner.lock().unwrap();
        if s.volume_dirty {
            s.volume_dirty = false;
            Some(s.volume)
        } else {
            None
        }
    }

    fn set_volume(&self, value: f32) {
        let mut s = self.inner.lock().unwrap();
        let v = value.clamp(0.0, 1.0);
        if (v - s.volume).abs() > f32::EPSILON {
            s.volume = v;
            s.volume_dirty = true;
        }
    }

    fn set_delay(&self, ms: u32) {
        let mut s = self.inner.lock().unwrap();
        s.delay_ms = ms.min(s.active_lead_ms);
    }

    /// Update the cap on `delay_ms` to the selected source's send lead, and
    /// re-clamp the current delay to it. Called by the selection watcher.
    pub fn set_active_lead(&self, lead_ms: u32) {
        let mut s = self.inner.lock().unwrap();
        s.active_lead_ms = lead_ms;
        if s.delay_ms > lead_ms {
            s.delay_ms = lead_ms;
        }
    }

    /// Select a source (0 = off). Bumps the epoch and wakes the watcher only on
    /// an actual change, so redundant re-sends of a command are idempotent.
    pub fn set_selection(&self, id: u64) {
        let mut s = self.inner.lock().unwrap();
        if s.selected_source_id != id {
            s.selected_source_id = id;
            s.selection_epoch += 1;
            drop(s);
            self.cv.notify_all();
        }
    }

    /// Apply a control command from the UI. Each field is applied only if
    /// present; unchanged values are no-ops (so the 3× re-send is harmless).
    pub fn apply_command(&self, cmd: &ControlCommand) {
        if let Some(v) = cmd.set_volume {
            self.set_volume(v);
        }
        if let Some(d) = cmd.set_delay_ms {
            self.set_delay(d);
        }
        if let Some(id) = cmd.set_source_id {
            self.set_selection(id);
        }
        if let Some(map) = &cmd.set_channel_map {
            self.set_channel_map(map.clone());
        }
    }

    /// Block until the selection epoch differs from `last_epoch` or `timeout`
    /// elapses, then return `(selected_source_id, epoch)`. The timeout lets the
    /// watcher retry resolving a source that wasn't in the catalog yet.
    pub fn wait_selection_change(&self, last_epoch: u64, timeout: Duration) -> (u64, u64) {
        let mut s = self.inner.lock().unwrap();
        if s.selection_epoch == last_epoch {
            let (g, _) = self.cv.wait_timeout(s, timeout).unwrap();
            s = g;
        }
        (s.selected_source_id, s.selection_epoch)
    }
}

impl Default for ClientSettings {
    fn default() -> Self {
        Self::new()
    }
}

/// Client side: repeatedly exchange timestamps with the server that owns the
/// currently-selected source (from `target`) and feed the results into `clock`.
/// When the target server changes, the clock is reset (offsets are per-server).
/// Runs forever; intended for its own thread.
pub fn run_client_sync(
    target: Arc<SyncTarget>,
    clock: Arc<SyncedClock>,
    settings: Arc<ClientSettings>,
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

    let mut nonce: u64 = 0;
    let mut buf = [0u8; 256];
    let mut current_ip: Option<Ipv4Addr> = None;

    loop {
        let dest = match target.get() {
            Some(d) => d,
            None => {
                // Nothing selected: idle (keep the last clock; playback is off).
                current_ip = None;
                std::thread::sleep(Duration::from_millis(IDLE_POLL_MS));
                continue;
            }
        };
        // Switched to a different server: its clock offset is unrelated.
        if current_ip != Some(dest.0) {
            clock.reset();
            current_ip = Some(dest.0);
        }

        nonce = nonce.wrapping_add(1);
        let t1 = now_epoch_ms();
        let req = TimeRequest {
            client_send_ms: t1,
            nonce,
            selected_source_id: settings.selected(),
            channel_map: settings.channel_map(),
        };
        if let Ok(bytes) = bincode::serialize(&req) {
            let _ = sock.send_to(&bytes, dest);
        }

        // Wait for the matching reply (drop stale/mismatched ones).
        if let Ok((n, _)) = sock.recv_from(&mut buf)
            && let Ok(resp) = bincode::deserialize::<TimeResponse>(&buf[..n])
            && resp.nonce == nonce
        {
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

        let interval = if clock.sample_count() < WARMUP_SAMPLES {
            WARMUP_INTERVAL_MS
        } else {
            STEADY_INTERVAL_MS
        };
        std::thread::sleep(Duration::from_millis(interval));
    }
}

/// How long a client counts as a listener of a source after its last time-sync
/// request naming it. A few sync intervals, so a dropped request is tolerated.
const LISTENER_TTL: Duration = Duration::from_secs(3);

/// One listening client: when it was last heard, and the output channel map it
/// reported (empty = default identity / full stream). The map lets the send path
/// stream only the source channels this client actually plays in unicast mode.
struct Listener {
    seen: Instant,
    channel_map: Vec<i16>,
}

/// Which clients are currently listening to each local source, learned from the
/// `selected_source_id` (and channel map) on their time-sync requests. Lets the
/// server stop an unheard source, target unicast mode, and — in unicast mode —
/// send each client only the channels it plays.
pub struct Listeners {
    inner: Mutex<HashMap<u64, HashMap<Ipv4Addr, Listener>>>,
}

impl Listeners {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Record that `ip` is listening to `source_id` (0 = none, ignored), with the
    /// client's current output `channel_map` (empty = identity / full stream).
    pub fn touch(&self, source_id: u64, ip: Ipv4Addr, channel_map: Vec<i16>) {
        if source_id == 0 {
            return;
        }
        self.inner
            .lock()
            .unwrap()
            .entry(source_id)
            .or_default()
            .insert(
                ip,
                Listener {
                    seen: Instant::now(),
                    channel_map,
                },
            );
    }

    /// Current listener IPs for a source (pruned to [`LISTENER_TTL`]).
    pub fn listeners(&self, source_id: u64) -> Vec<Ipv4Addr> {
        let mut map = self.inner.lock().unwrap();
        let now = Instant::now();
        if let Some(set) = map.get_mut(&source_id) {
            set.retain(|_, l| now.duration_since(l.seen) < LISTENER_TTL);
            set.keys().copied().collect()
        } else {
            Vec::new()
        }
    }

    /// Current listeners for a source as `(ip, channel_map)` pairs (pruned to
    /// [`LISTENER_TTL`]). The channel map (empty = full stream) drives per-client
    /// channel subsetting in unicast mode.
    pub fn targets(&self, source_id: u64) -> Vec<(Ipv4Addr, Vec<i16>)> {
        let mut map = self.inner.lock().unwrap();
        let now = Instant::now();
        if let Some(set) = map.get_mut(&source_id) {
            set.retain(|_, l| now.duration_since(l.seen) < LISTENER_TTL);
            set.iter()
                .map(|(ip, l)| (*ip, l.channel_map.clone()))
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Whether any client is currently listening to this source.
    pub fn has_listener(&self, source_id: u64) -> bool {
        !self.listeners(source_id).is_empty()
    }
}

impl Default for Listeners {
    fn default() -> Self {
        Self::new()
    }
}

/// Server side: answer time-sync requests with the current server clock, and
/// record the requester as a listener of the source it named. Runs forever,
/// unconditionally (a client can't start playing without it), on its own thread.
pub fn run_server_responder(sync_port: u16, listeners: Arc<Listeners>) {
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
                    if let std::net::SocketAddr::V4(v4) = from {
                        listeners.touch(req.selected_source_id, *v4.ip(), req.channel_map.clone());
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
