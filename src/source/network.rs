//! Client-side playback source: receives multicast [`AudioPacket`]s for the
//! currently-selected source into a sequence-ordered jitter buffer and plays
//! them out aligned to each packet's absolute `play_at` timestamp, measured
//! against a [`SyncedClock`] estimate of the owning server's clock.
//!
//! The client owns its selection (in [`ClientSettings`]); a *watcher* thread
//! reacts to selection changes and to the catalog, joining/leaving the right
//! multicast group and re-pointing the clock sync at the owning server. There is
//! no "server offline" concept: if nothing is selected, or the selected source
//! is silent, playback simply produces nothing and waits.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use crate::catalog::CatalogStore;
use crate::metrics::DeviceMetrics;
use crate::sync::{ClientSettings, SyncTarget, SyncedClock};
use crate::wire::AudioPacket;

/// Cap on buffered packets, so a stalled consumer can't grow memory without
/// bound. At realtime pacing the buffer holds only ~the server's lead.
const MAX_BUFFERED: usize = 2000;
/// How often the selection watcher re-checks the catalog, so a source selected
/// before its server was heard gets joined once it appears.
const SELECTION_POLL_MS: u64 = 500;

/// A block of decoded samples plus the format they're in — sources can differ,
/// so the format travels with each block (the mixer resamples to the device).
pub struct PlayChunk {
    pub samples: Vec<f32>,
    pub channels: u16,
    pub sample_rate: u32,
    /// Absolute play-at time (epoch ms, server clock) of this chunk's first
    /// sample — carried through so the player loop can estimate playback delay.
    pub play_at_ms: u64,
    /// Source-channel index of each channel present in `samples` (empty = the
    /// full contiguous stream). Lets the client route a subset packet — sent in
    /// unicast mode — back to output channels by original source index.
    pub channel_ids: Vec<u16>,
}

impl PlayChunk {
    pub fn duration_ms(&self) -> u64 {
        (self.samples.len() as f64 / self.channels as f64 / self.sample_rate as f64 * 1000.0)
            .round() as u64
    }
}

struct BufState {
    /// Buffered packets keyed by `play_at_ms`. Keying by play time (not seq)
    /// dedups the server's redundant copies for free — they share a play time.
    packets: BTreeMap<u64, AudioPacket>,
    /// The (seq, play_at_ms) of the newest packet seen for the active source.
    /// Used only to detect a timeline shift: a newer seq scheduled *earlier* than
    /// this means the source re-anchored (or its lead shrank), so the buffered
    /// packets from the old timeline can be dropped. `None` until the first
    /// packet after a (re)selection.
    newest: Option<(u64, u64)>,
    /// Highest sequence number already pulled from the buffer. The server sends
    /// each packet several times for redundancy (same seq); once we've taken one
    /// copy, later-arriving copies are ignored silently rather than re-buffered
    /// and counted late. `None` until the first pull after a (re)selection.
    consumed_seq: Option<u64>,
}

struct Shared {
    buf: Mutex<BufState>,
    cv: Condvar,
    closed: AtomicBool,
    /// Source id whose packets we currently accept; 0 = none. Read lock-free by
    /// the receiver to filter, set by the selection watcher.
    active_source_id: AtomicU64,
}

pub struct NetworkSource {
    shared: Arc<Shared>,
    clock: Arc<SyncedClock>,
    settings: Arc<ClientSettings>,
    metrics: Arc<DeviceMetrics>,
    _receiver: thread::JoinHandle<()>,
    _watcher: thread::JoinHandle<()>,
}

impl NetworkSource {
    /// Start receiving on `socket` (bound to the audio port, not yet joined to
    /// any group). Spawns the receiver and the selection watcher; returns at
    /// once (nothing plays until a source is selected).
    pub fn new(
        socket: UdpSocket,
        settings: Arc<ClientSettings>,
        catalog: Arc<CatalogStore>,
        sync_target: Arc<SyncTarget>,
        clock: Arc<SyncedClock>,
        metrics: Arc<DeviceMetrics>,
        iface: Ipv4Addr,
    ) -> std::io::Result<Self> {
        let socket = Arc::new(socket);
        let shared = Arc::new(Shared {
            buf: Mutex::new(BufState {
                packets: BTreeMap::new(),
                newest: None,
                consumed_seq: None,
            }),
            cv: Condvar::new(),
            closed: AtomicBool::new(false),
            active_source_id: AtomicU64::new(0),
        });

        let receiver = {
            let shared = shared.clone();
            let metrics = metrics.clone();
            let socket = socket.clone();
            let clock = clock.clone();
            thread::Builder::new()
                .name("net-recv".into())
                .spawn(move || recv_loop(socket, shared, clock, metrics))?
        };

        let watcher = {
            let shared = shared.clone();
            let settings = settings.clone();
            let metrics = metrics.clone();
            let clock = clock.clone();
            thread::Builder::new()
                .name("net-select".into())
                .spawn(move || {
                    run_selection_watcher(
                        shared,
                        socket,
                        settings,
                        catalog,
                        sync_target,
                        clock,
                        metrics,
                        iface,
                    )
                })?
        };

        Ok(Self {
            shared,
            clock,
            settings,
            metrics,
            _receiver: receiver,
            _watcher: watcher,
        })
    }

    /// A pending volume change from a control command, if any.
    pub fn take_volume_update(&self) -> Option<f32> {
        self.settings.take_volume_update()
    }

    /// Pull the earliest buffered packet for the active source, blocking until one
    /// is available or `timeout` elapses. Returns `None` if the receiver socket
    /// died or nothing arrived in time. Play timing is the caller's job.
    fn pull_next(&mut self, timeout: Duration) -> Option<AudioPacket> {
        let deadline = std::time::Instant::now() + timeout;
        let mut bs = self.shared.buf.lock().unwrap();
        loop {
            if self.shared.closed.load(Ordering::Relaxed) {
                return None;
            }
            // A source is selected and a packet is buffered: take the earliest.
            if self.shared.active_source_id.load(Ordering::Relaxed) != 0
                && let Some((_play_at, pkt)) = bs.packets.pop_first()
            {
                // Remember we've taken this seq, so its redundant copies arriving
                // later are ignored instead of re-buffered and counted late.
                bs.consumed_seq = Some(bs.consumed_seq.map_or(pkt.seq, |c| c.max(pkt.seq)));
                self.metrics.set_jitter_buffer_len(bs.packets.len());
                return Some(pkt);
            }
            // Nothing to play (no selection, or buffer empty). Silence is normal
            // (source paused / off); wait for a packet or the deadline.
            let now = std::time::Instant::now();
            if now >= deadline {
                return None;
            }
            let (guard, _) = self.shared.cv.wait_timeout(bs, deadline - now).unwrap();
            bs = guard;
        }
    }

    /// The next block of samples for the active source: the earliest buffered
    /// packet, decoded, tagged with its `play_at`. This is a plain jitter buffer —
    /// it does no play-time regulation; the player loop decides when (and whether)
    /// to append each chunk against the real output-queue depth. `None` on a dead
    /// socket, or if nothing arrives within `duration`.
    pub fn next_samples_timeout(&mut self, duration: Duration) -> Option<PlayChunk> {
        let start = std::time::Instant::now();
        loop {
            // Don't start (or resume after a server switch) until the clock has a
            // full warmup estimate — playing against a not-yet-settled offset
            // would jump. The receiver also rejects packets until then, so the
            // jitter buffer starts fresh here rather than holding stale ones.
            if !self.clock.is_initialized() {
                if self.shared.closed.load(Ordering::Relaxed) {
                    return None;
                }
                thread::sleep(Duration::from_millis(20));
                continue;
            }

            let remaining = duration.saturating_sub(start.elapsed());
            let pkt = self.pull_next(remaining)?;

            // Cheap pre-decode drop of a packet already wholly in the past (e.g. a
            // backlog after a stall), so we don't decode what can't be played.
            // Partial-late handling — cropping the still-on-time tail — is done by
            // the player loop, which knows the real output-queue depth.
            let delay = self.settings.delay_ms() as f64;
            if (pkt.play_at_ms as f64 - delay + packet_duration_ms(&pkt))
                < self.clock.server_now_ms()
            {
                self.metrics.record_late_drop();
                continue;
            }

            return Some(PlayChunk {
                samples: pkt.format.decode(&pkt.data),
                channels: pkt.channels,
                sample_rate: pkt.sample_rate,
                play_at_ms: pkt.play_at_ms,
                channel_ids: pkt.channel_ids,
            });
        }
    }
}

/// Duration in ms of a packet's audio, computed from its encoded size without
/// decoding it (bytes ÷ bytes-per-frame ÷ rate).
fn packet_duration_ms(pkt: &AudioPacket) -> f64 {
    let channels = pkt.channels.max(1) as usize;
    let bytes_per_frame = pkt.format.bytes_per_sample() * channels;
    let frames = pkt.data.len().checked_div(bytes_per_frame).unwrap_or(0);
    frames as f64 * 1000.0 / pkt.sample_rate.max(1) as f64
}

fn recv_loop(
    socket: Arc<UdpSocket>,
    shared: Arc<Shared>,
    clock: Arc<SyncedClock>,
    metrics: Arc<DeviceMetrics>,
) {
    let mut buf = [0u8; 65536];
    loop {
        match socket.recv_from(&mut buf) {
            Ok((n, _addr)) => {
                let active = shared.active_source_id.load(Ordering::Relaxed);
                if active == 0 {
                    continue; // nothing selected: ignore all audio
                }
                // Don't buffer anything until the clock warmup is complete: with
                // no trustworthy play-at reference yet, these packets would only
                // pile up and be dropped/late once playback starts. Reject them so
                // the jitter buffer starts fresh at warmup, aligned to the clock.
                if !clock.is_initialized() {
                    continue;
                }
                if let Ok(pkt) = bincode::deserialize::<AudioPacket>(&buf[..n]) {
                    // Reject packets from a source we aren't currently playing
                    // (e.g. lingering datagrams from a group we just left).
                    if pkt.source_id != active {
                        continue;
                    }
                    metrics.record_packet_received();
                    let mut bs = shared.buf.lock().unwrap();
                    // Redundant copy (or straggler) of a packet we've already taken
                    // from the buffer: we're fine, just ignore it — don't re-buffer
                    // it and don't count it late.
                    if bs.consumed_seq.is_some_and(|cs| pkt.seq <= cs) {
                        continue;
                    }
                    // Timeline-shift detection: a packet with a newer seq but an
                    // earlier play time than the newest we've seen means the
                    // source re-anchored (or its lead shrank). Every buffered
                    // packet scheduled at or after this new time belongs to the
                    // abandoned timeline and would play out of order — drop them.
                    if let Some((newest_seq, newest_play_at)) = bs.newest
                        && pkt.seq > newest_seq
                        && pkt.play_at_ms < newest_play_at
                    {
                        bs.packets.split_off(&pkt.play_at_ms);
                    }
                    if bs.newest.is_none_or(|(seq, _)| pkt.seq >= seq) {
                        bs.newest = Some((pkt.seq, pkt.play_at_ms));
                    }
                    bs.packets.insert(pkt.play_at_ms, pkt);
                    while bs.packets.len() > MAX_BUFFERED {
                        if let Some(&oldest) = bs.packets.keys().next() {
                            bs.packets.remove(&oldest);
                        }
                    }
                    metrics.set_jitter_buffer_len(bs.packets.len());
                    shared.cv.notify_one();
                }
            }
            Err(e) => {
                eprintln!("network receive error: {e}");
                shared.closed.store(true, Ordering::Relaxed);
                shared.cv.notify_all();
                return;
            }
        }
    }
}

/// Reacts to selection changes (and to the catalog catching up): joins the
/// selected source's multicast group, resets the jitter buffer, re-points the
/// clock sync at the owning server, and leaves the old group. Runs forever.
#[allow(clippy::too_many_arguments)]
fn run_selection_watcher(
    shared: Arc<Shared>,
    socket: Arc<UdpSocket>,
    settings: Arc<ClientSettings>,
    catalog: Arc<CatalogStore>,
    sync_target: Arc<SyncTarget>,
    clock: Arc<SyncedClock>,
    metrics: Arc<DeviceMetrics>,
    iface: Ipv4Addr,
) {
    let mut epoch: u64 = 0;
    let mut joined_group: Option<Ipv4Addr> = None;
    let mut active_id: u64 = 0;
    // The server whose clock the current selection is timed against. When it
    // changes, the clock offset is unrelated, so reset immediately (rather than
    // waiting for the sync thread to notice) to avoid a stale-offset pile-up.
    let mut active_server: Option<Ipv4Addr> = None;

    loop {
        // Wake on a selection change, or periodically to retry resolution of a
        // source whose server hadn't been heard from yet.
        let (selected, new_epoch) =
            settings.wait_selection_change(epoch, Duration::from_millis(SELECTION_POLL_MS));
        epoch = new_epoch;

        let resolved = if selected == 0 {
            None
        } else {
            catalog.resolve(selected)
        };

        match resolved {
            Some(rs) => {
                let group = Ipv4Addr::from(rs.entry.group);
                // Already playing this exact source: just refresh the sync target
                // and the delay cap (the source's lead may have changed live).
                if active_id == selected && joined_group == Some(group) {
                    sync_target.set(rs.server_ip, rs.sync_port);
                    settings.set_active_lead(rs.entry.lead_ms);
                    continue;
                }
                // Join the new group *before* switching so we don't miss its start.
                if joined_group != Some(group) {
                    let _ = socket.join_multicast_v4(&group, &iface);
                }
                // Switching to a different server: its clock offset differs, so
                // drop the old estimate now (playback plays arrival-paced until
                // the first sample for the new server arrives).
                if active_server != Some(rs.server_ip) {
                    clock.reset();
                    active_server = Some(rs.server_ip);
                }
                shared.active_source_id.store(selected, Ordering::Relaxed);
                {
                    let mut bs = shared.buf.lock().unwrap();
                    bs.packets.clear();
                    bs.newest = None;
                    bs.consumed_seq = None;
                    metrics.set_jitter_buffer_len(0);
                }
                shared.cv.notify_all();
                if let Some(old) = joined_group
                    && old != group
                {
                    let _ = socket.leave_multicast_v4(&old, &iface);
                }
                joined_group = Some(group);
                active_id = selected;
                sync_target.set(rs.server_ip, rs.sync_port);
                settings.set_active_lead(rs.entry.lead_ms);
                metrics.set_format(rs.entry.sample_rate, rs.entry.channels);
                println!("playing '{}' from {}", rs.entry.name, rs.server_ip);
            }
            None => {
                // Off, or the selected id isn't in the catalog (yet). Go silent.
                // If still selecting an unresolved id, the poll retries resolution.
                if active_id != 0 || joined_group.is_some() {
                    shared.active_source_id.store(0, Ordering::Relaxed);
                    {
                        let mut bs = shared.buf.lock().unwrap();
                        bs.packets.clear();
                        bs.newest = None;
                        bs.consumed_seq = None;
                        metrics.set_jitter_buffer_len(0);
                    }
                    shared.cv.notify_all();
                    if let Some(old) = joined_group {
                        let _ = socket.leave_multicast_v4(&old, &iface);
                    }
                    joined_group = None;
                    active_id = 0;
                    active_server = None;
                    sync_target.clear();
                }
            }
        }
    }
}
