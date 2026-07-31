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
/// How long to wait for a missing in-order packet before treating it as lost.
const JITTER_WAIT_MS: u64 = 0;
/// Upper bound on a single play-at sleep, so a bad clock estimate can't wedge
/// playback for a long time.
const MAX_SLEEP_MS: u64 = 2000;
/// Release each packet to the player this many ms *ahead* of its play time, so
/// the player queue keeps a steady cushion and doesn't underrun on jitter.
const CUSHION_MS: f64 = 60.0;
/// How often the selection watcher re-checks the catalog, so a source selected
/// before its server was heard gets joined once it appears.
const SELECTION_POLL_MS: u64 = 500;

/// A block of decoded samples plus the format they're in — sources can differ,
/// so the format travels with each block (the mixer resamples to the device).
pub struct PlayChunk {
    pub samples: Vec<f32>,
    pub channels: u16,
    pub sample_rate: u32,
}

struct BufState {
    packets: BTreeMap<u64, AudioPacket>,
    /// Next sequence number we expect to play (for the active source).
    next_seq: u64,
    /// After a source switch, adopt the earliest buffered seq on the next pull.
    need_resync: bool,
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
                next_seq: 0,
                need_resync: true,
            }),
            cv: Condvar::new(),
            closed: AtomicBool::new(false),
            active_source_id: AtomicU64::new(0),
        });

        let receiver = {
            let shared = shared.clone();
            let metrics = metrics.clone();
            let socket = socket.clone();
            thread::Builder::new()
                .name("net-recv".into())
                .spawn(move || recv_loop(socket, shared, metrics))?
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

    /// Pull the next in-order packet for the active source, skipping ones lost.
    /// Blocks while there's nothing to play. Returns `None` only if the receiver
    /// socket died.
    fn pull_next(&mut self) -> Option<AudioPacket> {
        let mut bs = self.shared.buf.lock().unwrap();
        loop {
            if self.shared.closed.load(Ordering::Relaxed) {
                return None;
            }
            // Nothing selected (or Off): wait for a selection / packets.
            if self.shared.active_source_id.load(Ordering::Relaxed) == 0 {
                bs = self.shared.cv.wait(bs).unwrap();
                continue;
            }

            // Just switched sources: adopt the earliest buffered seq.
            if bs.need_resync {
                if let Some((&seq, _)) = bs.packets.iter().next() {
                    bs.next_seq = seq;
                    bs.need_resync = false;
                } else {
                    bs = self.shared.cv.wait(bs).unwrap();
                    continue;
                }
            }

            // Discard anything older than what we're waiting for.
            while let Some(&k) = bs.packets.keys().next() {
                if k < bs.next_seq {
                    bs.packets.remove(&k);
                } else {
                    break;
                }
            }

            let want = bs.next_seq;
            if let Some(pkt) = bs.packets.remove(&want) {
                bs.next_seq = want + 1;
                self.metrics.set_jitter_buffer_len(bs.packets.len());
                return Some(pkt);
            }

            match bs.packets.keys().next().copied() {
                // A later packet is here but ours isn't: give it a jitter window,
                // then declare it lost and jump ahead.
                Some(_) => {
                    let (guard, res) = self
                        .shared
                        .cv
                        .wait_timeout(bs, Duration::from_millis(JITTER_WAIT_MS))
                        .unwrap();
                    bs = guard;
                    if res.timed_out() && !bs.packets.contains_key(&bs.next_seq) {
                        if let Some(&next_present) = bs.packets.keys().next() {
                            self.metrics.record_lost(next_present - bs.next_seq);
                            bs.next_seq = next_present;
                        }
                    }
                }
                // Buffer empty: wait. Silence is normal (source paused / off); we
                // never treat it as an error or "offline".
                None => {
                    bs = self.shared.cv.wait(bs).unwrap();
                }
            }
        }
    }

    /// The next block of samples to play, released to the player a cushion ahead
    /// of its play time on the synced server clock. Blocks until due. `None` on
    /// a dead receiver socket or timeout.
    pub fn next_samples_timeout(&mut self, duration: Duration) -> Option<PlayChunk> {
        let start = std::time::Instant::now();
        loop {
            // Don't start (or resume after a server switch) until the clock has a
            // full warmup estimate — playing against a not-yet-settled offset
            // would jump. The receiver keeps filling the jitter buffer meanwhile.
            if !self.clock.is_initialized() {
                if self.shared.closed.load(Ordering::Relaxed) {
                    return None;
                }
                thread::sleep(Duration::from_millis(20));
                continue;
            }

            let pkt = self.pull_next()?;

            let delay = self.settings.delay_ms() as f64;
            // When to hand this packet to the player: CUSHION_MS before its
            // (delay-adjusted) play time, so it waits in the player queue that
            // long — the queue holds ~CUSHION_MS and can't underrun on jitter.
            let target = pkt.play_at_ms as f64 - delay - CUSHION_MS;
            let ahead = target - self.clock.server_now_ms();

            if ahead < -CUSHION_MS {
                // Past its intended play time. Drop it rather than play late
                self.metrics.record_late_drop();
                continue;
            }
            if ahead > 0.0 {
                let sleep_duration = Duration::from_millis((ahead as u64).min(MAX_SLEEP_MS));
                if start.elapsed() + sleep_duration > duration {
                    return None; // timeout reached
                }
                thread::sleep(sleep_duration);
            }

            return Some(PlayChunk {
                samples: pkt.format.decode(&pkt.data),
                channels: pkt.channels,
                sample_rate: pkt.sample_rate,
            });
        }
    }
}

fn recv_loop(socket: Arc<UdpSocket>, shared: Arc<Shared>, metrics: Arc<DeviceMetrics>) {
    let mut buf = [0u8; 65536];
    loop {
        match socket.recv_from(&mut buf) {
            Ok((n, _addr)) => {
                let active = shared.active_source_id.load(Ordering::Relaxed);
                if active == 0 {
                    continue; // nothing selected: ignore all audio
                }
                if let Ok(pkt) = bincode::deserialize::<AudioPacket>(&buf[..n]) {
                    // Reject packets from a source we aren't currently playing
                    // (e.g. lingering datagrams from a group we just left).
                    if pkt.source_id != active {
                        continue;
                    }
                    metrics.record_packet_received();
                    let mut bs = shared.buf.lock().unwrap();
                    bs.packets.insert(pkt.seq, pkt);
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
                    bs.need_resync = true;
                    metrics.set_jitter_buffer_len(0);
                }
                shared.cv.notify_all();
                if let Some(old) = joined_group {
                    if old != group {
                        let _ = socket.leave_multicast_v4(&old, &iface);
                    }
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
                        bs.need_resync = true;
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
