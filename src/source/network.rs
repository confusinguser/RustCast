//! Client-side source: receives multicast [`AudioPacket`]s into a
//! sequence-ordered jitter buffer and plays them out aligned to each packet's
//! absolute `play_at` timestamp, measured against a [`SyncedClock`] estimate of
//! the server's clock.
//!
//! Three threads cooperate: a receiver fills the buffer and learns the server's
//! address; a time-sync thread keeps the clock estimate current; and the
//! calling thread runs `next_samples()`, which pulls packets in order and
//! sleeps until each one's play-at time on the synced clock.

use std::collections::BTreeMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::metrics::DeviceMetrics;
use crate::sync::{SyncedClock, VolumeCell, run_client_sync, run_settings_client};
use crate::wire::{AudioPacket, DEFAULT_SETTINGS_PORT, DEFAULT_SYNC_PORT, now_epoch_ms};

use super::Format;

/// Cap on buffered packets, so a stalled consumer can't grow memory without
/// bound. At realtime pacing the buffer holds only ~the server's lead.
const MAX_BUFFERED: usize = 2000;
/// How long to wait for a missing in-order packet before treating it as lost.
const JITTER_WAIT_MS: u64 = 30;
/// Upper bound on a single play-at sleep, so a bad clock estimate can't wedge
/// playback for a long time.
const MAX_SLEEP_MS: u64 = 2000;
/// How long `new()` waits for the first clock estimate before starting anyway
/// (falling back to a zero offset, i.e. assuming already-synced clocks).
const SYNC_WARMUP_TIMEOUT_MS: u64 = 1500;
/// If a packet is already this many ms past its (delay-adjusted) play time, we
/// drop it rather than play it late. This is what advances our stream position
/// so a delay increase actually plays earlier, and it bounds output latency
/// against clock drift. A hair above one packet, so steady-state pacing (which
/// never runs late) doesn't nuisance-drop.
const LATE_DROP_MS: f64 = 8.0;
/// If no packet arrives for this long, treat the server as offline: playback
/// stops (next_samples returns None) so the caller can wait for a new server.
const SERVER_TIMEOUT_MS: u64 = 5000;
/// How often to re-check for the server going silent while the buffer is empty.
const SERVER_POLL_MS: u64 = 250;

struct Shared {
    buf: Mutex<BTreeMap<u64, AudioPacket>>,
    cv: Condvar,
    closed: AtomicBool,
    /// Source address of the multicast stream, learned from the first datagram.
    /// A standalone `Arc` so the telemetry sender can address the server after
    /// the source itself has been moved into the playback loop.
    server_ip: Arc<Mutex<Option<Ipv4Addr>>>,
    /// Wall-clock (epoch ms) of the most recent received packet, for detecting
    /// that the server has gone silent.
    last_packet_ms: AtomicU64,
}

pub struct NetworkSource {
    shared: Arc<Shared>,
    clock: Arc<SyncedClock>,
    volume: Arc<VolumeCell>,
    /// Playback advance in ms: each packet is played this much earlier than its
    /// `play_at`, to compensate for this device's speaker latency.
    delay_ms: Arc<AtomicU32>,
    /// Live buffer/sample metrics for this device, shared with the playback loop.
    metrics: Arc<DeviceMetrics>,
    format: Format,
    /// Next sequence number we expect to play.
    next_seq: u64,
    _receiver: thread::JoinHandle<()>,
    _sync: Option<thread::JoinHandle<()>>,
}

impl NetworkSource {
    /// Start receiving on `socket` (already bound and joined to the group).
    /// Blocks until the first packet arrives (to learn the stream format) and,
    /// briefly, until an initial clock estimate is available.
    pub fn new(socket: UdpSocket, metrics: Arc<DeviceMetrics>) -> io::Result<Self> {
        let shared = Arc::new(Shared {
            buf: Mutex::new(BTreeMap::new()),
            cv: Condvar::new(),
            closed: AtomicBool::new(false),
            server_ip: Arc::new(Mutex::new(None)),
            last_packet_ms: AtomicU64::new(now_epoch_ms()),
        });
        let clock = Arc::new(SyncedClock::new());
        let volume = Arc::new(VolumeCell::new());
        let delay_ms = Arc::new(AtomicU32::new(0));

        let receiver = {
            let shared = shared.clone();
            let metrics = metrics.clone();
            thread::Builder::new()
                .name("net-recv".into())
                .spawn(move || recv_loop(socket, shared, metrics))?
        };

        // Wait for the first packet so we can report the stream's format.
        let first = {
            let mut buf = shared.buf.lock().unwrap();
            loop {
                if let Some((_, pkt)) = buf.iter().next() {
                    break pkt.clone();
                }
                if shared.closed.load(Ordering::Relaxed) {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "receiver closed before any packet arrived",
                    ));
                }
                buf = shared.cv.wait(buf).unwrap();
            }
        };

        // Start the time-sync and settings threads now that we know the server.
        let server_ip = *shared.server_ip.lock().unwrap();
        let sync = server_ip.map(|ip| {
            let clock = clock.clone();
            let metrics = metrics.clone();
            thread::Builder::new()
                .name("time-sync".into())
                .spawn(move || run_client_sync(ip, DEFAULT_SYNC_PORT, clock, metrics))
                .expect("spawn time-sync thread")
        });
        // Volume + delay arrive on their own channel, separate from time-sync.
        if let Some(ip) = server_ip {
            let volume = volume.clone();
            let delay_ms = delay_ms.clone();
            thread::Builder::new()
                .name("settings".into())
                .spawn(move || run_settings_client(ip, DEFAULT_SETTINGS_PORT, volume, delay_ms))
                .expect("spawn settings thread");
        }
        if server_ip.is_none() {
            eprintln!("warning: could not determine server address; playing without clock sync");
        }

        // Give the sync a moment to converge before playback leans on it.
        let deadline = Instant::now() + Duration::from_millis(SYNC_WARMUP_TIMEOUT_MS);
        while !clock.is_initialized() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }

        metrics.set_format(first.sample_rate, first.channels);

        Ok(Self {
            shared,
            clock,
            volume,
            delay_ms,
            metrics,
            format: Format {
                channels: first.channels,
                sample_rate: first.sample_rate,
            },
            next_seq: first.seq,
            _receiver: receiver,
            _sync: sync,
        })
    }

    /// A handle to the learned server address, for the telemetry sender. The
    /// value is `None` until the first datagram arrives.
    pub fn server_ip_handle(&self) -> Arc<Mutex<Option<Ipv4Addr>>> {
        self.shared.server_ip.clone()
    }

    /// Pull the next in-order packet, skipping ones detected as lost. Blocks
    /// while the buffer is empty. Returns `None` only if the receiver closed
    /// and the buffer is drained.
    fn pull_next(&mut self) -> Option<AudioPacket> {
        let mut buf = self.shared.buf.lock().unwrap();
        loop {
            // Discard anything older than what we're waiting for (late arrivals
            // for packets we've already played or skipped).
            while let Some(&k) = buf.keys().next() {
                if k < self.next_seq {
                    buf.remove(&k);
                } else {
                    break;
                }
            }

            if let Some(pkt) = buf.remove(&self.next_seq) {
                self.next_seq += 1;
                self.metrics.set_jitter_buffer_len(buf.len());
                return Some(pkt);
            }

            match buf.keys().next().copied() {
                // A later packet is here but the one we want isn't: give it a
                // short window (jitter), then declare it lost and jump ahead.
                Some(_) => {
                    let (guard, res) = self
                        .shared
                        .cv
                        .wait_timeout(buf, Duration::from_millis(JITTER_WAIT_MS))
                        .unwrap();
                    buf = guard;
                    if res.timed_out() && !buf.contains_key(&self.next_seq) {
                        if let Some(&next_present) = buf.keys().next() {
                            // Every seq we skip over is a packet that never
                            // arrived in time - count them as lost.
                            self.metrics.record_lost(next_present - self.next_seq);
                            self.next_seq = next_present;
                        }
                    }
                }
                // Buffer empty: wait for more, unless the stream has ended or
                // the server has gone silent for SERVER_TIMEOUT_MS.
                None => {
                    if self.shared.closed.load(Ordering::Relaxed) {
                        return None;
                    }
                    let idle_ms = now_epoch_ms()
                        .saturating_sub(self.shared.last_packet_ms.load(Ordering::Relaxed));
                    if idle_ms > SERVER_TIMEOUT_MS {
                        // Nothing from the server for a while: report end-of-stream
                        // so the caller can wait for a new server.
                        return None;
                    }
                    let (guard, _) = self
                        .shared
                        .cv
                        .wait_timeout(buf, Duration::from_millis(SERVER_POLL_MS))
                        .unwrap();
                    buf = guard;
                }
            }
        }
    }
}

impl NetworkSource {
    /// After the server has gone silent, block until a new stream appears and
    /// resync to it: drop any stale buffered packets and adopt the sequence
    /// number of the first packet from the (possibly new) server.
    pub fn wait_for_new_server(&mut self) {
        let mut buf = self.shared.buf.lock().unwrap();
        buf.clear();
        loop {
            if let Some((&seq, _)) = buf.iter().next() {
                self.next_seq = seq;
                self.metrics.set_jitter_buffer_len(buf.len());
                return;
            }
            if self.shared.closed.load(Ordering::Relaxed) {
                return;
            }
            buf = self.shared.cv.wait(buf).unwrap();
        }
    }

    pub fn format(&self) -> Format {
        self.format
    }

    /// A pending server-assigned volume change (linear gain), if any.
    pub fn take_volume_update(&mut self) -> Option<f32> {
        self.volume.take_update()
    }

    /// The next block of samples to play, or `None` once the stream ends.
    /// Blocks until each packet is due on the synced server clock.
    pub fn next_samples(&mut self) -> Option<Vec<f32>> {
        let delay = self.delay_ms.load(Ordering::Relaxed) as f64;
        loop {
            let pkt = self.pull_next()?;

            // Target wall-clock time (on the synced server clock) to hand this
            // packet to the device. The per-device delay pulls it earlier to
            // compensate for local speaker latency.
            let target = pkt.play_at_ms as f64 - delay;
            let ahead = target - self.clock.server_now_ms();

            if ahead < -LATE_DROP_MS {
                // Already past its play time: drop it and advance to the next
                // packet. This keeps the output queue near-empty (so append
                // time ≈ output time) and is what makes a larger delay actually
                // skip the stream forward and play earlier.
                self.metrics.record_late_drop();
                continue;
            }

            if ahead > 0.0 {
                // Early: wait until it's due. Waiting past what's buffered lets
                // the queue drain (brief silence) — which is how a *smaller*
                // delay plays later.
                thread::sleep(Duration::from_millis((ahead as u64).min(MAX_SLEEP_MS)));
            }

            return Some(pkt.format.decode(&pkt.data));
        }
    }
}

fn recv_loop(socket: UdpSocket, shared: Arc<Shared>, metrics: Arc<DeviceMetrics>) {
    // Max IPv4 UDP payload; our packets are far smaller but this is safe.
    let mut buf = [0u8; 65536];
    loop {
        match socket.recv_from(&mut buf) {
            Ok((n, addr)) => {
                if let Ok(pkt) = bincode::deserialize::<AudioPacket>(&buf[..n]) {
                    metrics.record_packet_received();
                    shared
                        .last_packet_ms
                        .store(now_epoch_ms(), Ordering::Relaxed);
                    // Learn the server's address from the first datagram.
                    if let SocketAddr::V4(v4) = addr {
                        let mut ip = shared.server_ip.lock().unwrap();
                        if ip.is_none() {
                            *ip = Some(*v4.ip());
                        }
                    }

                    let mut map = shared.buf.lock().unwrap();
                    map.insert(pkt.seq, pkt);
                    while map.len() > MAX_BUFFERED {
                        if let Some(&oldest) = map.keys().next() {
                            map.remove(&oldest);
                        }
                    }
                    metrics.set_jitter_buffer_len(map.len());
                    shared.cv.notify_one();
                }
                // Malformed datagram: ignore and keep listening.
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
