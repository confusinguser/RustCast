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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::sync::{SyncedClock, run_client_sync};
use crate::wire::{AudioPacket, DEFAULT_SYNC_PORT};

use super::{AudioSource, Format};

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

struct Shared {
    buf: Mutex<BTreeMap<u64, AudioPacket>>,
    cv: Condvar,
    closed: AtomicBool,
    /// Source address of the multicast stream, learned from the first datagram.
    server_ip: Mutex<Option<Ipv4Addr>>,
}

pub struct NetworkSource {
    shared: Arc<Shared>,
    clock: Arc<SyncedClock>,
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
    pub fn new(socket: UdpSocket) -> io::Result<Self> {
        let shared = Arc::new(Shared {
            buf: Mutex::new(BTreeMap::new()),
            cv: Condvar::new(),
            closed: AtomicBool::new(false),
            server_ip: Mutex::new(None),
        });
        let clock = Arc::new(SyncedClock::new());

        let receiver = {
            let shared = shared.clone();
            thread::Builder::new()
                .name("net-recv".into())
                .spawn(move || recv_loop(socket, shared))?
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

        // Start the time-sync thread now that we know the server's address.
        let server_ip = *shared.server_ip.lock().unwrap();
        let sync = server_ip.map(|ip| {
            let clock = clock.clone();
            thread::Builder::new()
                .name("time-sync".into())
                .spawn(move || run_client_sync(ip, DEFAULT_SYNC_PORT, clock))
                .expect("spawn time-sync thread")
        });
        if server_ip.is_none() {
            eprintln!("warning: could not determine server address; playing without clock sync");
        }

        // Give the sync a moment to converge before playback leans on it.
        let deadline = Instant::now() + Duration::from_millis(SYNC_WARMUP_TIMEOUT_MS);
        while !clock.is_initialized() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }

        Ok(Self {
            shared,
            clock,
            format: Format {
                channels: first.channels,
                sample_rate: first.sample_rate,
            },
            next_seq: first.seq,
            _receiver: receiver,
            _sync: sync,
        })
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
                            self.next_seq = next_present;
                        }
                    }
                }
                // Buffer empty: wait for more, unless the stream has ended.
                None => {
                    if self.shared.closed.load(Ordering::Relaxed) {
                        return None;
                    }
                    buf = self.shared.cv.wait(buf).unwrap();
                }
            }
        }
    }
}

impl AudioSource for NetworkSource {
    fn format(&self) -> Format {
        self.format
    }

    fn next_samples(&mut self) -> io::Result<Option<Vec<f32>>> {
        let pkt = match self.pull_next() {
            Some(p) => p,
            None => return Ok(None),
        };

        // Hold the packet until its scheduled play time, measured on the synced
        // server clock. Because we keep the output queue near-empty, appending
        // at ~play_at means it plays at ~play_at on every client.
        let server_now = self.clock.server_now_ms();
        let wait = pkt.play_at_ms as f64 - server_now;
        if wait > 0.0 {
            let wait_ms = (wait as u64).min(MAX_SLEEP_MS);
            thread::sleep(Duration::from_millis(wait_ms));
        }

        Ok(Some(pkt.format.decode(&pkt.data)))
    }
}

fn recv_loop(socket: UdpSocket, shared: Arc<Shared>) {
    // Max IPv4 UDP payload; our packets are far smaller but this is safe.
    let mut buf = [0u8; 65536];
    loop {
        match socket.recv_from(&mut buf) {
            Ok((n, addr)) => {
                if let Ok(pkt) = bincode::deserialize::<AudioPacket>(&buf[..n]) {
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
