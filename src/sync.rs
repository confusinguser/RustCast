//! NTP-like clock synchronization.
//!
//! Multicast is one-way (server → clients), so to estimate the server's clock a
//! client opens a *unicast* side-channel to the server and does an NTP-style
//! round-trip exchange. [`SyncedClock`] turns those measurements into a smooth,
//! continuously-corrected estimate of the server's clock that playback
//! schedules against — no external NTP daemon required.

use std::collections::VecDeque;
use std::net::{Ipv4Addr, UdpSocket};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::wire::{TimeRequest, TimeResponse, now_epoch_ms};

/// How many recent measurements to keep when choosing the best offset.
const SAMPLE_WINDOW: usize = 16;
/// Maximum rate at which the *applied* offset may move toward the target, as a
/// fraction of real time (0.005 = 0.5%). Keeps the induced tempo/pitch change
/// well below the threshold of perception while still tracking drift.
const MAX_SLEW_FRACTION: f64 = 0.005;

/// Number of quick exchanges to do before settling into the steady interval.
const WARMUP_SAMPLES: usize = 8;
const WARMUP_INTERVAL_MS: u64 = 150;
const STEADY_INTERVAL_MS: u64 = 2000;

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

/// Client side: repeatedly exchange timestamps with the server and feed the
/// results into `clock`. Runs forever; intended to live on its own thread.
pub fn run_client_sync(server_ip: Ipv4Addr, sync_port: u16, clock: Arc<SyncedClock>) {
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

/// Server side: answer time-sync requests with the current server clock. Runs
/// forever; intended to live on its own thread.
pub fn run_server_responder(sync_port: u16) {
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
