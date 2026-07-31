//! One source's server-side send path: read a source, packetize, and multicast
//! to that source's group on the shared [`AUDIO_PORT`]. Each source runs this on
//! its own thread with its own timeline anchor and sequence counter, so sources
//! are fully independent — one ending never disturbs the others.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::net::{Ipv4Addr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;

use crate::config::SourceKind;
use crate::metrics::ServerMetrics;
use crate::net::set_multicast_if;
use crate::sync::Listeners;
use crate::source::librespot::LibrespotSource;
use crate::source::pipe::PipeSource;
use crate::source::pulse::{PulseKind, PulseSource};
use crate::source::{AudioSource, Format};
use crate::wire::{
    AUDIO_PORT, AudioPacket, MAX_LEAD_MS, TARGET_PCM_BYTES, WireFormat, now_epoch_ms,
};

/// Default send lead (client jitter-buffer depth) in ms; the per-source value is
/// runtime-adjustable via [`SendParams`].
pub const SEND_LEAD_MS: u64 = 200;
/// Cap on redundant copies per packet.
pub const MAX_COPIES: u32 = 8;
/// Multicast TTL. 1 keeps traffic on the local subnet; raise to route further.
const MULTICAST_TTL: u32 = 1;
/// If the timeline falls this far behind real time, re-anchor it.
const REANCHOR_TOLERANCE_MS: u64 = 60;

/// Live, runtime-adjustable send timing for one source, shared between the send
/// loop, the catalog announcer (reads `lead`), and the HTTP API (writes).
pub struct SendParams {
    lead_ms: AtomicU64,
    redundancy: AtomicU32,
    last_lead_ms: AtomicU64,
    /// When true, stream to each listening client's IP by unicast instead of the
    /// multicast group.
    unicast: AtomicBool,
}

impl SendParams {
    pub fn new(lead_ms: u64, redundancy: u32, last_lead_ms: u64, unicast: bool) -> Self {
        let p = Self {
            lead_ms: AtomicU64::new(SEND_LEAD_MS),
            redundancy: AtomicU32::new(1),
            last_lead_ms: AtomicU64::new(0),
            unicast: AtomicBool::new(unicast),
        };
        p.set_lead(lead_ms);
        p.set_redundancy(redundancy);
        p.set_last_lead(last_lead_ms);
        p
    }

    pub fn lead(&self) -> u64 {
        self.lead_ms.load(Ordering::Relaxed)
    }
    pub fn redundancy(&self) -> u32 {
        self.redundancy.load(Ordering::Relaxed)
    }
    pub fn last_lead(&self) -> u64 {
        self.last_lead_ms.load(Ordering::Relaxed)
    }
    pub fn unicast(&self) -> bool {
        self.unicast.load(Ordering::Relaxed)
    }

    pub fn set_lead(&self, ms: u64) {
        self.lead_ms.store(ms.clamp(1, MAX_LEAD_MS), Ordering::Relaxed);
    }
    pub fn set_redundancy(&self, n: u32) {
        self.redundancy
            .store(n.clamp(1, MAX_COPIES), Ordering::Relaxed);
    }
    pub fn set_last_lead(&self, ms: u64) {
        self.last_lead_ms.store(ms.min(MAX_LEAD_MS), Ordering::Relaxed);
    }
    pub fn set_unicast(&self, on: bool) {
        self.unicast.store(on, Ordering::Relaxed);
    }
}

/// A scheduled redundant copy: send `bytes` (a serialized packet) at wall-clock
/// `at` (epoch ms). Ordered by `at` only, for the retransmit min-heap.
struct Copy {
    at: u64,
    bytes: Arc<Vec<u8>>,
}
impl PartialEq for Copy {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at
    }
}
impl Eq for Copy {}
impl PartialOrd for Copy {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Copy {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.at.cmp(&other.at)
    }
}

/// Construct an [`AudioSource`] from its config.
pub fn open_source(kind: &SourceKind) -> std::io::Result<Box<dyn AudioSource>> {
    match kind {
        SourceKind::Pipe {
            path,
            channels,
            sample_rate,
            ..
        } => Ok(Box::new(PipeSource::open(
            path,
            Format {
                channels: *channels,
                sample_rate: *sample_rate,
            },
        )?)),
        SourceKind::Spotify { device_name, .. } => {
            Ok(Box::new(LibrespotSource::new(device_name.clone())?))
        }
        SourceKind::Sink {
            device_name,
            channels,
            sample_rate,
            ..
        } => Ok(Box::new(PulseSource::open(
            PulseKind::Sink {
                sink_name: device_name.clone(),
            },
            Format {
                channels: *channels,
                sample_rate: *sample_rate,
            },
        )?)),
        SourceKind::Mic {
            device,
            channels,
            sample_rate,
            ..
        } => Ok(Box::new(PulseSource::open(
            PulseKind::Source {
                device: device.clone(),
            },
            Format {
                channels: *channels,
                sample_rate: *sample_rate,
            },
        )?)),
    }
}

/// Where a source's packets go right now: the multicast group, or (in unicast
/// mode) each currently-listening client. Empty ⇒ nobody listening ⇒ send nothing.
fn dests(
    params: &SendParams,
    group: Ipv4Addr,
    source_id: u64,
    listeners: &Listeners,
) -> Vec<(Ipv4Addr, u16)> {
    let ls = listeners.listeners(source_id);
    if ls.is_empty() {
        return Vec::new(); // no listeners: idle source sends nothing
    }
    if params.unicast() {
        ls.into_iter().map(|ip| (ip, AUDIO_PORT)).collect()
    } else {
        vec![(group, AUDIO_PORT)]
    }
}

/// Read `kind` and stream it as timestamped PCM — to the source's multicast
/// group, or (unicast mode) to each listening client — but only while at least
/// one client is listening. Runs forever; intended for its own thread. On
/// end-of-stream (e.g. a FIFO writer closing) the source is reopened rather than
/// exiting, so it stays available in the catalog.
#[allow(clippy::too_many_arguments)]
pub fn run_source_stream(
    source_id: u64,
    name: String,
    group: Ipv4Addr,
    wire_fmt: WireFormat,
    iface: Ipv4Addr,
    kind: SourceKind,
    params: Arc<SendParams>,
    listeners: Arc<Listeners>,
    metrics: Arc<ServerMetrics>,
) {
    let mut source = match open_source(&kind) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("source '{name}': could not open: {e}");
            return;
        }
    };

    let fmt = source.format();
    let channels = fmt.channels as usize;
    let sample_rate = fmt.sample_rate as u64;
    metrics.set_static(fmt.sample_rate, fmt.channels, params.lead() as u32);

    let socket = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("source '{name}': could not bind send socket: {e}");
            return;
        }
    };
    socket.set_multicast_ttl_v4(MULTICAST_TTL).ok();
    // Deliver our own multicast to listeners on this host too (a local client).
    socket.set_multicast_loop_v4(true).ok();
    if iface != Ipv4Addr::UNSPECIFIED {
        let _ = set_multicast_if(&socket, iface);
    }
    // Redundant copies (copies 1..N) are handed to a companion thread that sends
    // each at its scheduled time, so the read/pacing loop never blocks on them.
    // It resolves destinations live, so it honors unicast mode and idle-stop too.
    let (tx, rx) = mpsc::channel::<Copy>();
    {
        let socket = socket.clone();
        let name = name.clone();
        let params = params.clone();
        let listeners = listeners.clone();
        std::thread::Builder::new()
            .name(format!("retx-{name}"))
            .spawn(move || run_retransmit(socket, group, source_id, params, listeners, rx))
            .expect("spawn retransmit thread");
    }

    // Frames per datagram, sized to keep the PCM payload under TARGET_PCM_BYTES.
    let frames_per_packet = (TARGET_PCM_BYTES / (channels * wire_fmt.bytes_per_sample())).max(1);
    let samples_per_packet = frames_per_packet * channels;

    println!(
        "source '{name}' ({}): {wire_fmt:?} → {}:{AUDIO_PORT} ({frames_per_packet} frames/pkt, {}ms lead)",
        kind.type_name(),
        group,
        params.lead(),
    );

    // Timeline anchor: play_at = start_ms + lead + frames/rate. `start_ms` is
    // re-anchored to real time whenever the source falls behind (a late start
    // like Spotify connecting, or a pause/gap), so play_at always stays ~lead in
    // the future instead of drifting into the past.
    let mut start_ms = now_epoch_ms();
    let mut seq: u64 = 0;
    let mut total_frames: u64 = 0;
    let mut pending: Vec<f32> = Vec::with_capacity(samples_per_packet * 2);

    loop {
        // With pending samples, read with a short timeout; otherwise block.
        let have_pending = !pending.is_empty();
        let samples = if have_pending {
            source.next_samples_timeout(Duration::from_millis(20))
        } else {
            source.next_samples()
        };

        match samples {
            Ok(Some(s)) => pending.extend_from_slice(&s),
            // Timeout with a partial packet buffered: drop the sub-packet remnant.
            Ok(None) if have_pending => pending.clear(),
            // End of stream: reopen so the source stays in the catalog. Reopening
            // a FIFO blocks until a new writer connects (the natural behavior).
            Ok(None) => {
                match open_source(&kind) {
                    Ok(s) => source = s,
                    Err(e) => {
                        eprintln!("source '{name}': reopen failed: {e}");
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
                continue;
            }
            Err(e) => {
                eprintln!("source '{name}': read error: {e}");
                std::thread::sleep(Duration::from_millis(200));
                continue;
            }
        }

        while !pending.is_empty() {
            let chunk: Vec<f32> = pending
                .drain(..samples_per_packet.min(pending.len()))
                .collect();

            // Live send timing. `lead` is when the first copy goes (play - lead);
            // `last_lead` (< lead) is when the last of `copies` copies goes.
            let lead = params.lead();
            let copies = params.redundancy();
            let last_lead = params.last_lead().min(lead.saturating_sub(1));
            metrics.set_static(fmt.sample_rate, fmt.channels, lead as u32);

            // When this chunk should start playing, and when to put its first
            // copy on the wire (lead earlier). Pace sending to realtime so a
            // bursty source can't outrun the timeline and bloat client buffers.
            let mut offset_ms = total_frames * 1000 / sample_rate;
            let now = now_epoch_ms();
            if start_ms + offset_ms + REANCHOR_TOLERANCE_MS < now {
                start_ms = now;
                total_frames = 0;
                offset_ms = 0;
                metrics.record_reanchor();
            }
            let play_at_ms = start_ms + lead + offset_ms;
            let send_at_ms = start_ms + offset_ms;
            if send_at_ms > now {
                std::thread::sleep(Duration::from_millis(send_at_ms - now));
            }

            let pkt = AudioPacket {
                source_id,
                seq,
                play_at_ms,
                sample_rate: fmt.sample_rate,
                channels: fmt.channels,
                format: wire_fmt,
                data: wire_fmt.encode(&chunk),
            };
            // Only put anything on the wire if someone is listening.
            let targets = dests(&params, group, source_id, &listeners);
            if !targets.is_empty() {
                match bincode::serialize(&pkt) {
                    Ok(bytes) => {
                        let bytes = Arc::new(bytes);
                        // Copy 0: send now, to each destination.
                        for d in &targets {
                            if let Err(e) = socket.send_to(&bytes, d) {
                                eprintln!("source '{name}': send error: {e}");
                            }
                        }
                        // Copies 1..N: same packet, spaced evenly in time between
                        // `lead` (already sent) and `last_lead`, handed to the
                        // retransmit thread (which re-resolves destinations at
                        // send time). Clients dedup identical seqs for free.
                        if copies > 1 {
                            for k in 1..copies as u64 {
                                let lead_k = lead as f64
                                    - k as f64 * (lead - last_lead) as f64
                                        / (copies as u64 - 1) as f64;
                                let at = (play_at_ms as f64 - lead_k).round() as u64;
                                let _ = tx.send(Copy {
                                    at,
                                    bytes: bytes.clone(),
                                });
                                metrics.record_copy_sent();
                            }
                        }
                    }
                    Err(e) => eprintln!("source '{name}': serialize error: {e}"),
                }
            }

            seq += 1;
            total_frames += frames_per_packet as u64;
            metrics.record_packet_sent(frames_per_packet as u64);
            metrics.set_pending_len(pending.len());

            if pending.len() < samples_per_packet {
                break; // read more before sending the next packet
            }
        }
    }
}

/// Companion to [`run_source_stream`]: sends scheduled redundant copies at their
/// wall-clock times from a min-heap, re-resolving destinations at send time (so
/// unicast mode and idle-stop apply to copies too). Runs until the sender drops.
fn run_retransmit(
    socket: Arc<UdpSocket>,
    group: Ipv4Addr,
    source_id: u64,
    params: Arc<SendParams>,
    listeners: Arc<Listeners>,
    rx: mpsc::Receiver<Copy>,
) {
    let mut heap: BinaryHeap<Reverse<Copy>> = BinaryHeap::new();
    loop {
        // Send everything now due.
        let now = now_epoch_ms();
        while let Some(Reverse(top)) = heap.peek() {
            if top.at <= now {
                let it = heap.pop().unwrap().0;
                for d in dests(&params, group, source_id, &listeners) {
                    let _ = socket.send_to(&it.bytes, d);
                }
            } else {
                break;
            }
        }
        // Wait until the next copy is due, or a new one arrives.
        let timeout = heap
            .peek()
            .map(|Reverse(c)| Duration::from_millis(c.at.saturating_sub(now_epoch_ms())))
            .unwrap_or(Duration::from_millis(1000));
        match rx.recv_timeout(timeout) {
            Ok(c) => heap.push(Reverse(c)),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
    }
}
