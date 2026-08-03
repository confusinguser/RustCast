//! One source's server-side send path: read, packetize, and multicast to that
//! source's group on the shared [`AUDIO_PORT`]. Each source runs this on its own
//! thread with its own timeline anchor and seq counter, so sources are independent.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::net::{Ipv4Addr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;

use crate::config::SourceKind;
use crate::metrics::ServerMetrics;
use crate::net::set_multicast_if;
use crate::source::librespot::LibrespotSource;
use crate::source::pipe::PipeSource;
use crate::source::pulse::{PulseKind, PulseSource};
use crate::source::{AudioSource, Format};
use crate::sync::Listeners;
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
/// A sample at or below this amplitude counts as silence (≈ one s16 LSB).
const SILENCE_EPS: f32 = 1.0 / 32768.0;
/// After this much continuous silence, treat the source as producing no audio
/// and stop sending (matters most for a sink monitor, which streams digital
/// zeros while idle).
const SILENCE_HOLD_MS: u64 = 1000;
/// Max time a source read blocks before the loop re-checks its stop flag, so a
/// hot-removed source is torn down promptly (its `Drop` kills `parec` etc.).
const READ_POLL_MS: u64 = 100;

/// Live, runtime-adjustable send timing for one source, shared between the send
/// loop, the catalog announcer (reads `lead`), and the HTTP API (writes).
pub struct SendParams {
    lead_ms: AtomicU64,
    redundancy: AtomicU32,
    last_lead_ms: AtomicU64,
    /// When true, stream to each listening client's IP by unicast instead of the
    /// multicast group.
    unicast: AtomicBool,
    /// Set to request a manual timeline re-anchor; the send loop consumes it on
    /// its next packet (resetting `start_ms` to now) and clears it.
    reanchor: AtomicBool,
}

impl SendParams {
    pub fn new(lead_ms: u64, redundancy: u32, last_lead_ms: u64, unicast: bool) -> Self {
        let p = Self {
            lead_ms: AtomicU64::new(SEND_LEAD_MS),
            redundancy: AtomicU32::new(1),
            last_lead_ms: AtomicU64::new(0),
            unicast: AtomicBool::new(unicast),
            reanchor: AtomicBool::new(false),
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
        self.lead_ms
            .store(ms.clamp(1, MAX_LEAD_MS), Ordering::Relaxed);
    }
    pub fn set_redundancy(&self, n: u32) {
        self.redundancy
            .store(n.clamp(1, MAX_COPIES), Ordering::Relaxed);
    }
    pub fn set_last_lead(&self, ms: u64) {
        self.last_lead_ms
            .store(ms.min(MAX_LEAD_MS), Ordering::Relaxed);
    }
    pub fn set_unicast(&self, on: bool) {
        self.unicast.store(on, Ordering::Relaxed);
    }
    /// Request a manual timeline re-anchor on the next emitted packet.
    pub fn request_reanchor(&self) {
        self.reanchor.store(true, Ordering::Relaxed);
    }
    /// Consume a pending re-anchor request, returning whether one was set.
    pub fn take_reanchor(&self) -> bool {
        self.reanchor.swap(false, Ordering::Relaxed)
    }
}

/// A scheduled redundant copy: send `bytes` (a serialized packet) to `dest` at
/// wall-clock `at` (epoch ms). Pre-addressed at enqueue time since a client's channel
/// subset makes the payload per-destination. Ordered by `at` only, for the min-heap.
struct Copy {
    at: u64,
    dest: (Ipv4Addr, u16),
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

/// Where a source's packets go, each with the channel subset to send (`None` =
/// full stream). Multicast → the group, full stream. Unicast → each listening
/// client with only the channels its output map plays. Empty ⇒ nobody listening.
fn dests(
    params: &SendParams,
    group: Ipv4Addr,
    source_id: u64,
    listeners: &Listeners,
    src_channels: usize,
) -> Vec<(Ipv4Addr, u16, Option<Vec<u16>>)> {
    let targets = listeners.targets(source_id);
    if targets.is_empty() {
        return Vec::new(); // no listeners: idle source sends nothing
    }
    if params.unicast() {
        targets
            .into_iter()
            .map(|(ip, map)| (ip, AUDIO_PORT, needed_channels(&map, src_channels)))
            .collect()
    } else {
        vec![(group, AUDIO_PORT, None)]
    }
}

/// The distinct source channels a client with output `map` plays, on a source with
/// `src_channels` channels — or `None` to send the full stream.
///
/// `None` when the map is identity (empty) or already covers every source channel:
/// the full contiguous stream is wanted, so those clients (and multicast) share one
/// serialized packet. Otherwise `Some(sorted, distinct, in-range indices)`.
fn needed_channels(map: &[i16], src_channels: usize) -> Option<Vec<u16>> {
    if map.is_empty() || src_channels == 0 {
        return None;
    }
    let mut set: Vec<u16> = Vec::new();
    for &m in map {
        if m >= 0 && (m as usize) < src_channels && !set.contains(&(m as u16)) {
            set.push(m as u16);
        }
    }
    set.sort_unstable();
    // All-silence (empty) or full coverage ⇒ just send the whole stream.
    if set.is_empty() || set.len() == src_channels {
        None
    } else {
        Some(set)
    }
}

/// Extract `subset` source channels from interleaved `full` (`src_channels` wide)
/// into a new interleaved buffer `subset.len()` channels wide, in `subset` order.
fn select_channels(full: &[f32], src_channels: usize, subset: &[u16]) -> Vec<f32> {
    if src_channels == 0 {
        return Vec::new();
    }
    let frames = full.len() / src_channels;
    let width = subset.len();
    let mut out = vec![0.0f32; frames * width];
    for (oi, &c) in subset.iter().enumerate() {
        let c = c as usize;
        if c >= src_channels {
            continue;
        }
        for f in 0..frames {
            out[f * width + oi] = full[f * src_channels + c];
        }
    }
    out
}

/// Read `kind` and stream it as timestamped PCM — to the source's multicast group,
/// or (unicast) to each listening client — but only while someone is listening. On
/// end-of-stream the source is reopened, not exited, so it stays in the catalog. Runs forever.
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
    stop: Arc<AtomicBool>,
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
    // each to its (pre-resolved) destination at its scheduled time, so the
    // read/pacing loop never blocks on them.
    let (tx, rx) = mpsc::channel::<Copy>();
    {
        let socket = socket.clone();
        std::thread::Builder::new()
            .name(format!("retx-{name}"))
            .spawn(move || run_retransmit(socket, rx))
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
    // re-anchored to real time whenever the source falls behind (late start, pause,
    // or gap), so play_at stays ~lead in the future instead of drifting into the past.
    let mut start_ms = now_epoch_ms();
    let mut seq: u64 = 0;
    let mut total_frames: u64 = 0;
    let mut pending: Vec<f32> = Vec::with_capacity(samples_per_packet * 2);
    // Consecutive silent frames seen; drives the silence gate below.
    let mut silent_frames: u64 = 0;
    let silence_hold_frames = sample_rate * SILENCE_HOLD_MS / 1000;

    loop {
        // Stop promptly when hot-removed (drops the source, running its cleanup).
        if stop.load(Ordering::Relaxed) {
            return;
        }

        // Bounded read so the loop re-checks `stop` between chunks. An empty vec
        // means the timeout elapsed with no new data.
        match source.next_samples_timeout(Duration::from_millis(READ_POLL_MS)) {
            Ok(Some(s)) => pending.extend_from_slice(&s),
            // End of stream: reopen so the source stays in the catalog. Reopening
            // a FIFO blocks until a new writer connects (the natural behavior).
            Ok(None) => {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
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

        // Emit only whole packets; keep any sub-packet remainder for the next read.
        while pending.len() >= samples_per_packet {
            let chunk: Vec<f32> = pending.drain(..samples_per_packet).collect();

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
            if params.take_reanchor() || start_ms + offset_ms + REANCHOR_TOLERANCE_MS < now {
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

            // Silence gate: after a sustained run of silent samples, mark the
            // source as producing no audio and stop sending; resumes the instant
            // real audio returns.
            if chunk.iter().all(|s| s.abs() <= SILENCE_EPS) {
                silent_frames = silent_frames.saturating_add(frames_per_packet as u64);
            } else {
                silent_frames = 0;
            }
            let has_audio = silent_frames < silence_hold_frames;
            metrics.set_has_audio(has_audio);

            // Resolve destinations (and, in unicast mode, each client's channel
            // subset). Only put anything on the wire if the source has audio and
            // someone is listening.
            let targets = if has_audio {
                dests(&params, group, source_id, &listeners, channels)
            } else {
                Vec::new()
            };
            if !targets.is_empty() {
                // Serialize once per distinct channel subset — every full-stream
                // client and multicast share the `None` entry. Keyed by subset.
                let mut serialized: HashMap<Option<Vec<u16>>, Arc<Vec<u8>>> = HashMap::new();
                for (ip, port, subset) in &targets {
                    let bytes = match serialized.get(subset) {
                        Some(b) => b.clone(),
                        None => {
                            let (data, ch, ids) = match subset {
                                None => (wire_fmt.encode(&chunk), fmt.channels, Vec::new()),
                                Some(ids) => (
                                    wire_fmt.encode(&select_channels(&chunk, channels, ids)),
                                    ids.len() as u16,
                                    ids.clone(),
                                ),
                            };
                            let pkt = AudioPacket {
                                source_id,
                                seq,
                                play_at_ms,
                                sample_rate: fmt.sample_rate,
                                channels: ch,
                                format: wire_fmt,
                                data,
                                channel_ids: ids,
                            };
                            match bincode::serialize(&pkt) {
                                Ok(b) => {
                                    let b = Arc::new(b);
                                    serialized.insert(subset.clone(), b.clone());
                                    b
                                }
                                Err(e) => {
                                    eprintln!("source '{name}': serialize error: {e}");
                                    continue;
                                }
                            }
                        }
                    };
                    let dest = (*ip, *port);
                    // Copy 0: send now.
                    if let Err(e) = socket.send_to(&bytes, dest) {
                        eprintln!("source '{name}': send error: {e}");
                    }
                    // Copies 1..N: the same packet to this destination, spaced
                    // evenly in time between `lead` (already sent) and `last_lead`,
                    // handed to the retransmit thread. Clients dedup seqs for free.
                    if copies > 1 {
                        for k in 1..copies as u64 {
                            let lead_k = lead as f64
                                - k as f64 * (lead - last_lead) as f64 / (copies as u64 - 1) as f64;
                            let at = (play_at_ms as f64 - lead_k).round() as u64;
                            let _ = tx.send(Copy {
                                at,
                                dest,
                                bytes: bytes.clone(),
                            });
                            metrics.record_copy_sent();
                        }
                    }
                }
                // Count only packets actually sent to a listener; advance seq per
                // emitted packet so a held-back stream resumes gap-free.
                seq += 1;
                metrics.record_packet_sent(frames_per_packet as u64);
            }

            total_frames += frames_per_packet as u64;
            metrics.set_pending_len(pending.len());
        }
    }
}

/// Companion to [`run_source_stream`]: sends scheduled redundant copies from a
/// min-heap to their pre-resolved destinations at their wall-clock times. A listener
/// change within the short redundancy window isn't re-resolved; the next packet picks it up.
fn run_retransmit(socket: Arc<UdpSocket>, rx: mpsc::Receiver<Copy>) {
    let mut heap: BinaryHeap<Reverse<Copy>> = BinaryHeap::new();
    loop {
        // Send everything now due.
        let now = now_epoch_ms();
        while let Some(Reverse(top)) = heap.peek() {
            if top.at <= now {
                let it = heap.pop().unwrap().0;
                let _ = socket.send_to(&it.bytes, it.dest);
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
