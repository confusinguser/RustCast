//! One source's server-side send path: read a source, packetize, and multicast
//! to that source's group on the shared [`AUDIO_PORT`]. Each source runs this on
//! its own thread with its own timeline anchor and sequence counter, so sources
//! are fully independent — one ending never disturbs the others.

use std::net::{Ipv4Addr, UdpSocket};
use std::sync::Arc;
use std::time::Duration;

use crate::config::SourceKind;
use crate::metrics::ServerMetrics;
use crate::net::set_multicast_if;
use crate::source::librespot::LibrespotSource;
use crate::source::pipe::PipeSource;
use crate::source::{AudioSource, Format};
use crate::wire::{AUDIO_PORT, AudioPacket, TARGET_PCM_BYTES, WireFormat, now_epoch_ms};

/// How far ahead of playback we timestamp packets — the client's jitter-buffer
/// depth. Bigger = more resilient to jitter, more latency.
pub const SEND_LEAD_MS: u64 = 200;
/// Multicast TTL. 1 keeps traffic on the local subnet; raise to route further.
const MULTICAST_TTL: u32 = 1;
/// If the timeline falls this far behind real time, re-anchor it.
const REANCHOR_TOLERANCE_MS: u64 = 60;

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
    }
}

/// Read `kind` and multicast it as timestamped PCM to `group:`[`AUDIO_PORT`].
/// Runs forever; intended for its own thread. On end-of-stream (e.g. a FIFO
/// writer closing) the source is reopened rather than exiting, so it stays
/// available in the catalog.
pub fn run_source_stream(
    source_id: u64,
    name: String,
    group: Ipv4Addr,
    wire_fmt: WireFormat,
    iface: Ipv4Addr,
    kind: SourceKind,
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
    metrics.set_static(fmt.sample_rate, fmt.channels, SEND_LEAD_MS as u32);

    let socket = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) {
        Ok(s) => s,
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
    let dest = (group, AUDIO_PORT);

    // Frames per datagram, sized to keep the PCM payload under TARGET_PCM_BYTES.
    let frames_per_packet = (TARGET_PCM_BYTES / (channels * wire_fmt.bytes_per_sample())).max(1);
    let samples_per_packet = frames_per_packet * channels;

    println!(
        "source '{name}' ({}): {wire_fmt:?} → {}:{AUDIO_PORT} ({frames_per_packet} frames/pkt, {SEND_LEAD_MS}ms lead)",
        kind.type_name(),
        group,
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

            // When this chunk should start playing, and when to put it on the
            // wire (SEND_LEAD_MS earlier). Pace sending to realtime so a bursty
            // source can't outrun the timeline and bloat client buffers.
            let mut offset_ms = total_frames * 1000 / sample_rate;
            let now = now_epoch_ms();
            if start_ms + offset_ms + REANCHOR_TOLERANCE_MS < now {
                start_ms = now;
                total_frames = 0;
                offset_ms = 0;
                metrics.record_reanchor();
            }
            let play_at_ms = start_ms + SEND_LEAD_MS + offset_ms;
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
            match bincode::serialize(&pkt) {
                Ok(bytes) => {
                    if let Err(e) = socket.send_to(&bytes, dest) {
                        eprintln!("source '{name}': send error: {e}");
                    }
                }
                Err(e) => eprintln!("source '{name}': serialize error: {e}"),
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
