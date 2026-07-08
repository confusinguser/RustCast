//! RustCast server: read an audio source and multicast it as timestamped PCM.
//!
//! Usage: `server [pipe|spotify] [s16|f32]`
//!   - source: `pipe` (default) reads `testfifo`, `spotify` is a Connect device.
//!   - format: wire PCM format, `s16` (default) or `f32`.

use std::net::{Ipv4Addr, UdpSocket};
use std::os::fd::AsRawFd;
use std::process::exit;

use rustcast::api;
use rustcast::metrics::{ServerMetrics, TelemetryStore, run_telemetry_receiver};
use rustcast::source::librespot::LibrespotSource;
use rustcast::source::pipe::PipeSource;
use rustcast::source::{AudioSource, Format};
use rustcast::sync::{ClientRegistry, run_server_responder, run_settings_responder};
use rustcast::wire::{
    AudioPacket, DEFAULT_GROUP, DEFAULT_PORT, DEFAULT_SETTINGS_PORT, DEFAULT_SYNC_PORT,
    DEFAULT_TELEMETRY_PORT, TARGET_PCM_BYTES, WireFormat, now_epoch_ms,
};
use std::sync::Arc;
use std::time::Duration;

/// Port for the HTTP control API + web UI.
const HTTP_PORT: u16 = 8080;

// How far ahead of playback we timestamp packets. This is the client's jitter
// buffer depth: bigger = more resilient to network jitter, more latency.
const SEND_LEAD_MS: u64 = 200;
// Multicast TTL. 1 keeps traffic on the local subnet; raise to route further.
const MULTICAST_TTL: u32 = 1;
// How often the server samples its own send-path metrics into the telemetry
// history, matching the clients' ~10 Hz report rate.
const SERVER_SAMPLE_MS: u64 = 100;

const REANCHOR_TOLERANCE_MS: u64 = 60;

fn main() {
    let mut args = std::env::args().skip(1);
    let source_kind = args.next().unwrap_or_else(|| "pipe".into());
    let format_arg = args.next().unwrap_or_else(|| "s16".into());
    // Optional local interface IP to multicast on. Needed on multi-homed hosts
    // (std::net can't set this); "0.0.0.0" lets the kernel choose.
    let iface: Ipv4Addr = args
        .next()
        .map(|s| s.parse().expect("interface must be an IPv4 address"))
        .unwrap_or(Ipv4Addr::UNSPECIFIED);

    let wire_fmt = WireFormat::parse(&format_arg).unwrap_or_else(|| {
        eprintln!("unknown wire format '{format_arg}' (expected 's16' or 'f32')");
        exit(1);
    });

    // Shared client registry, populated by the time-sync responder and
    // read/written by the HTTP API. Start both before opening the source so the
    // control UI is reachable immediately.
    let registry = Arc::new(ClientRegistry::new());
    // Server send-path metrics and the per-source telemetry history the UI graphs.
    let server_metrics = Arc::new(ServerMetrics::new());
    let telemetry = Arc::new(TelemetryStore::new());
    {
        let reg = registry.clone();
        let store = telemetry.clone();
        let meta = server_metrics.clone();
        std::thread::spawn(move || api::run(reg, store, meta, HTTP_PORT));
    }
    {
        let reg = registry.clone();
        std::thread::spawn(move || run_server_responder(DEFAULT_SYNC_PORT, reg));
    }
    // Answer per-client settings (volume + delay) on their own port, and push
    // changes to clients immediately over the same socket.
    {
        let settings_sock = Arc::new(
            UdpSocket::bind((Ipv4Addr::UNSPECIFIED, DEFAULT_SETTINGS_PORT))
                .expect("bind settings socket"),
        );
        registry.set_push_socket(settings_sock.clone());
        let reg = registry.clone();
        std::thread::spawn(move || run_settings_responder(settings_sock, reg));
    }
    // Receive client telemetry reports into the store.
    {
        let store = telemetry.clone();
        std::thread::spawn(move || run_telemetry_receiver(DEFAULT_TELEMETRY_PORT, store));
    }
    // Sample the server's own send-path metrics into the history at ~10 Hz.
    {
        let store = telemetry.clone();
        let meta = server_metrics.clone();
        std::thread::spawn(move || {
            loop {
                store.push_server(meta.snapshot());
                std::thread::sleep(Duration::from_millis(SERVER_SAMPLE_MS));
            }
        });
    }

    let mut source: Box<dyn AudioSource> = match source_kind.as_str() {
        "pipe" => {
            let format = Format {
                channels: 2,
                sample_rate: 44_100,
            };
            Box::new(PipeSource::open("testfifo", format).expect("open FIFO"))
        }
        "spotify" => {
            Box::new(LibrespotSource::new("RustCast".into()).expect("start Spotify receiver"))
        }
        other => {
            eprintln!("unknown source '{other}' (expected 'pipe' or 'spotify')");
            exit(1);
        }
    };

    let fmt = source.format();
    let channels = fmt.channels as usize;
    let sample_rate = fmt.sample_rate as u64;
    server_metrics.set_static(fmt.sample_rate, fmt.channels, SEND_LEAD_MS as u32);

    // Socket for sending. Bind to an ephemeral port; set the multicast TTL.
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).expect("bind send socket");
    socket
        .set_multicast_ttl_v4(MULTICAST_TTL)
        .expect("set multicast ttl");
    // Deliver our own multicast to listeners on this host too (e.g. a local client).
    socket.set_multicast_loop_v4(true).ok();
    // Pin the outgoing interface if one was given.
    if iface != Ipv4Addr::UNSPECIFIED {
        set_multicast_if(&socket, iface).expect("set multicast interface");
        println!("multicasting via interface {iface}");
    }
    let dest = (DEFAULT_GROUP, DEFAULT_PORT);

    // Frames per datagram, sized to keep the PCM payload under TARGET_PCM_BYTES.
    let frames_per_packet = (TARGET_PCM_BYTES / (channels * wire_fmt.bytes_per_sample())).max(1);
    let samples_per_packet = frames_per_packet * channels;

    println!(
        "Multicasting {source_kind} as {wire_fmt:?} to {}:{} ({frames_per_packet} frames/packet, {SEND_LEAD_MS}ms lead)",
        DEFAULT_GROUP, DEFAULT_PORT
    );

    // Timeline anchor: play_at = start_ms + lead + frames/rate. `start_ms` is
    // re-anchored to real time whenever the source falls behind (a late start
    // like Spotify connecting minutes later, or a pause/gap), so play_at always
    // stays ~lead in the future instead of drifting into the past.
    let mut start_ms = now_epoch_ms();
    let mut seq: u64 = 0;
    let mut total_frames: u64 = 0;
    let mut pending: Vec<f32> = Vec::with_capacity(samples_per_packet * 2);

    loop {
        // If we have pending samples, try to read more with a short timeout.
        // If we don't get any, we flush the ones we already have.
        let have_pending_samples = pending.len() > 0;
        let samples = if have_pending_samples {
            source.next_samples_timeout(Duration::from_millis(20))
        } else {
            source.next_samples()
        };

        match samples {
            Ok(Some(samples)) => pending.extend_from_slice(&samples),
            Ok(None) if !have_pending_samples => break, // source ended
            Ok(None) => pending.clear(), // flush pending samples, None because of timeout
            Err(e) => {
                eprintln!("source error: {e}");
                break;
            }
        }

        while pending.len() > 0 {
            let chunk: Vec<f32> = pending
                .drain(..samples_per_packet.min(pending.len()))
                .collect();

            // When this chunk's audio should start playing, and when we should
            // actually put it on the wire (SEND_LEAD_MS earlier). Pace sending
            // to realtime so a bursty source can't outrun the timeline and
            // bloat client buffers.
            let offset_ms = total_frames * 1000 / sample_rate;
            let now = now_epoch_ms();
            // Re-anchor if the timeline has fallen behind real time (source
            // started late or paused). Without this, play_at would be in the
            // past and clients would drop every packet as "too late".
            if start_ms + offset_ms + REANCHOR_TOLERANCE_MS < now {
                start_ms = now - offset_ms;
                server_metrics.record_reanchor();
            }
            let play_at_ms = start_ms + SEND_LEAD_MS + offset_ms;
            let send_at_ms = start_ms + offset_ms;
            if send_at_ms > now {
                std::thread::sleep(Duration::from_millis(send_at_ms - now));
            }

            let pkt = AudioPacket {
                seq,
                play_at_ms,
                sample_rate: fmt.sample_rate,
                channels: fmt.channels,
                format: wire_fmt,
                data: wire_fmt.encode(&chunk),
            };
            let bytes = bincode::serialize(&pkt).expect("serialize packet");
            if let Err(e) = socket.send_to(&bytes, dest) {
                eprintln!("send error: {e}");
                break;
            }

            seq += 1;
            total_frames += frames_per_packet as u64;
            server_metrics.record_packet_sent(frames_per_packet as u64);
            server_metrics.set_pending_len(pending.len());

            if pending.len() < samples_per_packet {
                break; // try to read more samples before sending the next packet
            }
        }
    }

    println!("Source ended after {seq} packets.");
}

/// Set the outgoing interface for multicast (IP_MULTICAST_IF). `std::net` does
/// not expose this, so we call setsockopt directly.
fn set_multicast_if(socket: &UdpSocket, iface: Ipv4Addr) -> std::io::Result<()> {
    let addr = libc::in_addr {
        // s_addr is in network byte order; octets() are already network order.
        s_addr: u32::from_ne_bytes(iface.octets()),
    };
    let ret = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_IP,
            libc::IP_MULTICAST_IF,
            &addr as *const libc::in_addr as *const libc::c_void,
            std::mem::size_of::<libc::in_addr>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}
