//! RustCast client: discover sources from the multicast catalog, play the one
//! selected for it from the web UI, aligned to each packet's play-at timestamp
//! so all clients stay in sync. Reports its telemetry (and its own settings) by
//! multicast to every server; applies control commands addressed to it.
//!
//! Usage: `client [interface-ip]`

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::num::NonZero;
use std::sync::Arc;
use std::thread;

use rodio::SampleRate;
use rodio::cpal::BufferSize;
use rustcast::catalog::{CatalogStore, run_catalog_receiver};
use rustcast::metrics::{
    DeviceMetrics, TelemetryTargets, run_telemetry_ping_listener, run_telemetry_sender,
};
use rustcast::net::bind_reuse;
use rustcast::source::network::NetworkSource;
use rustcast::sync::{ClientSettings, SyncTarget, SyncedClock, run_client_sync};
use rustcast::wire::{ANNOUNCE_PORT, AUDIO_PORT, CONTROL_GROUP, CONTROL_PORT, ControlCommand};

/// How often each client multicasts its telemetry + settings to the servers.
const TELEMETRY_INTERVAL_MS: u64 = 100; // ~10 Hz

// Small cpal device buffer to keep output-side latency low.
const DEVICE_BUFFER_FRAMES: u32 = 512; // ~11ms at 44.1 kHz
// Hard cap on the rodio output-queue depth, in appended blocks. Kept short so
// that raising the delay skips the stream forward rather than piling audio into
// the output queue: once the queue is this deep we drop the new block instead
// of appending it, keeping output latency bounded.
const MAX_QUEUED_BUFFERS: usize = 12;

fn main() {
    // Optional local interface IP for multicast on multi-homed hosts;
    // "0.0.0.0" lets the kernel choose.
    let iface: Ipv4Addr = std::env::args()
        .nth(1)
        .map(|s| s.parse().expect("interface must be an IPv4 address"))
        .unwrap_or(Ipv4Addr::UNSPECIFIED);

    // This client's own IP, used to match control commands and (implicitly) as
    // the telemetry source address servers key on.
    let my_ip = local_ip(iface);
    // Stable identity + default display name reported to servers.
    let mac = primary_mac();
    let host = hostname();
    println!(
        "RustCast client '{host}' on {my_ip} (interface {iface}, mac {})",
        rustcast::clients::mac_hex(mac)
    );

    // Shared client state.
    let settings = Arc::new(ClientSettings::new()); // starts Off (silent)
    let catalog = Arc::new(CatalogStore::new());
    let sync_target = Arc::new(SyncTarget::new());
    let clock = Arc::new(SyncedClock::new());
    let metrics = Arc::new(DeviceMetrics::new());
    // Servers that have pinged us for telemetry; we unicast to them for a grace
    // window after each ping.
    let telemetry_targets = Arc::new(TelemetryTargets::new());

    // Learn the source catalog from every server's announcements.
    {
        let catalog = catalog.clone();
        thread::Builder::new()
            .name("catalog-recv".into())
            .spawn(move || run_catalog_receiver(catalog, iface))
            .expect("spawn catalog receiver");
    }

    // Apply control commands addressed to this client (source/volume/delay).
    {
        let settings = settings.clone();
        thread::Builder::new()
            .name("control".into())
            .spawn(move || run_control_listener(settings, my_ip, iface))
            .expect("spawn control listener");
    }

    // The audio receive socket: bound to the shared audio port; the network
    // source joins/leaves per-source groups on it at runtime.
    let socket = bind_reuse(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, AUDIO_PORT))
        .expect("bind audio socket");

    let mut source = NetworkSource::new(
        socket,
        settings.clone(),
        catalog.clone(),
        sync_target.clone(),
        clock.clone(),
        metrics.clone(),
        iface,
    )
    .expect("start network source");

    // Time-sync against whichever server owns the selected source.
    {
        let clock = clock.clone();
        let metrics = metrics.clone();
        thread::Builder::new()
            .name("time-sync".into())
            .spawn(move || run_client_sync(sync_target, clock, metrics))
            .expect("spawn time-sync");
    }

    // Listen for telemetry-request pings from watching servers.
    {
        let targets = telemetry_targets.clone();
        thread::Builder::new()
            .name("telemetry-ping".into())
            .spawn(move || run_telemetry_ping_listener(targets, iface))
            .expect("spawn telemetry ping listener");
    }

    // Unicast telemetry + settings (~10 Hz) to each server currently pinging us.
    {
        let settings = settings.clone();
        let metrics = metrics.clone();
        let host = host.clone();
        let targets = telemetry_targets.clone();
        thread::Builder::new()
            .name("telemetry".into())
            .spawn(move || {
                run_telemetry_sender(settings, metrics, mac, host, targets, TELEMETRY_INTERVAL_MS)
            })
            .expect("spawn telemetry");
    }

    // Fixed output device; per-block sample-rate/channels are passed to the
    // mixer, which resamples so differing sources play through one sink.
    let handle = rodio::DeviceSinkBuilder::from_default_device()
        .expect("find default output device")
        .with_buffer_size(BufferSize::Fixed(DEVICE_BUFFER_FRAMES))
        .open_stream()
        .expect("open default audio stream");
    let player = rodio::Player::connect_new(&handle.mixer());

    println!("Ready; waiting for a source selection from the web UI.");

    // Tracks whether we've fed anything, so a never-yet-fed empty queue isn't
    // miscounted as an underrun.
    let mut started = false;
    // Runs forever: next_samples() only returns None if the receive socket dies.
    while let Some(chunk) = source.next_samples() {
        if let Some(vol) = source.take_volume_update() {
            player.set_volume(vol);
        }
        if chunk.samples.is_empty() {
            continue;
        }
        let (Some(channels), Some(sample_rate)) = (
            NonZero::new(chunk.channels),
            SampleRate::new(chunk.sample_rate),
        ) else {
            continue; // ignore a malformed format
        };

        let queued = player.len();
        metrics.set_output_queue_len(queued);
        if started && queued == 0 {
            metrics.record_underrun();
        }
        // Drop this block if the output queue is running deep (card slower than
        // the stream) to keep latency bounded.
        if queued > MAX_QUEUED_BUFFERS {
            metrics.record_overrun_drop();
            continue;
        }
        let n = chunk.samples.len();
        player.append(rodio::buffer::SamplesBuffer::new(
            channels,
            sample_rate,
            chunk.samples,
        ));
        metrics.record_append(n);
        started = true;
    }

    eprintln!("Audio receive socket closed; exiting.");
}

/// Determine this host's primary IPv4 address. If an interface was given, use
/// it; otherwise ask the kernel which local address it would use to reach the
/// multicast range (no packet is sent).
fn local_ip(iface: Ipv4Addr) -> Ipv4Addr {
    if iface != Ipv4Addr::UNSPECIFIED {
        return iface;
    }
    let probe = || -> std::io::Result<Ipv4Addr> {
        let s = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
        s.connect((Ipv4Addr::new(239, 255, 42, 100), ANNOUNCE_PORT))?;
        match s.local_addr()? {
            SocketAddr::V4(v4) => Ok(*v4.ip()),
            _ => Ok(Ipv4Addr::UNSPECIFIED),
        }
    };
    probe().unwrap_or(Ipv4Addr::UNSPECIFIED)
}

/// This host's primary NIC MAC (first non-loopback interface with a real MAC).
/// Falls back to a stable hostname-derived pseudo-MAC (locally-administered bit
/// set) if none is found, so distinct hosts still get distinct identities.
fn primary_mac() -> [u8; 6] {
    if let Ok(entries) = std::fs::read_dir("/sys/class/net") {
        let mut names: Vec<String> = entries
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        for name in names {
            if name == "lo" {
                continue;
            }
            if let Ok(addr) = std::fs::read_to_string(format!("/sys/class/net/{name}/address")) {
                if let Some(mac) = parse_mac(addr.trim()) {
                    if mac != [0u8; 6] {
                        return mac;
                    }
                }
            }
        }
    }
    // Fallback: derive from the hostname (FNV-1a), mark locally-administered.
    let h = rustcast::catalog::fnv1a(hostname().as_bytes()).to_le_bytes();
    [(h[0] & 0xfe) | 0x02, h[1], h[2], h[3], h[4], h[5]]
}

fn parse_mac(s: &str) -> Option<[u8; 6]> {
    let mut out = [0u8; 6];
    let mut parts = s.split(':');
    for b in out.iter_mut() {
        *b = u8::from_str_radix(parts.next()?, 16).ok()?;
    }
    if parts.next().is_some() {
        return None;
    }
    Some(out)
}

/// This host's name (the default display name until overridden in the UI).
fn hostname() -> String {
    let mut buf = [0u8; 256];
    let ret = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if ret == 0 {
        let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        String::from_utf8_lossy(&buf[..end]).into_owned()
    } else {
        "device".into()
    }
}

/// Listen for control commands and apply the ones addressed to this client.
fn run_control_listener(settings: Arc<ClientSettings>, my_ip: Ipv4Addr, iface: Ipv4Addr) {
    let sock = match bind_reuse(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, CONTROL_PORT)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("control listener: could not bind {CONTROL_PORT}: {e}");
            return;
        }
    };
    if let Err(e) = sock.join_multicast_v4(&CONTROL_GROUP, &iface) {
        eprintln!("control listener: could not join {CONTROL_GROUP}: {e}");
    }
    let mut buf = [0u8; 2048];
    loop {
        match sock.recv_from(&mut buf) {
            Ok((n, _)) => {
                if let Ok(cmd) = bincode::deserialize::<ControlCommand>(&buf[..n]) {
                    if Ipv4Addr::from(cmd.target_ip) == my_ip {
                        settings.apply_command(&cmd);
                    }
                }
            }
            Err(e) => {
                eprintln!("control listener: recv error: {e}");
                return;
            }
        }
    }
}
