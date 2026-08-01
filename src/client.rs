//! The client playback stack as a library function, so it runs both as the
//! `client` binary and in-process inside a server (`local_client` in the config).
//!
//! It discovers sources from the multicast catalog, plays the one selected for it
//! from the web UI aligned to each packet's play-at timestamp, routes source
//! channels to the output device's channels per the client-owned channel map,
//! and streams telemetry (with its own settings) to every server over TCP.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::num::NonZero;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rodio::SampleRate;
use rodio::cpal::BufferSize;
use rodio::cpal::traits::{DeviceTrait, HostTrait};

use crate::catalog::{CatalogStore, run_catalog_receiver, run_unicast_catalog_client};
use crate::clients::mac_hex;
use crate::metrics::{DeviceMetrics, run_telemetry_sender};
use crate::net::bind_reuse;
use crate::source::network::NetworkSource;
use crate::sync::{ClientSettings, SyncTarget, SyncedClock, run_client_sync};
use crate::wire::{ANNOUNCE_PORT, AUDIO_PORT, CONTROL_GROUP, CONTROL_PORT, ControlCommand};

/// How often each client sends its telemetry + settings to the servers.
const TELEMETRY_INTERVAL_MS: u64 = 100; // ~10 Hz
/// Small cpal device buffer to keep output-side latency low.
const DEVICE_BUFFER_FRAMES: u32 = 512; // ~11ms at 44.1 kHz
/// Headroom added to the source's send lead when computing the total-buffer
/// budget, so steady-state jitter doesn't nuisance-drop.
const BUDGET_MARGIN_MS: u32 = 20;

/// Run the client playback stack. Blocks forever (intended for its own thread /
/// process). `device_id` defaults to the primary NIC MAC in hex; `display_name`
/// defaults to the hostname.
pub fn run_client(
    iface: Ipv4Addr,
    direct_server: Option<Ipv4Addr>,
    device_id: Option<String>,
    display_name: Option<String>,
) {
    // This client's own IP, used to match control commands and (implicitly) as
    // the telemetry source address servers key on.
    let my_ip = local_ip(iface);
    // Stable identity + default display name reported to servers.
    let mac = primary_mac();
    let device_id = device_id.unwrap_or_else(|| mac_hex(mac));
    let host = display_name.unwrap_or_else(hostname);
    println!(
        "RustCast client '{host}' ({device_id}) on {my_ip} (interface {iface}, mac {})",
        mac_hex(mac)
    );

    // Shared client state.
    let settings = Arc::new(ClientSettings::new()); // starts Off (silent)
    let catalog = Arc::new(CatalogStore::new());
    let sync_target = Arc::new(SyncTarget::new());
    let clock = Arc::new(SyncedClock::new());
    let metrics = Arc::new(DeviceMetrics::new());

    // Learn the source catalog from every server's announcements.
    {
        let catalog = catalog.clone();
        thread::Builder::new()
            .name("catalog-recv".into())
            .spawn(move || run_catalog_receiver(catalog, iface))
            .expect("spawn catalog receiver");
    }

    // If pointed at a specific server, also fetch its catalog by unicast (for
    // networks where multicast discovery doesn't route).
    if let Some(server_ip) = direct_server {
        let catalog = catalog.clone();
        thread::Builder::new()
            .name("catalog-unicast".into())
            .spawn(move || run_unicast_catalog_client(server_ip, catalog))
            .expect("spawn unicast catalog client");
    }

    // Apply control commands addressed to this client (source/volume/delay/map).
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

    // Time-sync against whichever server owns the selected source. Also carries
    // our selection to that server so it knows we're listening.
    {
        let clock = clock.clone();
        let metrics = metrics.clone();
        let settings = settings.clone();
        thread::Builder::new()
            .name("time-sync".into())
            .spawn(move || run_client_sync(sync_target, clock, settings, metrics))
            .expect("spawn time-sync");
    }

    // Stream telemetry + settings (~10 Hz) to every server over TCP.
    {
        let settings = settings.clone();
        let metrics = metrics.clone();
        let device_id = device_id.clone();
        let host = host.clone();
        let catalog = catalog.clone();
        thread::Builder::new()
            .name("telemetry".into())
            .spawn(move || {
                run_telemetry_sender(
                    settings,
                    metrics,
                    device_id,
                    mac,
                    host,
                    catalog,
                    TELEMETRY_INTERVAL_MS,
                )
            })
            .expect("spawn telemetry");
    }

    // Detect the output device's channel count; audio is remapped to exactly this
    // width per the channel map, and the count is reported for the UI matrix.
    let out_channels: u16 = rodio::cpal::default_host()
        .default_output_device()
        .and_then(|d| d.default_output_config().ok())
        .map(|c| c.channels())
        .unwrap_or(2)
        .max(1);
    metrics.set_output_channels(out_channels);
    println!("output device has {out_channels} channel(s)");

    // Fixed output device; each appended block is sized to `out_channels` so the
    // per-output channel routing is honored without rodio remixing it.
    let handle = rodio::DeviceSinkBuilder::from_default_device()
        .expect("find default output device")
        .with_buffer_size(BufferSize::Fixed(DEVICE_BUFFER_FRAMES))
        .open_stream()
        .expect("open default audio stream");
    let player = rodio::Player::connect_new(handle.mixer());

    println!("Ready; waiting for a source selection from the web UI.");

    // Tracks whether we've fed anything, so a never-yet-fed empty queue isn't
    // miscounted as an underrun.
    let mut started = false;
    // Runs forever: next_samples() only returns None if the receive socket dies.
    loop {
        let Some(chunk) = source.next_samples_timeout(Duration::from_millis(1000)) else {
            metrics.set_output_queue_len(0);
            continue;
        };
        if let Some(vol) = source.take_volume_update() {
            player.set_volume(vol);
        }
        if chunk.samples.is_empty() {
            continue;
        }
        // Play at the output device's channel width; the source channels are
        // routed into the output channels per the (client-owned) channel map.
        let (Some(channels), Some(sample_rate)) = (
            NonZero::new(out_channels),
            SampleRate::new(chunk.sample_rate),
        ) else {
            continue; // ignore a malformed format
        };

        let queued = player.len();
        metrics.set_output_queue_len(queued);
        if started && queued == 0 {
            metrics.record_underrun();
        }
        // Combined-budget overrun: the jitter buffer and the player queue are one
        // latency budget. Drop when their sum exceeds ~the source's send lead
        // (+ margin), so raising the delay shifts packets between the two buffers
        // rather than growing the total without bound.
        let frames = chunk.samples.len() / chunk.channels.max(1) as usize;
        let pkt_ms = if chunk.sample_rate > 0 {
            (frames as f64 * 1000.0 / chunk.sample_rate as f64).max(1.0)
        } else {
            1.0
        };
        let budget_ms = (settings.active_lead_ms() + BUDGET_MARGIN_MS) as f64;
        let budget_pkts = (budget_ms / pkt_ms).ceil() as usize;
        let total = metrics.jitter_buffer_len() as usize + queued;
        if total > budget_pkts {
            metrics.record_overrun_drop();
            continue;
        }
        let routed = remap_channels(
            &chunk.samples,
            chunk.channels as usize,
            out_channels as usize,
            &settings.channel_map(),
            &chunk.channel_ids,
        );
        let n = routed.len();
        player.append(rodio::buffer::SamplesBuffer::new(
            channels,
            sample_rate,
            routed,
        ));
        metrics.record_append(n);
        started = true;
    }
}

/// Route interleaved `samples` (`src_ch` channels physically present) into a new
/// interleaved buffer of `out_ch` channels. `map` holds one *source*-channel index
/// per output channel (`-1`, or out of range, = silence); an empty `map` is the
/// default identity mapping (output channel i plays source channel i).
///
/// `channel_ids` gives the source-channel index of each channel present in
/// `samples`, in order; empty means the full contiguous stream (present channel i
/// is source channel i). In unicast mode the server may send only the subset of
/// source channels a client plays, so a requested source channel is looked up
/// through `channel_ids` to its physical position (not present ⇒ silence).
fn remap_channels(
    samples: &[f32],
    src_ch: usize,
    out_ch: usize,
    map: &[i16],
    channel_ids: &[u16],
) -> Vec<f32> {
    if src_ch == 0 || out_ch == 0 {
        return Vec::new();
    }
    let frames = samples.len() / src_ch;
    let mut out = vec![0.0f32; frames * out_ch];
    for o in 0..out_ch {
        // Which source channel feeds this output channel.
        let want = if map.is_empty() {
            o as i32 // identity
        } else {
            map.get(o).map(|&s| s as i32).unwrap_or(-1)
        };
        if want < 0 {
            continue; // silence for this output channel
        }
        // Its physical position within `samples`: direct for the full stream, or
        // via `channel_ids` for a subset (absent ⇒ silence).
        let phys = if channel_ids.is_empty() {
            want as usize
        } else {
            match channel_ids.iter().position(|&c| c as i32 == want) {
                Some(i) => i,
                None => continue,
            }
        };
        if phys >= src_ch {
            continue;
        }
        for f in 0..frames {
            out[f * out_ch + o] = samples[f * src_ch + phys];
        }
    }
    out
}

/// Determine this host's primary IPv4 address. If an interface was given, use it;
/// otherwise ask the kernel which local address it would use to reach the
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
            if let Ok(addr) = std::fs::read_to_string(format!("/sys/class/net/{name}/address"))
                && let Some(mac) = parse_mac(addr.trim())
                && mac != [0u8; 6]
            {
                return mac;
            }
        }
    }
    // Fallback: derive from the hostname (FNV-1a), mark locally-administered.
    let h = crate::catalog::fnv1a(hostname().as_bytes()).to_le_bytes();
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
pub fn hostname() -> String {
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
                if let Ok(cmd) = bincode::deserialize::<ControlCommand>(&buf[..n])
                    && Ipv4Addr::from(cmd.target_ip) == my_ip
                {
                    settings.apply_command(&cmd);
                }
            }
            Err(e) => {
                eprintln!("control listener: recv error: {e}");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::remap_channels;

    #[test]
    fn identity_map_passes_through() {
        let s = [1.0, 2.0, 3.0, 4.0]; // 2 frames stereo
        assert_eq!(remap_channels(&s, 2, 2, &[], &[]), s);
    }

    #[test]
    fn swap_and_silence() {
        let s = [1.0, 2.0, 3.0, 4.0];
        assert_eq!(
            remap_channels(&s, 2, 2, &[1, -1], &[]),
            [2.0, 0.0, 4.0, 0.0]
        );
    }

    #[test]
    fn mono_upmixed_to_stereo() {
        let s = [1.0, 2.0, 3.0]; // 3 frames mono
        assert_eq!(
            remap_channels(&s, 1, 2, &[0, 0], &[]),
            [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        );
    }

    #[test]
    fn out_of_range_index_is_silent() {
        let s = [1.0, 2.0]; // 1 frame stereo
        assert_eq!(remap_channels(&s, 2, 2, &[5, 0], &[]), [0.0, 1.0]);
    }

    #[test]
    fn downmix_to_fewer_outputs() {
        let s = [1.0, 2.0, 3.0, 4.0]; // 2 frames stereo
        assert_eq!(remap_channels(&s, 2, 1, &[1], &[]), [2.0, 4.0]);
    }

    #[test]
    fn subset_stream_routed_by_source_index() {
        // A unicast subset packet carrying source channels 2 then 0 (physical
        // order), for a client mapping output0 ← src0, output1 ← src2.
        let s = [10.0, 100.0]; // 1 frame: [src2, src0]
        let ids = [2u16, 0];
        assert_eq!(remap_channels(&s, 2, 2, &[0, 2], &ids), [100.0, 10.0]);
    }

    #[test]
    fn subset_missing_source_is_silent() {
        // Only source channel 0 was sent; an output wanting src1 gets silence.
        let s = [5.0]; // 1 frame, 1 present channel (src0)
        let ids = [0u16];
        assert_eq!(remap_channels(&s, 1, 2, &[0, 1], &ids), [5.0, 0.0]);
    }
}
