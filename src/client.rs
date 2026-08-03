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
/// Target depth of the realtime player queue. This is the *only* thing the
/// cushion controls: how much decoded audio sits in the output queue (for jitter
/// absorption on the output side). The rest of the delay budget stays in the
/// network jitter buffer. The total latency is fixed by the delay setting, not
/// by this value.
const TARGET_PLAY_QUEUE_MS: f64 = 60.0;
/// A chunk within this many ms of its target play time is treated as on time.
const LATE_LEEWAY_MS: f64 = 3.0;
/// Cap on a single scheduling sleep, so a bad clock estimate can't wedge the
/// player loop for long.
const MAX_SLEEP_MS: f64 = 2000.0;

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
    // Server-clock time (ms) at which the audio currently in the player queue will
    // finish playing — i.e. when a freshly appended chunk would begin. `None` when
    // the queue is (or has drained) empty. This is our model of the output queue,
    // used to place each chunk so it plays at exactly its target time and to keep
    // the queue near CUSHION_MS deep.
    let mut queue_end_ms: Option<f64> = None;
    // Runs forever: next_samples() only returns None if the receive socket dies.
    loop {
        let Some(mut chunk) = source.next_samples_timeout(Duration::from_millis(20)) else {
            metrics.set_output_queue_len(0);
            queue_end_ms = None;
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
        // The player queue is the ground truth: if it has fully drained, our model
        // of its end is stale — reset it so the next chunk (re)anchors playback.
        if queued == 0 {
            queue_end_ms = None;
        }

        let src_channels = chunk.channels.max(1) as usize;
        let frames = chunk.samples.len() / src_channels;
        let chunk_ms = if chunk.sample_rate > 0 {
            frames as f64 * 1000.0 / chunk.sample_rate as f64
        } else {
            0.0
        };
        // Report the per-packet duration so the UI can show buffer depths in ms.
        metrics.set_packet_ms(chunk_ms.max(1.0));

        // Decide when (and whether) to append this chunk so its first sample plays
        // at exactly `play_at - delay`, keeping the player queue ~CUSHION_MS deep.
        let now = clock.server_now_ms();
        let target = chunk.play_at_ms as f64 - settings.delay_ms() as f64;
        let sched = schedule_chunk(now, target, chunk_ms, queue_end_ms);
        if sched.wait_ms > 1.0 {
            thread::sleep(Duration::from_millis(sched.wait_ms as u64));
        }
        if sched.drop {
            // Wholly late (e.g. delay was lowered, or a slow arrival): shed it and
            // let the queue drain toward the new, shorter target. `queue_end_ms`
            // stays as-is — the already-queued audio is untouched.
            metrics.record_late_drop();
            continue;
        }
        if sched.crop_ms > 0.0 {
            // Partially late: crop the already-past frames off the front and play
            // the rest, which still lands on time. Record it as a late event.
            metrics.record_late_drop();
            let crop_frames = (sched.crop_ms * chunk.sample_rate as f64 / 1000.0).round() as usize;
            let crop_samples = (crop_frames * src_channels).min(chunk.samples.len());
            chunk.samples.drain(..crop_samples);
        }
        if chunk.samples.is_empty() {
            queue_end_ms = Some(sched.new_queue_end);
            continue;
        }

        // Actual delay from the source: how far this chunk's first sample plays
        // behind its origin timestamp. In steady state this sits at the delay
        // setting (the whole point of the regulation above).
        metrics.set_source_delay(chunk.play_at_ms as f64 - sched.start_ms);
        queue_end_ms = Some(sched.new_queue_end);

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

/// How to place a decoded chunk into the realtime player queue.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Placement {
    /// Sleep this many ms before appending (pace the queue / wait for the target).
    wait_ms: f64,
    /// Crop this many ms off the chunk's front (already-past audio); `0` = none.
    crop_ms: f64,
    /// Drop the chunk entirely — it's wholly behind the queue's play position.
    drop: bool,
    /// Server-clock time the (surviving) first sample will actually play at.
    start_ms: f64,
    /// Server-clock time the queue ends after appending (unused when `drop`).
    new_queue_end: f64,
}

/// Decide how to place a chunk of `dur` ms whose first sample should play at
/// `target` (server-clock ms) into a realtime player queue that currently ends at
/// `queue_end` (`None` = empty), keeping the queue about `cushion` ms deep.
///
/// The queue plays FIFO at realtime, so an appended chunk plays right after the
/// audio already queued: at `queue_end` if the queue is non-empty, else whenever
/// we append. That fixes the play time regardless of *when* we append, so the
/// only ways to change latency are to wait (let the queue drain / delay the start)
/// or to crop/drop (shed audio when the queue runs later than the target). `now`
/// is the current server clock; sleeps are capped at `max_sleep`.
#[allow(clippy::too_many_arguments)]
fn schedule_chunk(now: f64, target: f64, dur: f64, queue_end: Option<f64>) -> Placement {
    // Where this chunk begins playing, and how long to wait before appending.
    let (wait, start) = match queue_end {
        // Queue still has audio, and it ends at/after this chunk's target: append
        // right after it (plays at `qe`), paced so the queue drains toward the
        // cushion first. Appending later doesn't change the play time, so waiting
        // only bounds the queue depth.
        Some(qe) if qe > now + LATE_LEEWAY_MS && qe >= target - LATE_LEEWAY_MS => (
            (qe - TARGET_PLAY_QUEUE_MS - now).clamp(0.0, MAX_SLEEP_MS),
            qe,
        ),
        // Empty queue, or a gap (queued audio ends before the target): (re)start
        // playback exactly at the target. Wait until then (the queue drains to
        // silence meanwhile); if we're already late, `start` runs past `target`.
        _ => {
            let w = (target - now).clamp(0.0, MAX_SLEEP_MS);
            (w, now + w)
        }
    };

    let lateness = start - target;
    if lateness > LATE_LEEWAY_MS {
        if lateness >= dur {
            // The whole chunk is behind the queue's play position: drop it.
            return Placement {
                wait_ms: wait,
                crop_ms: 0.0,
                drop: true,
                start_ms: start,
                new_queue_end: start,
            };
        }
        // Crop the already-past front; the survivor plays on time at `start`.
        return Placement {
            wait_ms: wait,
            crop_ms: lateness,
            drop: false,
            start_ms: start,
            new_queue_end: start + (dur - lateness),
        };
    }

    Placement {
        wait_ms: wait,
        crop_ms: 0.0,
        drop: false,
        start_ms: start,
        new_queue_end: start + dur,
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
    use super::{remap_channels, schedule_chunk};

    fn sched(now: f64, target: f64, dur: f64, qe: Option<f64>) -> super::Placement {
        schedule_chunk(now, target, dur, qe)
    }

    #[test]
    fn empty_queue_waits_until_target() {
        // Nothing queued: hold the chunk until its target time, then start.
        let p = sched(1000.0, 1200.0, 20.0, None);
        assert_eq!(p.wait_ms, 200.0);
        assert!(!p.drop && p.crop_ms == 0.0);
        assert_eq!(p.start_ms, 1200.0);
        assert_eq!(p.new_queue_end, 1220.0);
    }

    #[test]
    fn steady_state_paces_to_cushion() {
        // Queue ends right at the target (chunk plays on time). We should wait
        // only until the queue has drained to the cushion, and it plays at `qe`.
        let target = 5000.0;
        let qe = 5000.0;
        let now = 4900.0; // queue is 100ms deep, cushion is 60ms
        let p = sched(now, target, 20.0, Some(qe));
        assert_eq!(p.wait_ms, qe - super::TARGET_PLAY_QUEUE_MS - now); // 40ms: drain 100→60
        assert_eq!(p.crop_ms, 0.0);
        assert_eq!(p.start_ms, qe);
        assert_eq!(p.new_queue_end, qe + 20.0);
    }

    #[test]
    fn queue_below_cushion_appends_immediately() {
        // Queue shallower than the cushion: no wait, build it up; still on time.
        let p = sched(4990.0, 5000.0, 20.0, Some(5000.0)); // 10ms queued
        assert_eq!(p.wait_ms, 0.0);
        assert_eq!(p.start_ms, 5000.0);
        assert_eq!(p.new_queue_end, 5020.0);
    }

    #[test]
    fn late_queue_crops_front() {
        // Delay was lowered slightly: target is 5000 but the queue still ends at
        // 5010 (10ms too deep). Crop 10ms off the front so the tail lands on time;
        // the survivor (meant for 5010) plays at 5010 and the queue-end advances
        // to target + dur.
        let p = sched(4980.0, 5000.0, 40.0, Some(5010.0));
        assert!(!p.drop);
        assert!((p.crop_ms - 10.0).abs() < 1e-9);
        assert_eq!(p.start_ms, 5010.0);
        assert_eq!(p.new_queue_end, 5010.0 + (40.0 - 10.0)); // = 5040 = target + dur
    }

    #[test]
    fn wholly_late_chunk_dropped() {
        // Queue ends 40ms past the target and the chunk is only 20ms long: every
        // sample is behind the queue's play position, so drop it entirely.
        let p = sched(4980.0, 5000.0, 20.0, Some(5040.0));
        assert!(p.drop);
        assert_eq!(p.crop_ms, 0.0);
    }

    #[test]
    fn gap_restarts_at_target() {
        // Queued audio ends well before the target (a gap / delay was raised):
        // wait out the gap and restart exactly at the target.
        let p = sched(4900.0, 5200.0, 20.0, Some(4950.0));
        assert_eq!(p.wait_ms, 300.0); // wait to target, queue underruns meanwhile
        assert_eq!(p.start_ms, 5200.0);
        assert_eq!(p.new_queue_end, 5220.0);
    }

    #[test]
    fn sleep_is_capped() {
        let p = sched(0.0, 100_000.0, 20.0, None);
        assert_eq!(p.wait_ms, super::MAX_SLEEP_MS);
    }

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
