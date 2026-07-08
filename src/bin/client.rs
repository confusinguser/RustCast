//! RustCast client: join the multicast group and play the stream, aligned to
//! each packet's play-at timestamp so all clients stay in sync.
//!
//! Usage: `client [interface-ip]`

use std::net::{Ipv4Addr, UdpSocket};
use std::num::NonZero;
use std::sync::Arc;
use std::thread;

use rodio::SampleRate;
use rodio::cpal::BufferSize;
use rustcast::metrics::{DeviceMetrics, run_telemetry_sender};
use rustcast::source::network::NetworkSource;
use rustcast::wire::{DEFAULT_GROUP, DEFAULT_PORT, DEFAULT_TELEMETRY_PORT};

/// How often each client reports its live buffer/sample telemetry to the server.
const TELEMETRY_INTERVAL_MS: u64 = 100; // ~10 Hz

// Small cpal device buffer to keep output-side latency low.
const DEVICE_BUFFER_FRAMES: u32 = 512; // ~11ms at 44.1 kHz
// Hard cap on the rodio output-queue depth, in appended blocks. Kept short so
// that raising the delay skips the stream forward rather than piling audio into
// the output queue: once the queue is this deep we drop the new block instead
// of appending it, keeping output latency bounded. ~8 blocks leaves a couple of
// device-buffer callbacks of headroom against underrun while staying tight.
const MAX_QUEUED_BUFFERS: usize = 8;

fn main() {
    // Optional local interface IP to receive multicast on (for multi-homed
    // hosts); "0.0.0.0" lets the kernel choose.
    let iface: Ipv4Addr = std::env::args()
        .nth(1)
        .map(|s| s.parse().expect("interface must be an IPv4 address"))
        .unwrap_or(Ipv4Addr::UNSPECIFIED);

    let socket =
        UdpSocket::bind((Ipv4Addr::UNSPECIFIED, DEFAULT_PORT)).expect("bind receive socket");
    socket
        .join_multicast_v4(&DEFAULT_GROUP, &iface)
        .expect("join multicast group");

    println!("Listening on {DEFAULT_GROUP}:{DEFAULT_PORT} (interface {iface}) ...");

    // Shared live metrics, written by both the network source and this playback
    // loop and shipped to the server for the web UI's graphs.
    let metrics = Arc::new(DeviceMetrics::new());

    let mut source =
        NetworkSource::new(socket, metrics.clone()).expect("start network source");
    println!("Stream started; playing.");

    // Report telemetry to the server (address learned from the stream) at ~10 Hz.
    {
        let server_ip = source.server_ip_handle();
        let metrics = metrics.clone();
        thread::Builder::new()
            .name("telemetry".into())
            .spawn(move || {
                run_telemetry_sender(
                    server_ip,
                    DEFAULT_TELEMETRY_PORT,
                    metrics,
                    TELEMETRY_INTERVAL_MS,
                )
            })
            .expect("spawn telemetry thread");
    }

    let fmt = source.format();
    let handle = rodio::DeviceSinkBuilder::from_default_device()
        .expect("find default output device")
        .with_buffer_size(BufferSize::Fixed(DEVICE_BUFFER_FRAMES))
        .open_stream()
        .expect("open default audio stream");
    let player = rodio::Player::connect_new(&handle.mixer());

    let channels = NonZero::new(fmt.channels).expect("channels must be > 0");
    let sample_rate = SampleRate::new(fmt.sample_rate).expect("sample_rate must be > 0");

    // Tracks whether we've started feeding, so a never-yet-fed queue at length 0
    // isn't miscounted as an underrun.
    let mut started = false;
    // Play the current server until it goes silent (no packets for
    // SERVER_TIMEOUT_MS), then wait for a new server and resume.
    loop {
        while let Some(samples) = source.next_samples() {
            // Apply any server-assigned volume change.
            if let Some(vol) = source.take_volume_update() {
                player.set_volume(vol);
            }
            if samples.is_empty() {
                continue;
            }

            let queued = player.len();
            metrics.set_output_queue_len(queued);
            // Queue drained to empty while streaming: the card is starved
            // (likely audible as a gap/crackle).
            if started && queued == 0 {
                metrics.record_underrun();
            }

            // Drop this block if the output queue is running deep (card slower
            // than the stream) to keep latency bounded.
            if queued > MAX_QUEUED_BUFFERS {
                metrics.record_overrun_drop();
                continue;
            }
            let n = samples.len();
            player.append(rodio::buffer::SamplesBuffer::new(channels, sample_rate, samples));
            metrics.record_append(n);
            started = true;
        }

        // next_samples() returned None: nothing from the server for
        // SERVER_TIMEOUT_MS. Stop, wait for a new server, then resume.
        println!("Server went offline; waiting for a new server...");
        source.wait_for_new_server();
        println!("Stream resumed; playing.");
        started = false;
    }
}
