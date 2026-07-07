//! Generic playback: drive any [`AudioSource`] into the default output device.

use rodio::SampleRate;
use rodio::cpal::BufferSize;
use std::num::NonZero;

use crate::source::AudioSource;

/// Small cpal device buffer keeps the standing downstream backlog low so that
/// pause→silence latency stays short.
pub const DEVICE_BUFFER_FRAMES: u32 = 512; // ~11ms at 44.1 kHz

/// If the output queue grows beyond this many buffers, the source is feeding
/// faster than the sound card drains it (the card/clock is slower than the
/// stream), so buffered latency is accumulating. We drop a buffer to bring the
/// queue back toward empty rather than let latency creep up unbounded.
pub const MAX_QUEUED_BUFFERS: usize = 60;

/// Append each block of samples from `source` to the output device as it
/// arrives. Runs until the source ends. Pacing/buffering is the source's job
/// (the network source schedules by play-at timestamp; the pipe self-paces).
pub fn play(mut source: impl AudioSource) {
    let fmt = source.format();

    let handle = rodio::DeviceSinkBuilder::from_default_device()
        .expect("find default output device")
        .with_buffer_size(BufferSize::Fixed(DEVICE_BUFFER_FRAMES))
        .open_stream()
        .expect("open default audio stream");
    let player = rodio::Player::connect_new(&handle.mixer());

    let channels = NonZero::new(fmt.channels).expect("channels must be > 0");
    let sample_rate = SampleRate::new(fmt.sample_rate).expect("sample_rate must be > 0");

    loop {
        // Apply any remotely-driven volume change (network source only).
        if let Some(vol) = source.take_volume_update() {
            player.set_volume(vol);
        }

        match source.next_samples() {
            Ok(Some(samples)) if samples.is_empty() => continue,
            Ok(Some(samples)) => {
                // Drop this block if we're running ahead of the card (queue too
                // deep), to keep latency bounded — see MAX_QUEUED_BUFFERS.
                if player.len() > MAX_QUEUED_BUFFERS {
                    continue;
                }
                let buf = rodio::buffer::SamplesBuffer::new(channels, sample_rate, samples);
                player.append(buf);
            }
            Ok(None) => break,
            Err(e) => {
                eprintln!("source error: {e}");
                break;
            }
        }
    }

    player.sleep_until_end();
}
