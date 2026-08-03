//! Acoustic delay measurement DSP.
//!
//! The server plays a short, distinct-frequency tone burst on each speaker under
//! test (all speakers sharing one `play_at` timeline), the browser records the
//! room with its microphone, and this module recovers — per speaker — *when* that
//! speaker's tone was actually heard. From those onset times we report:
//!
//! - **relative delay**: how far each speaker lags the fastest one, and
//! - **absolute delay**: server-emit → heard, i.e. the real end-to-end latency
//!   (network + jitter buffer + hardware + acoustic travel to the mic).
//!
//! Detection is a [generalized Goertzel][g] evaluated in short windows around each
//! burst's *expected* position (mapped from the shared server clock via the
//! recording's `capture_start_ms`); the window with the most energy at the
//! speaker's frequency marks the heard time. Because every speaker emits into one
//! recording at one `play_at`, the *differences* between heard times (the relative
//! delay) are immune to any browser↔server clock error — only the absolute number
//! leans on that offset.
//!
//! Everything here is pure (no I/O), so it is exercised directly by unit tests.
//!
//! [g]: https://en.wikipedia.org/wiki/Goertzel_algorithm

use std::f64::consts::PI;

/// Analyzer window sizing / search bounds. Time resolution is `HOP_MS`; the
/// widest measurable delay is `SEARCH_MS` (so burst spacing must exceed
/// `2 * SEARCH_MS`, enforced by the caller when it picks the schedule).
const HOP_MS: f64 = 1.0;
const WIN_MS: f64 = 8.0;
const SEARCH_MS: f64 = 250.0;
/// Coarse hop for sampling the per-frequency noise floor across the whole
/// recording. Bursts occupy a small fraction of the timeline, so the median of
/// these samples reflects true ambient noise at that frequency.
const BASELINE_HOP_MS: f64 = 13.0;

/// Detection gates, so a speaker that produced no sound (off / muted / mic didn't
/// pick it up) reports "not detected" instead of a spurious delay:
/// - a burst counts only if its energy is at least this many times the noise floor,
const DETECT_SNR: f64 = 5.0;
/// - a majority of bursts must clear that bar, and their measured delays must agree
///   to within this many ms (a real tone lands at the same delay each burst; noise
///   peaks scatter across the whole search window),
const CONSISTENCY_MS: f64 = 15.0;
/// - and a detected band must be within this fraction of the loudest detected tone,
///   which rejects a silent speaker's band catching a louder neighbour's leakage.
const LEAK_FRACTION: f64 = 0.06;

/// One speaker under test and the frequency it was assigned.
#[derive(Debug, Clone)]
pub struct Assignment {
    pub client_id: String,
    pub freq_hz: f64,
}

/// How the test was emitted — everything the analyzer needs to know, in server
/// clock milliseconds. Frequencies live in [`Assignment`].
#[derive(Debug, Clone)]
pub struct TestPlan {
    /// Server-clock time the first burst's first sample was scheduled to play.
    pub play_at_ms: u64,
    /// Gap between successive bursts. Must be `> 2 * SEARCH_MS`.
    pub burst_spacing_ms: u64,
    /// Bursts emitted per speaker (medianed for robustness).
    pub burst_count: u32,
    /// Duration of each burst.
    pub burst_ms: u64,
    pub assignments: Vec<Assignment>,
}

/// Per-speaker measurement result.
#[derive(Debug, Clone)]
pub struct SpeakerResult {
    pub client_id: String,
    /// Server-clock time (ms) the speaker's tone was first heard. `NaN` if the
    /// tone couldn't be located in the recording.
    pub onset_ms: f64,
    /// How far this speaker lags the fastest measured one (ms, ≥ 0).
    pub relative_delay_ms: f64,
    /// Server emit → heard delay (ms), including acoustic travel to the mic.
    pub absolute_delay_ms: f64,
    /// 0..1 detection confidence (peak energy vs. the surrounding noise floor).
    pub confidence: f64,
}

/// A Hann-windowed sine burst, interleaved-mono f32 in [-1, 1]. The window tapers
/// both ends so the burst has no hard edges — hard edges splatter energy across
/// the spectrum and would smear a neighbouring speaker's band. The acoustic energy
/// therefore peaks at the burst's centre (`dur_ms / 2` after the first sample),
/// which the analyzer accounts for.
pub fn tone_burst(freq_hz: f64, sample_rate: u32, dur_ms: u64) -> Vec<f32> {
    let n = (sample_rate as f64 * dur_ms as f64 / 1000.0).round() as usize;
    if n < 2 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let w = 0.5 - 0.5 * (2.0 * PI * i as f64 / (n as f64 - 1.0)).cos();
        let s = (2.0 * PI * freq_hz * i as f64 / sample_rate as f64).sin();
        out.push((w * s) as f32);
    }
    out
}

/// Hann-windowed generalized Goertzel magnitude of `block` at `freq_hz` (arbitrary
/// frequency, not restricted to a DFT bin), normalized by block length so windows
/// of equal size are comparable. The Hann window sharply suppresses spectral
/// leakage from the *other* speakers' tones (hundreds of Hz away), so a band with
/// no real tone reads near the noise floor instead of catching a neighbour.
/// Returns 0 for a block shorter than 2 samples.
fn goertzel_mag(block: &[f32], freq_hz: f64, sample_rate: u32) -> f64 {
    let n = block.len();
    if n < 2 {
        return 0.0;
    }
    let w = 2.0 * PI * freq_hz / sample_rate as f64;
    let coeff = 2.0 * w.cos();
    let denom = (n - 1) as f64;
    let mut s_prev = 0.0f64;
    let mut s_prev2 = 0.0f64;
    for (i, &x) in block.iter().enumerate() {
        let win = 0.5 - 0.5 * (2.0 * PI * i as f64 / denom).cos();
        let s = x as f64 * win + coeff * s_prev - s_prev2;
        s_prev2 = s_prev;
        s_prev = s;
    }
    let power = s_prev2 * s_prev2 + s_prev * s_prev - coeff * s_prev * s_prev2;
    power.max(0.0).sqrt() / n as f64
}

/// Median of the finite values in `v` (`NaN` if none are finite).
fn median(v: &[f64]) -> f64 {
    let mut xs: Vec<f64> = v.iter().copied().filter(|x| x.is_finite()).collect();
    if xs.is_empty() {
        return f64::NAN;
    }
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = xs.len();
    if n % 2 == 1 {
        xs[n / 2]
    } else {
        (xs[n / 2 - 1] + xs[n / 2]) / 2.0
    }
}

/// Recover per-speaker onset/delay from a mono recording.
///
/// `pcm` is mono f32 at `sample_rate`; `capture_start_ms` is the server-clock time
/// of `pcm[0]` (derived by the browser from an NTP-style offset). Results come back
/// in `plan.assignments` order.
pub fn analyze(
    pcm: &[f32],
    sample_rate: u32,
    plan: &TestPlan,
    capture_start_ms: f64,
) -> Vec<SpeakerResult> {
    let sr = sample_rate as f64;
    let hop = ((sr * HOP_MS / 1000.0).round() as usize).max(1);
    let win = ((sr * WIN_MS / 1000.0).round() as usize).max(1);
    let search = (sr * SEARCH_MS / 1000.0) as isize;
    let baseline_hop = ((sr * BASELINE_HOP_MS / 1000.0).round() as usize).max(1);
    // Server-ms → fractional sample index in the recording.
    let idx_of = |ms: f64| (ms - capture_start_ms) * sr / 1000.0;
    let half_burst = plan.burst_ms as f64 / 2.0;

    // First pass: locate each speaker's tone and decide whether it was really heard.
    struct Detection {
        delay: f64, // median server-ms offset (heard − expected) across detected bursts
        peak: f64,  // median in-band energy of the detected bursts
        snr: f64,   // median energy ÷ noise floor
    }
    let detections: Vec<Option<Detection>> = plan
        .assignments
        .iter()
        .map(|a| {
            // Noise floor at this frequency: median energy sampled across the whole
            // recording. An absent tone reads at this floor; a real tone sits far
            // above it. This is the reference that makes detection *absolute* rather
            // than just picking the loudest window (which noise always has).
            let mut floor_mags = Vec::new();
            let mut s = 0;
            while s + win <= pcm.len() {
                floor_mags.push(goertzel_mag(&pcm[s..s + win], a.freq_hz, sample_rate));
                s += baseline_hop;
            }
            let floor = median(&floor_mags).max(1e-9);

            // Per burst: find the highest-energy window near where the burst's centre
            // is expected; keep it only if it clearly beats the noise floor.
            let mut delays = Vec::new();
            let mut peaks = Vec::new();
            let mut snrs = Vec::new();
            for k in 0..plan.burst_count {
                let expected_peak_ms =
                    plan.play_at_ms as f64 + k as f64 * plan.burst_spacing_ms as f64 + half_burst;
                let center = idx_of(expected_peak_ms);
                let mut best_mag = 0.0f64;
                let mut best_center = center;
                let mut any = false;
                let mut off = -search;
                while off <= search {
                    let start = center as isize + off - win as isize / 2;
                    if start >= 0 && (start as usize + win) <= pcm.len() {
                        let m = goertzel_mag(
                            &pcm[start as usize..start as usize + win],
                            a.freq_hz,
                            sample_rate,
                        );
                        any = true;
                        if m > best_mag {
                            best_mag = m;
                            best_center = (start + win as isize / 2) as f64;
                        }
                    }
                    off += hop as isize;
                }
                if !any {
                    continue;
                }
                let snr = best_mag / floor;
                if snr >= DETECT_SNR {
                    let measured_peak_ms = capture_start_ms + best_center * 1000.0 / sr;
                    delays.push(measured_peak_ms - expected_peak_ms);
                    peaks.push(best_mag);
                    snrs.push(snr);
                }
            }

            // A real tone clears the floor on a majority of bursts...
            let needed = (plan.burst_count as usize).div_ceil(2).max(1);
            if delays.len() < needed {
                return None;
            }
            // ...and lands at a consistent delay each time; noise peaks land at
            // random offsets across the ±SEARCH window, so a wide spread means we
            // were tracking noise, not a tone.
            let med = median(&delays);
            let mad = median(&delays.iter().map(|d| (d - med).abs()).collect::<Vec<_>>());
            if !mad.is_finite() || mad > CONSISTENCY_MS {
                return None;
            }
            Some(Detection {
                delay: med,
                peak: median(&peaks),
                snr: median(&snrs),
            })
        })
        .collect();

    // A silent speaker's band can still catch a louder neighbour's spectral
    // leakage — consistent and above its own (near-zero) floor. Reject any detected
    // band far quieter than the loudest, which real leakage always is.
    let max_peak = detections
        .iter()
        .flatten()
        .map(|d| d.peak)
        .fold(0.0f64, f64::max);

    let mut results: Vec<SpeakerResult> = plan
        .assignments
        .iter()
        .zip(detections.iter())
        .map(|(a, det)| {
            let ok = det
                .as_ref()
                .filter(|d| max_peak <= 0.0 || d.peak >= LEAK_FRACTION * max_peak);
            match ok {
                Some(d) => SpeakerResult {
                    client_id: a.client_id.clone(),
                    onset_ms: plan.play_at_ms as f64 + d.delay,
                    relative_delay_ms: 0.0,
                    absolute_delay_ms: d.delay,
                    confidence: (1.0 - DETECT_SNR / d.snr).clamp(0.0, 1.0),
                },
                None => SpeakerResult {
                    client_id: a.client_id.clone(),
                    onset_ms: f64::NAN,
                    relative_delay_ms: f64::NAN,
                    absolute_delay_ms: f64::NAN,
                    confidence: 0.0,
                },
            }
        })
        .collect();

    // Relative delay is measured against the fastest speaker we actually detected.
    let fastest = results
        .iter()
        .filter(|r| r.absolute_delay_ms.is_finite())
        .map(|r| r.absolute_delay_ms)
        .fold(f64::INFINITY, f64::min);
    if fastest.is_finite() {
        for r in &mut results {
            if r.absolute_delay_ms.is_finite() {
                r.relative_delay_ms = r.absolute_delay_ms - fastest;
            }
        }
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tone_burst_is_windowed() {
        let b = tone_burst(2000.0, 48_000, 40);
        assert_eq!(b.len(), 48_000 * 40 / 1000);
        // Hann taper: ends near silent, centre near full amplitude.
        assert!(b[0].abs() < 0.02);
        assert!(b[b.len() - 1].abs() < 0.02);
        let peak = b.iter().fold(0.0f32, |m, &x| m.max(x.abs()));
        assert!(peak > 0.9, "peak {peak}");
    }

    /// Synthesize a recording of two speakers at distinct frequencies with known
    /// delays and assert the analyzer recovers them.
    #[test]
    fn analyze_recovers_known_delays() {
        let sr = 48_000u32;
        let plan = TestPlan {
            play_at_ms: 1_000,
            burst_spacing_ms: 700,
            burst_count: 5,
            burst_ms: 40,
            assignments: vec![
                Assignment {
                    client_id: "fast".into(),
                    freq_hz: 2000.0,
                },
                Assignment {
                    client_id: "slow".into(),
                    freq_hz: 3000.0,
                },
            ],
        };
        // Recording starts at server time 0; runs long enough to cover all bursts.
        let capture_start_ms = 0.0;
        let total_ms = plan.play_at_ms + plan.burst_count as u64 * plan.burst_spacing_ms + 500;
        let mut pcm = vec![0.0f32; (sr as u64 * total_ms / 1000) as usize];

        let place = |pcm: &mut [f32], freq: f64, delay_ms: f64| {
            let burst = tone_burst(freq, sr, plan.burst_ms);
            for k in 0..plan.burst_count {
                let play_at = plan.play_at_ms as f64 + k as f64 * plan.burst_spacing_ms as f64;
                let start =
                    ((play_at + delay_ms - capture_start_ms) * sr as f64 / 1000.0).round() as usize;
                for (i, &s) in burst.iter().enumerate() {
                    if let Some(slot) = pcm.get_mut(start + i) {
                        *slot += s;
                    }
                }
            }
        };
        place(&mut pcm, 2000.0, 0.0);
        place(&mut pcm, 3000.0, 15.0);

        let results = analyze(&pcm, sr, &plan, capture_start_ms);
        let fast = &results[0];
        let slow = &results[1];
        assert!(fast.confidence > 0.5 && slow.confidence > 0.5);
        // Absolute delays land near the injected values (±2 ms resolution).
        assert!(
            fast.absolute_delay_ms.abs() < 2.0,
            "fast abs {}",
            fast.absolute_delay_ms
        );
        assert!(
            (slow.absolute_delay_ms - 15.0).abs() < 2.0,
            "slow abs {}",
            slow.absolute_delay_ms
        );
        // Relative: fastest reads ~0, the other ~15 ms behind.
        assert!(fast.relative_delay_ms.abs() < 2.0);
        assert!((slow.relative_delay_ms - 15.0).abs() < 2.0);
    }

    /// Deterministic low-amplitude noise, so the noise floor is realistic rather
    /// than digital silence. A plain LCG keeps the test reproducible.
    fn add_noise(pcm: &mut [f32], amp: f32) {
        let mut state: u32 = 0x1234_5678;
        for s in pcm.iter_mut() {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let u = (state >> 8) as f32 / (1u32 << 24) as f32; // [0, 1)
            *s += (u - 0.5) * 2.0 * amp;
        }
    }

    fn base_plan() -> TestPlan {
        TestPlan {
            play_at_ms: 1_000,
            burst_spacing_ms: 700,
            burst_count: 5,
            burst_ms: 40,
            assignments: vec![
                Assignment {
                    client_id: "on".into(),
                    freq_hz: 2000.0,
                },
                Assignment {
                    client_id: "off".into(),
                    freq_hz: 2700.0,
                },
            ],
        }
    }

    fn empty_recording(plan: &TestPlan, sr: u32) -> Vec<f32> {
        let total_ms = plan.play_at_ms + plan.burst_count as u64 * plan.burst_spacing_ms + 500;
        vec![0.0f32; (sr as u64 * total_ms / 1000) as usize]
    }

    fn place_tone(pcm: &mut [f32], plan: &TestPlan, sr: u32, freq: f64, delay_ms: f64) {
        let burst = tone_burst(freq, sr, plan.burst_ms);
        for k in 0..plan.burst_count {
            let play_at = plan.play_at_ms as f64 + k as f64 * plan.burst_spacing_ms as f64;
            let start = ((play_at + delay_ms) * sr as f64 / 1000.0).round() as usize;
            for (i, &s) in burst.iter().enumerate() {
                if let Some(slot) = pcm.get_mut(start + i) {
                    *slot += s;
                }
            }
        }
    }

    /// The reported bug: a speaker that produced no sound (physically off) must NOT
    /// get a spurious result while a real speaker is playing.
    #[test]
    fn silent_speaker_not_detected() {
        let sr = 48_000u32;
        let plan = base_plan();
        let mut pcm = empty_recording(&plan, sr);
        add_noise(&mut pcm, 0.01);
        place_tone(&mut pcm, &plan, sr, 2000.0, 0.0); // only the "on" speaker emits

        let r = analyze(&pcm, sr, &plan, 0.0);
        assert!(
            r[0].absolute_delay_ms.is_finite(),
            "playing speaker should be detected"
        );
        assert!(r[0].confidence > 0.5);
        assert!(
            !r[1].absolute_delay_ms.is_finite(),
            "off speaker must be undetected, got {} ms (conf {})",
            r[1].absolute_delay_ms,
            r[1].confidence,
        );
        assert_eq!(r[1].confidence, 0.0);
    }

    /// No tones at all (the mic picked up nothing): every speaker is undetected,
    /// never a fabricated delay.
    #[test]
    fn pure_noise_not_detected() {
        let sr = 48_000u32;
        let plan = base_plan();
        let mut pcm = empty_recording(&plan, sr);
        add_noise(&mut pcm, 0.02);

        for r in analyze(&pcm, sr, &plan, 0.0) {
            assert!(
                !r.absolute_delay_ms.is_finite(),
                "{} should be undetected in pure noise, got {} ms",
                r.client_id,
                r.absolute_delay_ms,
            );
            assert_eq!(r.confidence, 0.0);
        }
    }
}
