//! Capture from the system audio server (PulseAudio or PipeWire) via the `parec`
//! / `pactl` CLIs, yielding interleaved f32 samples.
//!
//! Two flavors:
//! - [`PulseKind::Sink`]: registers a *virtual playback device* (a null sink) that
//!   apps can select as their output; we capture that sink's `.monitor`, so all
//!   audio routed to it is streamed. The sink is created on open and removed on
//!   drop.
//! - [`PulseKind::Source`]: captures an existing input (microphone / line-in),
//!   named or the default source.
//!
//! `parec`/`pactl` come with PulseAudio and with PipeWire's pulse compatibility
//! layer (`pipewire-pulse`), so this works on either. The server process must run
//! in the user's audio session (with `XDG_RUNTIME_DIR` set) to reach the server.

use std::io::{self, Read};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::time::Duration;

use super::{AudioSource, Format};

/// `parec` is asked for s16le; we decode that to f32 (as the pipe source does).
const BYTES_PER_SAMPLE: usize = 2;

/// Which audio-server endpoint to capture.
pub enum PulseKind {
    /// A virtual playback device (null sink) we create; `sink_name` is what apps
    /// see. We capture its `.monitor`.
    Sink { sink_name: String },
    /// An existing capture source; `None` = the server's default input.
    Source { device: Option<String> },
}

/// A running `parec` capture, decoded to f32.
pub struct PulseSource {
    child: Child,
    stdout: ChildStdout,
    format: Format,
    buf: [u8; 8192],
    // Carries bytes that didn't form a whole sample across read boundaries.
    leftover: Vec<u8>,
    /// If we created a null-sink module, its id (to unload on drop).
    module_id: Option<String>,
}

impl PulseSource {
    /// Open a capture for `kind` at `format` (channels + sample rate). For a
    /// sink, registers the null sink first (idempotent if it already exists).
    pub fn open(kind: PulseKind, format: Format) -> io::Result<Self> {
        let (device, module_id) = match kind {
            PulseKind::Sink { sink_name } => {
                let id = ensure_null_sink(&sink_name)?;
                (format!("{sink_name}.monitor"), id)
            }
            PulseKind::Source { device } => (device.unwrap_or_default(), None),
        };

        let mut cmd = Command::new("parec");
        cmd.arg("--format=s16le")
            .arg(format!("--rate={}", format.sample_rate))
            .arg(format!("--channels={}", format.channels));
        if !device.is_empty() {
            cmd.arg(format!("--device={device}"));
        }
        cmd.stdout(Stdio::piped()).stderr(Stdio::null());

        let mut child = cmd.spawn().map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "could not run `parec` ({e}); install pulseaudio-utils or \
                     pipewire-pulse, and ensure the server runs in the audio session"
                ),
            )
        })?;
        let stdout = child.stdout.take().expect("parec stdout piped");

        Ok(Self {
            child,
            stdout,
            format,
            buf: [0u8; 8192],
            leftover: Vec::new(),
            module_id,
        })
    }
}

impl AudioSource for PulseSource {
    fn format(&self) -> Format {
        self.format
    }

    fn next_samples(&mut self) -> io::Result<Option<Vec<f32>>> {
        // `parec` delivers audio at realtime as the device produces it, so this
        // read blocks until ~a buffer is available — no extra pacing needed.
        let n = self.stdout.read(&mut self.buf)?;
        if n == 0 {
            return Ok(None); // parec exited (device/sink gone)
        }
        self.leftover.extend_from_slice(&self.buf[..n]);

        let complete = self.leftover.len() - (self.leftover.len() % BYTES_PER_SAMPLE);
        let mut samples = Vec::with_capacity(complete / BYTES_PER_SAMPLE);
        for chunk in self.leftover[..complete].chunks_exact(BYTES_PER_SAMPLE) {
            let s = i16::from_le_bytes([chunk[0], chunk[1]]);
            samples.push(s as f32 / 32768.0);
        }
        self.leftover.drain(..complete);
        Ok(Some(samples))
    }

    fn next_samples_timeout(&mut self, _duration: Duration) -> io::Result<Option<Vec<f32>>> {
        // Capture is realtime-paced, so a plain (effectively prompt) read is fine.
        self.next_samples()
    }
}

impl Drop for PulseSource {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // Only unload a module we created (not one we reused / someone else's).
        if let Some(id) = &self.module_id {
            let _ = Command::new("pactl").args(["unload-module", id]).status();
        }
    }
}

/// Ensure a null sink named `sink_name` exists. Returns the id of the module we
/// created, or `None` if the sink already existed (so we don't unload someone
/// else's — and so a SIGKILL-orphaned sink is reused across restarts, not
/// duplicated).
fn ensure_null_sink(sink_name: &str) -> io::Result<Option<String>> {
    if sink_exists(sink_name) {
        return Ok(None);
    }
    let out = Command::new("pactl")
        .arg("load-module")
        .arg("module-null-sink")
        .arg(format!("sink_name={sink_name}"))
        .arg(format!("sink_properties=device.description={sink_name}"))
        .output()
        .map_err(|e| io::Error::new(e.kind(), format!("could not run `pactl`: {e}")))?;
    if !out.status.success() {
        return Err(io::Error::other(format!(
            "pactl load-module failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    Ok(if id.is_empty() { None } else { Some(id) })
}

/// Whether a sink with this name is already registered.
fn sink_exists(name: &str) -> bool {
    Command::new("pactl")
        .args(["list", "short", "sinks"])
        .output()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .any(|l| l.split('\t').nth(1) == Some(name))
        })
        .unwrap_or(false)
}
