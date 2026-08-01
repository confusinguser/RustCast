use super::{AudioSource, Format, poll_readable};
use std::fs::File;
use std::io::{self, Read};
use std::os::fd::AsRawFd;
use std::path::Path;
use std::time::Duration;

const BYTES_PER_SAMPLE: usize = 2; // i16

/// Reads interleaved signed-16-bit little-endian PCM from a FIFO and yields it
/// as `f32` samples.
pub struct PipeSource {
    file: File,
    format: Format,
    buf: [u8; 8192],
    // Carries bytes that didn't form a whole i16 across read boundaries, so
    // samples stay 2-byte aligned.
    leftover: Vec<u8>,
}

impl PipeSource {
    /// Open a FIFO carrying s16le PCM at `format`.
    pub fn open(path: impl AsRef<Path>, format: Format) -> io::Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            file,
            format,
            buf: [0u8; 8192],
            leftover: Vec::new(),
        })
    }

    /// Read one chunk (already known readable / blocking) and decode it.
    fn read_decoded(&mut self) -> io::Result<Option<Vec<f32>>> {
        let n = self.file.read(&mut self.buf)?;
        if n == 0 {
            // Writer closed the FIFO (EOF).
            return Ok(None);
        }
        self.leftover.extend_from_slice(&self.buf[..n]);

        // Decode every complete i16 sample, normalizing to [-1.0, 1.0], and
        // keep any trailing partial-sample byte for the next call.
        let complete = self.leftover.len() - (self.leftover.len() % BYTES_PER_SAMPLE);
        let mut samples = Vec::with_capacity(complete / BYTES_PER_SAMPLE);
        for chunk in self.leftover[..complete].chunks_exact(BYTES_PER_SAMPLE) {
            let s = i16::from_le_bytes([chunk[0], chunk[1]]);
            samples.push(s as f32 / 32768.0);
        }
        self.leftover.drain(..complete);
        Ok(Some(samples))
    }
}

impl AudioSource for PipeSource {
    fn format(&self) -> Format {
        self.format
    }

    fn next_samples(&mut self) -> io::Result<Option<Vec<f32>>> {
        self.read_decoded()
    }

    fn next_samples_timeout(&mut self, duration: Duration) -> io::Result<Option<Vec<f32>>> {
        // Wait up to `duration` for data; an empty vec means "nothing yet".
        if !poll_readable(self.file.as_raw_fd(), duration)? {
            return Ok(Some(Vec::new()));
        }
        self.read_decoded()
    }
}
