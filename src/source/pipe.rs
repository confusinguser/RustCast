use std::fs::File;
use std::io::{self, BufReader, Read};
use std::os::fd::AsRawFd;
use std::path::Path;

use super::{AudioSource, Format};

const BYTES_PER_SAMPLE: usize = 2; // i16

/// Reads interleaved signed-16-bit little-endian PCM from a FIFO and yields it
/// as `f32` samples. On open it shrinks the pipe's kernel buffer so little
/// already-produced audio stays parked in the pipe when the writer pauses.
pub struct PipeSource {
    reader: BufReader<File>,
    format: Format,
    buf: [u8; 8192],
    // Carries bytes that didn't form a whole i16 across read boundaries, so
    // samples stay 2-byte aligned.
    leftover: Vec<u8>,
}

impl PipeSource {
    /// Open a FIFO carrying s16le PCM at `format`, shrinking its kernel buffer
    /// to (about) `pipe_bytes`. The kernel rounds the size up to at least one
    /// page; a failure to resize is logged but not fatal.
    pub fn open(path: impl AsRef<Path>, format: Format, pipe_bytes: i32) -> io::Result<Self> {
        let file = File::open(path)?;

        // F_SETPIPE_SZ is Linux-only; returns the actual size or -1.
        let fd = file.as_raw_fd();
        let set = unsafe { libc::fcntl(fd, libc::F_SETPIPE_SZ, pipe_bytes) };
        if set < 0 {
            eprintln!(
                "warning: could not shrink pipe buffer: {}",
                io::Error::last_os_error()
            );
        } else {
            println!("pipe buffer size: {set} bytes");
        }

        Ok(Self {
            reader: BufReader::new(file),
            format,
            buf: [0u8; 8192],
            leftover: Vec::new(),
        })
    }
}

impl AudioSource for PipeSource {
    fn format(&self) -> Format {
        self.format
    }

    fn next_samples(&mut self) -> io::Result<Option<Vec<f32>>> {
        let n = self.reader.read(&mut self.buf)?;
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
