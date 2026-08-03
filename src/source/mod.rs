use std::io;
use std::time::Duration;

pub mod librespot;
pub mod network;
pub mod pipe;
pub mod pulse;

/// Wait up to `timeout` for `fd` to become readable. `Ok(true)` if it is (data
/// ready, EOF, or hangup — all make a subsequent read return promptly),
/// `Ok(false)` on timeout, so a read loop can periodically check a stop flag.
pub(crate) fn poll_readable(fd: i32, timeout: Duration) -> io::Result<bool> {
    let mut pfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    let r = unsafe { libc::poll(&mut pfd, 1, ms) };
    if r < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(r > 0)
}

/// Format of an interleaved-sample stream. Assumed stable for the lifetime of
/// a given source.
#[derive(Clone, Copy, Debug)]
pub struct Format {
    pub channels: u16,
    pub sample_rate: u32,
}

/// A PCM audio input for the server, yielding blocks of interleaved `f32`
/// samples in the range `[-1.0, 1.0]`. Implementors convert from whatever
/// native format they carry (the pipe decodes s16le; librespot emits f32).
pub trait AudioSource {
    /// The format of the samples produced by [`AudioSource::next_samples`].
    fn format(&self) -> Format;

    /// Pull the next block of samples. `Ok(None)` signals end of stream.
    /// May block until samples are available.
    fn next_samples(&mut self) -> io::Result<Option<Vec<f32>>>;

    /// Like [`AudioSource::next_samples`] but bounded by `duration`: `Ok(Some(_))`
    /// with whatever samples are ready (an empty vec if the timeout elapsed),
    /// `Ok(None)` only on real end of stream. Lets the caller check a stop flag.
    fn next_samples_timeout(&mut self, duration: Duration) -> io::Result<Option<Vec<f32>>>;
}
