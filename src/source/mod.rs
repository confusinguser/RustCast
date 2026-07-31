use std::io;
use std::time::Duration;

pub mod librespot;
pub mod network;
pub mod pipe;
pub mod pulse;

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
    
    fn next_samples_timeout(&mut self, duration: Duration) -> io::Result<Option<Vec<f32>>>;
}
