use std::io;

pub mod librespot;
pub mod network;
pub mod pipe;

/// Format of an interleaved-sample stream. Assumed stable for the lifetime of
/// a given source.
#[derive(Clone, Copy, Debug)]
pub struct Format {
    pub channels: u16,
    pub sample_rate: u32,
}

/// A source of PCM audio that yields blocks of interleaved `f32` samples in
/// the range `[-1.0, 1.0]`. Implementors convert from whatever native format
/// they carry (e.g. the pipe decodes s16le; librespot already emits f32).
pub trait AudioSource {
    /// The format of the samples produced by [`AudioSource::next_samples`].
    fn format(&self) -> Format;

    /// Pull the next block of samples. `Ok(None)` signals end of stream.
    /// May block until samples are available.
    fn next_samples(&mut self) -> io::Result<Option<Vec<f32>>>;

    /// Return a pending output-volume change (linear gain, 1.0 = unity) if one
    /// is available, else `None`. Lets a source drive volume remotely (the
    /// network source applies the server-assigned volume here). Default: none.
    fn take_volume_update(&mut self) -> Option<f32> {
        None
    }
}
