//! RustCast: stream audio from a source (FIFO or Spotify Connect) over
//! multicast UDP to any number of synchronized clients.
//!
//! - `server` binary: reads an [`source::AudioSource`] and multicasts it.
//! - `client` binary: joins the multicast group and plays it back, aligned to
//!   the per-packet play-at timestamp so multiple clients stay in sync.

pub mod api;
pub mod player;
pub mod source;
pub mod sync;
pub mod wire;
