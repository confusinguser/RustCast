//! RustCast: stream audio from sources (FIFOs or Spotify Connect) over multicast
//! UDP to any number of synchronized clients, with zero..many servers.
//!
//! - `server` binary: reads sources from a YAML config and multicasts each on its
//!   own group; announces its catalog; answers time-sync; serves the web UI.
//! - `client` binary: discovers sources from the catalog, plays the one selected
//!   for it from the UI, aligned to each packet's play-at timestamp so all clients
//!   stay in sync; multicasts its telemetry to every server.

pub mod api;
pub mod catalog;
pub mod clients;
pub mod config;
pub mod metrics;
pub mod net;
pub mod source;
pub mod stream;
pub mod sync;
pub mod wire;
