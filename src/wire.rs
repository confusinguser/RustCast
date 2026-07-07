//! The multicast wire protocol: one bincode-serialized [`AudioPacket`] per UDP
//! datagram. Each packet carries the absolute wall-clock time at which its
//! samples should begin playing, so every client renders the same sample at the
//! same instant (assuming their clocks are NTP-synced).

use std::net::Ipv4Addr;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Default administratively-scoped multicast group and port.
pub const DEFAULT_GROUP: Ipv4Addr = Ipv4Addr::new(239, 255, 42, 99);
pub const DEFAULT_PORT: u16 = 5004;
/// Unicast port on the server that answers time-sync requests.
pub const DEFAULT_SYNC_PORT: u16 = 5005;

/// Target PCM payload per datagram. Kept well under a 1500-byte MTU (with room
/// for the bincode header + IP/UDP headers) to avoid IP fragmentation.
pub const TARGET_PCM_BYTES: usize = 1024;

/// PCM encoding used on the wire. The server picks this; the client learns it
/// from each packet, so it needs no matching flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireFormat {
    S16Le,
    F32Le,
}

impl WireFormat {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "s16" | "s16le" => Some(Self::S16Le),
            "f32" | "f32le" => Some(Self::F32Le),
            _ => None,
        }
    }

    pub fn bytes_per_sample(self) -> usize {
        match self {
            Self::S16Le => 2,
            Self::F32Le => 4,
        }
    }

    /// Encode interleaved f32 samples (`[-1.0, 1.0]`) to this format.
    pub fn encode(self, samples: &[f32]) -> Vec<u8> {
        match self {
            Self::F32Le => {
                let mut out = Vec::with_capacity(samples.len() * 4);
                for s in samples {
                    out.extend_from_slice(&s.to_le_bytes());
                }
                out
            }
            Self::S16Le => {
                let mut out = Vec::with_capacity(samples.len() * 2);
                for s in samples {
                    let v = (s.clamp(-1.0, 1.0) * 32767.0) as i16;
                    out.extend_from_slice(&v.to_le_bytes());
                }
                out
            }
        }
    }

    /// Decode wire bytes back to interleaved f32 samples.
    pub fn decode(self, data: &[u8]) -> Vec<f32> {
        match self {
            Self::F32Le => data
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect(),
            Self::S16Le => data
                .chunks_exact(2)
                .map(|c| i16::from_le_bytes([c[0], c[1]]) as f32 / 32768.0)
                .collect(),
        }
    }
}

/// One datagram's worth of audio.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioPacket {
    /// Monotonic per-stream sequence number, for ordering and loss detection.
    pub seq: u64,
    /// Absolute UNIX-epoch time (milliseconds) at which to start playing this
    /// packet's first sample.
    pub play_at_ms: u64,
    pub sample_rate: u32,
    pub channels: u16,
    pub format: WireFormat,
    /// PCM samples encoded per `format`, interleaved across channels.
    pub data: Vec<u8>,
}

/// NTP-style time-sync request sent by a client to the server (unicast).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRequest {
    /// Client's local clock (epoch ms) at send time (T1).
    pub client_send_ms: u64,
    /// Correlates a response with its request.
    pub nonce: u64,
}

/// The server's reply to a [`TimeRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeResponse {
    /// Echoed T1 from the request.
    pub client_send_ms: u64,
    /// Server's local clock (epoch ms) when it answered (≈ T2 ≈ T3).
    pub server_ms: u64,
    pub nonce: u64,
}

/// Current local wall-clock time as UNIX-epoch milliseconds. On the client this
/// is only the *raw* local clock; playback scheduling uses the offset-corrected
/// estimate from [`crate::sync::SyncedClock`].
pub fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
