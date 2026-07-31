//! The multicast wire protocol.
//!
//! There are four multicast planes plus one unicast one:
//! - **Audio** (server → clients): one bincode [`AudioPacket`] per datagram, sent
//!   to a per-source group on the shared [`AUDIO_PORT`]. Each packet carries the
//!   absolute wall-clock time at which its samples should begin playing, so every
//!   client renders the same sample at the same instant (clocks NTP-synced).
//! - **Catalog** (server → everyone, [`ANNOUNCE_GROUP`]): each server periodically
//!   multicasts its [`CatalogAnnounce`] so clients and other servers learn the full
//!   set of available sources across the LAN.
//! - **Telemetry** (clients → servers, unicast on [`TELEMETRY_PORT`]): while a web
//!   user watches, the server multicasts a [`TelemetryRequest`] ping on
//!   [`TELEMETRY_REQ_GROUP`]; each client replies by *unicasting* its
//!   [`TelemetryReport`] to that server for a short grace period, refreshed by
//!   each ping. Unicast avoids the loss/jitter of multicast (esp. over Wi-Fi).
//! - **Control** (server → clients, [`CONTROL_GROUP`]): the web UI mutates a client's
//!   settings by multicasting a [`ControlCommand`]; the client applies it and reflects
//!   the new value in its next telemetry report.
//! - **Time-sync** (unicast, [`DEFAULT_SYNC_PORT`]): the NTP-style exchange each
//!   client runs against the server that owns its currently-selected source.

use std::net::Ipv4Addr;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Shared UDP port every source's audio stream is sent to. Sources differ by
/// *group address*, not port, so a client can receive whichever source it
/// selected on a single bound socket.
pub const AUDIO_PORT: u16 = 5004;
/// Unicast port on each server that answers time-sync requests.
pub const DEFAULT_SYNC_PORT: u16 = 5005;

/// Catalog announcements (server → everyone).
pub const ANNOUNCE_GROUP: Ipv4Addr = Ipv4Addr::new(239, 255, 42, 100);
pub const ANNOUNCE_PORT: u16 = 5008;
/// Control commands (server → clients).
pub const CONTROL_GROUP: Ipv4Addr = Ipv4Addr::new(239, 255, 42, 101);
pub const CONTROL_PORT: u16 = 5009;
/// Unicast port a client sends its telemetry reports to (the requesting server).
pub const TELEMETRY_PORT: u16 = 5006;
/// Server → clients telemetry request ("ping"), multicast while a web user is
/// watching; clients unicast telemetry back in response.
pub const TELEMETRY_REQ_GROUP: Ipv4Addr = Ipv4Addr::new(239, 255, 42, 104);
pub const TELEMETRY_REQ_PORT: u16 = 5012;
/// Server send-path stats (server → servers), so any UI can graph every
/// server's streams, not just its own.
pub const STATS_GROUP: Ipv4Addr = Ipv4Addr::new(239, 255, 42, 103);
pub const STATS_PORT: u16 = 5011;

/// Max playback advance (ms). A client can only skip forward into audio it has
/// already buffered, so this must stay comfortably below the server's send lead.
pub const MAX_DELAY_MS: u32 = 150;

/// Target PCM payload per datagram. Kept well under a 1500-byte MTU (with room
/// for the bincode header + IP/UDP headers) to avoid IP fragmentation.
pub const TARGET_PCM_BYTES: usize = 1024;

/// PCM encoding used on the wire. The server picks this per source; the client
/// learns it from each packet, so it needs no matching flag.
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
    /// Which source this belongs to. Lets a client that briefly straddles two
    /// groups (during a source switch) reject packets from the source it left.
    pub source_id: u64,
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

/// One available source, as advertised in a [`CatalogAnnounce`]. The client
/// resolves its selected `source_id` to this to find the group to join and the
/// server to time-sync against.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    /// Globally-unique, stable id (hash of server id + source name). Never 0.
    pub source_id: u64,
    /// Human display name for the UI dropdown.
    pub name: String,
    /// "pipe" | "spotify", for UI grouping.
    pub source_type: String,
    /// Multicast group this source streams to (octets; audio port is [`AUDIO_PORT`]).
    pub group: [u8; 4],
    pub sample_rate: u32,
    pub channels: u16,
    pub format: WireFormat,
    /// Server's send lead in ms (jitter-buffer depth), for the UI.
    pub lead_ms: u32,
}

/// A server's periodic advertisement of the sources it hosts. Multicast on
/// [`ANNOUNCE_GROUP`]; received by clients (to resolve sources) and by other
/// servers (so any web UI can render the full global catalog).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogAnnounce {
    /// Per-process random id, so two servers with identical configs don't merge.
    pub server_id: u64,
    /// Server's unicast IP for time-sync (octets). The catalog receiver overrides
    /// this with the datagram's actual source address, so the sender may leave it 0.
    pub server_ip: [u8; 4],
    pub sync_port: u16,
    pub sent_ms: u64,
    pub sources: Vec<CatalogEntry>,
}

/// One source's cumulative send-path counters, as broadcast between servers.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SourceStat {
    pub source_id: u64,
    pub packets_sent: u64,
    pub frames_sent: u64,
    pub reanchors: u64,
    pub pending_len: u32,
}

/// A server's periodic broadcast of its sources' send-path stats on
/// [`STATS_GROUP`], so every server can graph every server's streams.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsBroadcast {
    pub server_id: u64,
    pub sent_ms: u64,
    pub sources: Vec<SourceStat>,
}

/// A settings change targeting one client, multicast by a server's web UI on
/// [`CONTROL_GROUP`]. The client is the source of truth: it applies the command
/// and reflects the new value in its next [`TelemetryReport`], which is how every
/// server's UI converges without server-to-server state sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlCommand {
    /// Which client this targets (octets). Others ignore it.
    pub target_ip: [u8; 4],
    /// Monotonic per-server id; lets clients dedup the redundant re-sends.
    pub cmd_id: u64,
    /// `None` = leave unchanged; `Some(0)` = Off (play nothing); `Some(id)` = select.
    pub set_source_id: Option<u64>,
    pub set_volume: Option<f32>,
    pub set_delay_ms: Option<u32>,
}

/// Server → clients ping. While a web user is watching, the server multicasts
/// this (~1 Hz) on [`TELEMETRY_REQ_GROUP`]; a client that receives it unicasts
/// its [`TelemetryReport`] to the ping's source for a short grace window,
/// refreshed by each subsequent ping. When pings stop, the client stops sending.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TelemetryRequest {
    /// The requesting server's id (for logging / dedup); the client replies to
    /// the datagram's source address regardless.
    pub server_id: u64,
}

/// A telemetry snapshot a client unicasts to a requesting server (~10 Hz) so
/// the web UI can graph each device's buffers and sample flow live. Counters are
/// cumulative since the client started; gauges are the instantaneous value at
/// report time. Also carries the client's self-owned settings, since with many
/// servers no single server owns them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryReport {
    /// Client's local clock (epoch ms) at report time.
    pub sent_ms: u64,
    /// Stable device identity: primary NIC MAC (or a hostname-derived fallback).
    pub mac: [u8; 6],
    /// The device's hostname, the default display name until overridden.
    pub hostname: String,
    /// Stream format, so the UI can convert buffer depths to milliseconds.
    pub sample_rate: u32,
    pub channels: u16,
    // --- client-owned settings (the client is the source of truth) ---
    /// Currently-selected source id (0 = off / playing nothing).
    pub selected_source_id: u64,
    /// Assigned playback volume (0.0..=1.0).
    pub volume: f32,
    /// Playback advance in milliseconds (played this much earlier than play_at).
    pub delay_ms: u32,
    // --- counters (cumulative) ---
    /// Sample blocks handed to the player (`player.append`).
    pub blocks_appended: u64,
    /// Individual interleaved samples handed to the player.
    pub samples_appended: u64,
    /// Packets received from the multicast stream.
    pub packets_received: u64,
    /// Blocks dropped because the output queue was already too deep (overrun).
    pub overrun_drops: u64,
    /// Packets dropped for arriving past their play-at time (late).
    pub late_drops: u64,
    /// Packets skipped as lost (never arrived within the jitter window).
    pub lost_packets: u64,
    /// Times the output queue was observed empty while streaming - a proxy for
    /// device underrun (rodio/cpal does not expose true callback underflows).
    pub underruns: u64,
    // --- gauges (instantaneous at report time) ---
    /// Output (device) queue depth in buffers, i.e. `player.len()`.
    pub output_queue_len: u32,
    /// Jitter-buffer depth in packets.
    pub jitter_buffer_len: u32,
    // --- clock sync ---
    /// Applied clock offset (server_clock - client_clock), ms; what playback uses.
    pub clock_offset_ms: f64,
    /// Best clock-offset estimate, ms (lowest-RTT sample in the window).
    pub clock_target_offset_ms: f64,
    /// Raw offset from the most recent NTP exchange, ms (before lowest-RTT pick).
    pub last_offset_ms: f64,
    /// RTT of the most recent NTP exchange, ms.
    pub last_rtt_ms: f64,
    /// Best round-trip time in the sync window, ms.
    pub rtt_ms: f64,
    /// Number of sync samples currently held.
    pub sync_samples: u32,
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
