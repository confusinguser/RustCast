//! Server configuration, loaded from a YAML file.
//!
//! A server hosts a list of sources, each with a `type` and its own settings:
//!
//! ```yaml
//! interface: 192.168.1.10        # optional multicast egress interface IP
//! sources:
//!   - type: pipe
//!     name: "Living Room"
//!     path: testfifo
//!     format: s16                # s16 | f32 (per source)
//!     channels: 2
//!     sample_rate: 44100
//!     # group: 239.255.130.7     # optional; auto-derived from the id otherwise
//!   - type: spotify
//!     name: "Kitchen"
//!     device_name: "RustCast Kitchen"
//!     format: f32
//! ```

use std::io;
use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

use crate::wire::WireFormat;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Optional multicast egress interface IP (`IP_MULTICAST_IF`). `None` lets
    /// the kernel choose — fine on a single-homed host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface: Option<Ipv4Addr>,
    pub sources: Vec<SourceConfig>,
    /// Run a playback client inside the server process, so the server machine can
    /// also play a source. It appears as a normal client in the UI. Absent = off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_client: Option<LocalClientConfig>,
}

/// Settings for the in-process local client (see [`Config::local_client`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalClientConfig {
    /// Device id (defaults to a hostname-derived id, distinct from a standalone
    /// client on the same host).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Display name (defaults to the hostname).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    /// Human display name, shown in the UI dropdown.
    pub name: String,
    /// Explicit multicast group override; auto-derived from the source id if absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<Ipv4Addr>,
    /// Send lead in ms: how far ahead of play time the first packet copy is sent
    /// (the client's jitter-buffer depth). Adjustable live from the UI.
    #[serde(default = "default_lead_ms")]
    pub lead_ms: u64,
    /// Number of identical copies of each packet to send (repetition FEC).
    #[serde(default = "default_redundancy")]
    pub redundancy: u32,
    /// How long before play time the *last* copy is sent; copies are spaced
    /// evenly between `lead_ms` (first) and this (last).
    #[serde(default = "default_last_lead_ms")]
    pub last_lead_ms: u64,
    /// Stream by unicast to each listening client instead of the multicast group.
    #[serde(default)]
    pub unicast: bool,
    #[serde(flatten)]
    pub kind: SourceKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SourceKind {
    Pipe {
        path: String,
        #[serde(default = "default_format")]
        format: String,
        #[serde(default = "default_channels")]
        channels: u16,
        #[serde(default = "default_sample_rate")]
        sample_rate: u32,
    },
    Spotify {
        #[serde(default = "default_device_name")]
        device_name: String,
        #[serde(default = "default_spotify_format")]
        format: String,
    },
    /// A virtual playback device (null sink) registered with the audio server:
    /// apps can select it as an output, and everything routed to it is streamed.
    Sink {
        /// The sink name apps see (keep it simple: no spaces).
        #[serde(default = "default_sink_name")]
        device_name: String,
        #[serde(default = "default_format")]
        format: String,
        #[serde(default = "default_channels")]
        channels: u16,
        #[serde(default = "default_sample_rate")]
        sample_rate: u32,
    },
    /// A capture device (microphone / line-in) whose audio is streamed.
    Mic {
        /// Audio-server source name to capture; the default input if omitted.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        device: Option<String>,
        #[serde(default = "default_format")]
        format: String,
        #[serde(default = "default_channels")]
        channels: u16,
        #[serde(default = "default_sample_rate")]
        sample_rate: u32,
    },
}

impl SourceKind {
    /// The `type` label, used for the wire catalog and UI grouping.
    pub fn type_name(&self) -> &'static str {
        match self {
            SourceKind::Pipe { .. } => "pipe",
            SourceKind::Spotify { .. } => "spotify",
            SourceKind::Sink { .. } => "sink",
            SourceKind::Mic { .. } => "mic",
        }
    }

    /// The configured wire-format string for this source.
    pub fn format_str(&self) -> &str {
        match self {
            SourceKind::Pipe { format, .. } => format,
            SourceKind::Spotify { format, .. } => format,
            SourceKind::Sink { format, .. } => format,
            SourceKind::Mic { format, .. } => format,
        }
    }
}

/// Written when no config exists, so a fresh checkout runs out of the box.
const DEFAULT_CONFIG: &str = r#"# RustCast server config (auto-generated). Edit to add or change sources.
# Optional multicast egress interface IP (IP_MULTICAST_IF); omit on a
# single-homed host, set it on machines with several NICs (Wi-Fi + VPN, etc.).
# interface: 192.168.1.10

sources:
  - type: pipe
    name: "Pipe"
    path: testfifo          # FIFO carrying raw PCM
    format: s16             # s16 | f32
    channels: 2
    sample_rate: 44100

  # A Spotify Connect receiver (advertises over Zeroconf as `device_name`):
  # - type: spotify
  #   name: "Spotify"
  #   device_name: "RustCast"
  #   format: f32
"#;

impl Config {
    /// Load the config at `path`, writing a default there first if it's missing,
    /// so the server always has something to run.
    pub fn load_or_create(path: &str) -> io::Result<Config> {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::parse(&text, path),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                std::fs::write(path, DEFAULT_CONFIG)?;
                println!("no config at '{path}'; wrote a default one — edit it to add sources");
                Self::parse(DEFAULT_CONFIG, path)
            }
            Err(e) => Err(e),
        }
    }

    fn parse(text: &str, path: &str) -> io::Result<Config> {
        serde_norway::from_str(text)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{path}: {e}")))
    }

    /// Write the config back to `path`. Used to persist send-timing changes made
    /// from the web UI. Note: this is a serde round-trip, so any comments in the
    /// original file are not preserved.
    pub fn save(&self, path: &str) -> io::Result<()> {
        let yaml = serde_norway::to_string(self)
            .map_err(|e| io::Error::other(format!("serialize config: {e}")))?;
        std::fs::write(path, yaml)
    }

    /// Reject configs that can't be run, with a clear message (better than a
    /// panic deep inside a source thread later).
    pub fn validate(&self) -> Result<(), String> {
        if self.sources.is_empty() {
            return Err("config has no sources".into());
        }
        for s in &self.sources {
            if WireFormat::parse(s.kind.format_str()).is_none() {
                return Err(format!(
                    "source '{}': unknown format '{}' (expected s16 or f32)",
                    s.name,
                    s.kind.format_str()
                ));
            }
        }
        Ok(())
    }
}

fn default_format() -> String {
    "s16".into()
}
fn default_spotify_format() -> String {
    "f32".into()
}
fn default_channels() -> u16 {
    2
}
fn default_sample_rate() -> u32 {
    44_100
}
fn default_device_name() -> String {
    "RustCast".into()
}
fn default_sink_name() -> String {
    "RustCast".into()
}
fn default_lead_ms() -> u64 {
    200
}
fn default_redundancy() -> u32 {
    1
}
fn default_last_lead_ms() -> u64 {
    60
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_params_and_tagged_enum_round_trip() {
        // Defaults apply when send fields are absent.
        let cfg = serde_norway::from_str::<Config>(
            "sources:\n  - type: pipe\n    name: A\n    path: fifo\n",
        )
        .unwrap();
        assert_eq!(cfg.sources[0].lead_ms, 200);
        assert_eq!(cfg.sources[0].redundancy, 1);
        assert_eq!(cfg.sources[0].last_lead_ms, 60);

        // Explicit values parse, and survive a save/reload round-trip together
        // with the flattened `type`-tagged kind.
        let cfg = serde_norway::from_str::<Config>(
            "sources:\n  - type: mic\n    name: Mic\n    lead_ms: 400\n    redundancy: 3\n    last_lead_ms: 100\n",
        )
        .unwrap();
        let out = serde_norway::to_string(&cfg).unwrap();
        let back = serde_norway::from_str::<Config>(&out).unwrap();
        assert_eq!(back.sources[0].lead_ms, 400);
        assert_eq!(back.sources[0].redundancy, 3);
        assert_eq!(back.sources[0].last_lead_ms, 100);
        assert_eq!(back.sources[0].kind.type_name(), "mic");
    }

    #[test]
    fn local_client_and_unicast_round_trip() {
        let yaml = "\
local_client:\n  id: server-box\n  name: Server\n\
sources:\n  - type: sink\n    name: Multi\n    device_name: RustCast\n    channels: 6\n    unicast: true\n";
        let cfg = serde_norway::from_str::<Config>(yaml).unwrap();
        let lc = cfg.local_client.as_ref().expect("local_client present");
        assert_eq!(lc.id.as_deref(), Some("server-box"));
        assert_eq!(lc.name.as_deref(), Some("Server"));
        assert!(cfg.sources[0].unicast);

        // Survives a save/reload round-trip.
        let out = serde_norway::to_string(&cfg).unwrap();
        let back = serde_norway::from_str::<Config>(&out).unwrap();
        assert_eq!(back.local_client.unwrap().id.as_deref(), Some("server-box"));
        assert_eq!(back.sources[0].kind.type_name(), "sink");
    }
}
