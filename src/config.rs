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

use serde::Deserialize;

use crate::wire::WireFormat;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Optional multicast egress interface IP (`IP_MULTICAST_IF`). `None` lets
    /// the kernel choose — fine on a single-homed host.
    #[serde(default)]
    pub interface: Option<Ipv4Addr>,
    pub sources: Vec<SourceConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    /// Human display name, shown in the UI dropdown.
    pub name: String,
    /// Explicit multicast group override; auto-derived from the source id if absent.
    #[serde(default)]
    pub group: Option<Ipv4Addr>,
    #[serde(flatten)]
    pub kind: SourceKind,
}

#[derive(Debug, Clone, Deserialize)]
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
}

impl SourceKind {
    /// The `type` label, used for the wire catalog and UI grouping.
    pub fn type_name(&self) -> &'static str {
        match self {
            SourceKind::Pipe { .. } => "pipe",
            SourceKind::Spotify { .. } => "spotify",
        }
    }

    /// The configured wire-format string for this source.
    pub fn format_str(&self) -> &str {
        match self {
            SourceKind::Pipe { format, .. } => format,
            SourceKind::Spotify { format, .. } => format,
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
