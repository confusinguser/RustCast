//! Durable per-client settings, persisted to `clients.json`.
//!
//! Clients are identified by their MAC address (stable across IP changes and
//! restarts). Each record holds the volume, delay, and an optional display-name
//! override (the default name is the device's hostname, reported via telemetry).
//! The server treats these records as the authority: a [`crate::api`] edit writes
//! here, and a reconciler pushes the stored values back to a (re)connecting
//! client, so settings survive client restarts.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// One persisted client. `name = None` means "use the reported hostname".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientRecord {
    #[serde(default)]
    pub name: Option<String>,
    pub volume: f32,
    pub delay_ms: u32,
    /// Output channel map (one source-channel index per output channel, `-1` =
    /// silence). Empty = default identity mapping.
    #[serde(default)]
    pub channel_map: Vec<i16>,
    /// Name of the source this client last played *on this server* (restored on
    /// reconnect). `None` when off or playing another server's source.
    #[serde(default)]
    pub source: Option<String>,
    /// Id of the group this client belongs to (`None` = ungrouped). Members of a
    /// group follow the group's chosen source; see [`crate::groups`].
    #[serde(default)]
    pub group: Option<String>,
}

impl Default for ClientRecord {
    fn default() -> Self {
        Self {
            name: None,
            volume: 1.0,
            delay_ms: 0,
            channel_map: Vec::new(),
            source: None,
            group: None,
        }
    }
}

/// The persistent store, mirrored to `path` on every change.
pub struct ClientStore {
    path: String,
    inner: Mutex<HashMap<String, ClientRecord>>,
}

impl ClientStore {
    /// Load from `path`, or start empty if it doesn't exist / can't be parsed.
    pub fn load(path: &str) -> Self {
        let inner = match std::fs::read_to_string(path) {
            Ok(text) => serde_json::from_str(&text).unwrap_or_else(|e| {
                eprintln!("clients store: '{path}' is invalid ({e}); starting empty");
                HashMap::new()
            }),
            Err(_) => HashMap::new(),
        };
        Self {
            path: path.to_string(),
            inner: Mutex::new(inner),
        }
    }

    pub fn get(&self, mac: &str) -> Option<ClientRecord> {
        self.inner.lock().unwrap().get(mac).cloned()
    }

    /// Create a record for a newly-seen client from its reported values, if we
    /// don't already have one. Returns the effective record.
    pub fn get_or_create(&self, mac: &str, volume: f32, delay_ms: u32) -> ClientRecord {
        let mut map = self.inner.lock().unwrap();
        if let Some(rec) = map.get(mac) {
            return rec.clone();
        }
        let rec = ClientRecord {
            name: None,
            volume,
            delay_ms,
            channel_map: Vec::new(),
            source: None,
            group: None,
        };
        map.insert(mac.to_string(), rec.clone());
        self.save(&map);
        rec
    }

    pub fn set_volume(&self, mac: &str, volume: f32) {
        let mut map = self.inner.lock().unwrap();
        map.entry(mac.to_string()).or_default().volume = volume.clamp(0.0, 1.0);
        self.save(&map);
    }

    pub fn set_delay(&self, mac: &str, delay_ms: u32) {
        let mut map = self.inner.lock().unwrap();
        map.entry(mac.to_string()).or_default().delay_ms = delay_ms;
        self.save(&map);
    }

    /// Set (or clear) the name of the source this client is playing on this
    /// server, so it can be restored on reconnect.
    pub fn set_source_name(&self, mac: &str, source: Option<String>) {
        let mut m = self.inner.lock().unwrap();
        m.entry(mac.to_string()).or_default().source = source;
        self.save(&m);
    }

    /// Set the output channel map (one source-channel index per output channel).
    pub fn set_channel_map(&self, mac: &str, map: Vec<i16>) {
        let mut m = self.inner.lock().unwrap();
        m.entry(mac.to_string()).or_default().channel_map = map;
        self.save(&m);
    }

    /// Set (or, with `None`, clear) the client's group membership.
    pub fn set_group(&self, mac: &str, group: Option<String>) {
        let mut m = self.inner.lock().unwrap();
        m.entry(mac.to_string()).or_default().group = group;
        self.save(&m);
    }

    /// Ids of all clients belonging to `group`.
    pub fn members(&self, group: &str) -> Vec<String> {
        let m = self.inner.lock().unwrap();
        m.iter()
            .filter(|(_, r)| r.group.as_deref() == Some(group))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Drop every client out of `group` (used when the group is deleted).
    pub fn clear_group(&self, group: &str) {
        let mut m = self.inner.lock().unwrap();
        let mut changed = false;
        for r in m.values_mut() {
            if r.group.as_deref() == Some(group) {
                r.group = None;
                changed = true;
            }
        }
        if changed {
            self.save(&m);
        }
    }

    /// Set (or, with `None`/empty, clear) the display-name override.
    pub fn set_name(&self, mac: &str, name: Option<String>) {
        let mut map = self.inner.lock().unwrap();
        map.entry(mac.to_string()).or_default().name = name.filter(|s| !s.trim().is_empty());
        self.save(&map);
    }

    fn save(&self, map: &HashMap<String, ClientRecord>) {
        match serde_json::to_string_pretty(map) {
            Ok(json) => {
                if let Err(e) = std::fs::write(&self.path, json) {
                    eprintln!("clients store: could not write '{}': {e}", self.path);
                }
            }
            Err(e) => eprintln!("clients store: serialize error: {e}"),
        }
    }
}

/// Render a MAC as lowercase colon-separated hex, the `clients.json` key form.
pub fn mac_hex(mac: [u8; 6]) -> String {
    format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
    )
}

/// Parse a colon-separated hex MAC (as produced by [`mac_hex`]).
pub fn parse_mac_hex(s: &str) -> Option<[u8; 6]> {
    let mut out = [0u8; 6];
    let mut parts = s.split(':');
    for b in out.iter_mut() {
        *b = u8::from_str_radix(parts.next()?, 16).ok()?;
    }
    if parts.next().is_some() {
        return None; // too many octets
    }
    Some(out)
}
