//! Client groups, persisted to `groups.json`.
//!
//! A group steers several clients at once: members follow the group's chosen
//! source. Membership lives on each [`crate::clients::ClientRecord`]; this store
//! holds the group's display name and selected source.
//!
//! A group's source is stored by *name*, not id: ids derive from the per-boot
//! server id and change across restarts, whereas the name is stable.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// One persisted group. Both fields optional: a freshly created group has no
/// name and no source yet (an empty rectangle in the UI).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GroupRecord {
    #[serde(default)]
    pub name: Option<String>,
    /// Source *name* this group points at (`None` = not listening to anything).
    #[serde(default)]
    pub source: Option<String>,
}

/// The persistent store, mirrored to `path` on every change.
pub struct GroupStore {
    path: String,
    inner: Mutex<HashMap<String, GroupRecord>>,
}

impl GroupStore {
    /// Load from `path`, or start empty if it doesn't exist / can't be parsed.
    pub fn load(path: &str) -> Self {
        let inner = match std::fs::read_to_string(path) {
            Ok(text) => serde_json::from_str(&text).unwrap_or_else(|e| {
                eprintln!("groups store: '{path}' is invalid ({e}); starting empty");
                HashMap::new()
            }),
            Err(_) => HashMap::new(),
        };
        Self {
            path: path.to_string(),
            inner: Mutex::new(inner),
        }
    }

    /// All groups as `(id, record)`, sorted by id's numeric suffix so the UI
    /// order is stable.
    pub fn list(&self) -> Vec<(String, GroupRecord)> {
        let map = self.inner.lock().unwrap();
        let mut v: Vec<(String, GroupRecord)> =
            map.iter().map(|(k, r)| (k.clone(), r.clone())).collect();
        v.sort_by_key(|(id, _)| num_suffix(id));
        v
    }

    pub fn get(&self, id: &str) -> Option<GroupRecord> {
        self.inner.lock().unwrap().get(id).cloned()
    }

    pub fn exists(&self, id: &str) -> bool {
        self.inner.lock().unwrap().contains_key(id)
    }

    /// Create a new, empty group and return its generated id (`g<N>`).
    pub fn create(&self) -> String {
        let mut map = self.inner.lock().unwrap();
        let next = map.keys().map(|k| num_suffix(k)).max().unwrap_or(0) + 1;
        let id = format!("g{next}");
        map.insert(id.clone(), GroupRecord::default());
        self.save(&map);
        id
    }

    /// Remove a group. Returns whether it existed.
    pub fn delete(&self, id: &str) -> bool {
        let mut map = self.inner.lock().unwrap();
        let existed = map.remove(id).is_some();
        if existed {
            self.save(&map);
        }
        existed
    }

    pub fn set_name(&self, id: &str, name: Option<String>) {
        let mut map = self.inner.lock().unwrap();
        if let Some(r) = map.get_mut(id) {
            r.name = name.filter(|s| !s.trim().is_empty());
            self.save(&map);
        }
    }

    /// Set (or, with `None`, clear) the group's source name.
    pub fn set_source(&self, id: &str, source: Option<String>) {
        let mut map = self.inner.lock().unwrap();
        if let Some(r) = map.get_mut(id) {
            r.source = source;
            self.save(&map);
        }
    }

    fn save(&self, map: &HashMap<String, GroupRecord>) {
        match serde_json::to_string_pretty(map) {
            Ok(json) => {
                if let Err(e) = std::fs::write(&self.path, json) {
                    eprintln!("groups store: could not write '{}': {e}", self.path);
                }
            }
            Err(e) => eprintln!("groups store: serialize error: {e}"),
        }
    }
}

/// Numeric part of a `g<N>` id (0 if it doesn't match), for ordering + id gen.
fn num_suffix(id: &str) -> u64 {
    id.strip_prefix('g')
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}
