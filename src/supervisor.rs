//! Runtime source supervisor: owns the live sources, added/removed while the
//! server runs (hot-reload from the web UI). Each runs its own
//! [`run_source_stream`] thread; the announcer, stats samplers, and HTTP API
//! read the current set here, so changes take effect without a restart.

use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crate::catalog::{auto_group, source_id};
use crate::config::{SourceConfig, SourceKind};
use crate::metrics::ServerMetrics;
use crate::stream::{SendParams, run_source_stream};
use crate::sync::Listeners;
use crate::wire::{CatalogEntry, WireFormat};

/// One running source and everything needed to advertise, meter, and stop it.
struct RunningSource {
    name: String,
    group: Ipv4Addr,
    wire_fmt: WireFormat,
    channels: u16,
    sample_rate: u32,
    params: Arc<SendParams>,
    metrics: Arc<ServerMetrics>,
    /// The config that produced this source (its kind/name/group), used to
    /// persist the current source set back to the YAML.
    cfg: SourceConfig,
    stop: Arc<AtomicBool>,
    _join: JoinHandle<()>,
}

struct Inner {
    sources: HashMap<u64, RunningSource>,
    /// Multicast groups currently in use, so auto-derived groups don't collide.
    used_groups: HashSet<Ipv4Addr>,
}

/// The live set of sources this server streams. Shared across the announcer,
/// stats threads, and the HTTP API. Add/remove mutate it under a single lock.
pub struct SourceRegistry {
    server_id: u64,
    iface: Ipv4Addr,
    listeners: Arc<Listeners>,
    inner: Mutex<Inner>,
}

impl SourceRegistry {
    pub fn new(server_id: u64, iface: Ipv4Addr, listeners: Arc<Listeners>) -> Self {
        Self {
            server_id,
            iface,
            listeners,
            inner: Mutex::new(Inner {
                sources: HashMap::new(),
                used_groups: HashSet::new(),
            }),
        }
    }

    /// Start streaming `cfg` and register it. Returns its source id, or an error
    /// if the config is invalid or a source with the same name already exists.
    pub fn spawn(&self, cfg: &SourceConfig) -> Result<u64, String> {
        let wire_fmt = WireFormat::parse(cfg.kind.format_str())
            .ok_or_else(|| format!("unknown format '{}'", cfg.kind.format_str()))?;
        let id = source_id(self.server_id, &cfg.name);

        let mut inner = self.inner.lock().unwrap();
        if inner.sources.contains_key(&id) {
            return Err(format!("a source named '{}' already exists", cfg.name));
        }

        // Explicit group, or auto-derive one and nudge it off any collision.
        let mut group = cfg.group.unwrap_or_else(|| auto_group(id));
        while !inner.used_groups.insert(group) {
            let o = group.octets();
            group = Ipv4Addr::new(o[0], o[1], o[2], o[3].wrapping_add(1));
        }

        let (channels, sample_rate) = nominal_format(&cfg.kind);
        let params = Arc::new(SendParams::new(
            cfg.lead_ms,
            cfg.redundancy,
            cfg.last_lead_ms,
            cfg.unicast,
        ));
        let metrics = Arc::new(ServerMetrics::new());
        let stop = Arc::new(AtomicBool::new(false));

        let join = {
            let name = cfg.name.clone();
            let kind = cfg.kind.clone();
            let params = params.clone();
            let metrics = metrics.clone();
            let listeners = self.listeners.clone();
            let stop = stop.clone();
            let iface = self.iface;
            std::thread::Builder::new()
                .name(format!("stream-{name}"))
                .spawn(move || {
                    run_source_stream(
                        id, name, group, wire_fmt, iface, kind, params, listeners, metrics, stop,
                    )
                })
                .map_err(|e| format!("spawn stream thread: {e}"))?
        };

        inner.sources.insert(
            id,
            RunningSource {
                name: cfg.name.clone(),
                group,
                wire_fmt,
                channels,
                sample_rate,
                params,
                metrics,
                cfg: cfg.clone(),
                stop,
                _join: join,
            },
        );
        Ok(id)
    }

    /// Stop and forget the source with this id. The stream thread notices the
    /// stop flag and exits, dropping the source (which cleans up `parec` / a
    /// null sink). Returns whether a source was removed.
    pub fn remove(&self, id: u64) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if let Some(src) = inner.sources.remove(&id) {
            src.stop.store(true, Ordering::Relaxed);
            inner.used_groups.remove(&src.group);
            true
        } else {
            false
        }
    }

    /// Whether this server hosts the given source id.
    pub fn contains(&self, id: u64) -> bool {
        self.inner.lock().unwrap().sources.contains_key(&id)
    }

    /// The live send params for one source, if hosted here.
    pub fn params(&self, id: u64) -> Option<Arc<SendParams>> {
        self.inner
            .lock()
            .unwrap()
            .sources
            .get(&id)
            .map(|s| s.params.clone())
    }

    /// The name of one source, if hosted here.
    pub fn name(&self, id: u64) -> Option<String> {
        self.inner
            .lock()
            .unwrap()
            .sources
            .get(&id)
            .map(|s| s.name.clone())
    }

    /// The source id currently hosting a source with this name, if any.
    pub fn id_for_name(&self, name: &str) -> Option<u64> {
        let inner = self.inner.lock().unwrap();
        inner
            .sources
            .iter()
            .find(|(_, s)| s.name == name)
            .map(|(id, _)| *id)
    }

    /// Catalog entries (with live params) for the announcer / catalog responder.
    pub fn entries(&self) -> Vec<(CatalogEntry, Arc<SendParams>)> {
        let inner = self.inner.lock().unwrap();
        inner
            .sources
            .iter()
            .map(|(id, s)| {
                (
                    CatalogEntry {
                        source_id: *id,
                        name: s.name.clone(),
                        source_type: s.cfg.kind.type_name().to_string(),
                        group: s.group.octets(),
                        sample_rate: s.sample_rate,
                        channels: s.channels,
                        format: s.wire_fmt,
                        lead_ms: s.params.lead() as u32,
                    },
                    s.params.clone(),
                )
            })
            .collect()
    }

    /// `(id, metrics)` pairs for the send-path stats samplers.
    pub fn samplers(&self) -> Vec<(u64, Arc<ServerMetrics>)> {
        let inner = self.inner.lock().unwrap();
        inner
            .sources
            .iter()
            .map(|(id, s)| (*id, s.metrics.clone()))
            .collect()
    }

    /// The current source set as `SourceConfig`s (with live send-timing fields
    /// folded back in), sorted by name, for persisting to the YAML config.
    pub fn configs(&self) -> Vec<SourceConfig> {
        let inner = self.inner.lock().unwrap();
        let mut out: Vec<SourceConfig> = inner
            .sources
            .values()
            .map(|s| {
                let mut c = s.cfg.clone();
                c.lead_ms = s.params.lead();
                c.redundancy = s.params.redundancy();
                c.last_lead_ms = s.params.last_lead();
                c.unicast = s.params.unicast();
                c
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }
}

/// The nominal (channels, sample_rate) advertised for a source. Spotify decodes
/// at a fixed 44.1 kHz stereo; the rest carry it in their config.
fn nominal_format(kind: &SourceKind) -> (u16, u32) {
    match kind {
        SourceKind::Pipe {
            channels,
            sample_rate,
            ..
        }
        | SourceKind::Sink {
            channels,
            sample_rate,
            ..
        }
        | SourceKind::Mic {
            channels,
            sample_rate,
            ..
        } => (*channels, *sample_rate),
        SourceKind::Spotify { .. } => (2, 44_100),
    }
}
