//! RustCast server: read the sources listed in a YAML config and multicast each
//! as timestamped PCM on its own group. Announces its catalog, answers time-sync,
//! receives client telemetry (only while a web user is present), and serves the
//! control UI.
//!
//! Usage: `server [config.yaml]`  (default: `rustcast.yaml`)

use std::collections::HashSet;
use std::io::Read;
use std::net::Ipv4Addr;
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;

use rustcast::api::{self, ControlSender};
use rustcast::catalog::{
    CatalogStore, auto_group, run_catalog_announcer, run_catalog_receiver, source_id,
};
use rustcast::clients::{ClientStore, mac_hex};
use rustcast::config::{Config, SourceKind};
use rustcast::metrics::{
    ServerMetrics, TelemetryStore, WebActivity, run_stats_broadcaster, run_stats_receiver,
    run_telemetry_receiver, run_telemetry_requester,
};
use rustcast::stream::run_source_stream;
use rustcast::sync::run_server_responder;
use rustcast::wire::{CatalogEntry, DEFAULT_SYNC_PORT, WireFormat, now_epoch_ms};

/// Port for the HTTP control API + web UI.
const HTTP_PORT: u16 = 8080;
/// How often each source's send-path metrics are sampled into history (~10 Hz).
const SERVER_SAMPLE_MS: u64 = 100;
/// Where per-client settings are persisted.
const CLIENTS_JSON: &str = "clients.json";
/// How often the reconciler enforces persisted settings onto clients.
const RECONCILE_MS: u64 = 500;

/// One fully-resolved source, ready to stream.
struct Prepared {
    id: u64,
    name: String,
    group: Ipv4Addr,
    wire_fmt: WireFormat,
    channels: u16,
    sample_rate: u32,
    kind: SourceKind,
    metrics: Arc<ServerMetrics>,
}

fn main() {
    let config_path = std::env::args().nth(1).unwrap_or_else(|| "rustcast.yaml".into());
    let config = Config::load_or_create(&config_path).unwrap_or_else(|e| {
        eprintln!("failed to load config '{config_path}': {e}");
        exit(1);
    });
    if let Err(e) = config.validate() {
        eprintln!("invalid config '{config_path}': {e}");
        exit(1);
    }
    let iface = config.interface.unwrap_or(Ipv4Addr::UNSPECIFIED);
    let server_id = gen_server_id();

    // Resolve every source: id, group (explicit or auto), wire format, metrics.
    let mut prepared: Vec<Prepared> = Vec::new();
    let mut used_groups: HashSet<Ipv4Addr> = HashSet::new();
    for s in &config.sources {
        let id = source_id(server_id, &s.name);
        let mut group = s.group.unwrap_or_else(|| auto_group(id));
        // Nudge apart any two of *our own* sources that landed on the same group.
        while !used_groups.insert(group) {
            let o = group.octets();
            group = Ipv4Addr::new(o[0], o[1], o[2], o[3].wrapping_add(1));
        }
        let wire_fmt = WireFormat::parse(s.kind.format_str()).expect("validated above");
        let (channels, sample_rate) = nominal_format(&s.kind);
        prepared.push(Prepared {
            id,
            name: s.name.clone(),
            group,
            wire_fmt,
            channels,
            sample_rate,
            kind: s.kind.clone(),
            metrics: Arc::new(ServerMetrics::new()),
        });
    }

    // Shared state.
    let catalog_store = Arc::new(CatalogStore::new());
    let telemetry = Arc::new(TelemetryStore::new());
    let activity = Arc::new(WebActivity::new());
    let control = Arc::new(ControlSender::new(iface).expect("bind control socket"));
    // Durable per-client settings, keyed by MAC.
    let clients_store = Arc::new(ClientStore::load(CLIENTS_JSON));

    // Catalog entries we advertise.
    let mut entries: Vec<CatalogEntry> = Vec::new();
    for p in &prepared {
        entries.push(CatalogEntry {
            source_id: p.id,
            name: p.name.clone(),
            source_type: p.kind.type_name().to_string(),
            group: p.group.octets(),
            sample_rate: p.sample_rate,
            channels: p.channels,
            format: p.wire_fmt,
            lead_ms: rustcast::stream::SEND_LEAD_MS as u32,
        });
    }
    let entries = Arc::new(entries);

    println!(
        "RustCast server {server_id:016x}: {} source(s), UI on http://0.0.0.0:{HTTP_PORT}",
        prepared.len()
    );

    // Always-on service threads.
    {
        let catalog = catalog_store.clone();
        let telemetry = telemetry.clone();
        let clients = clients_store.clone();
        let activity = activity.clone();
        let control = control.clone();
        std::thread::spawn(move || {
            api::run(server_id, catalog, telemetry, clients, activity, control, HTTP_PORT)
        });
    }
    // Reconcile persisted settings onto clients: adopt a new client's reported
    // values, and push the stored volume/delay to a (re)connecting client whose
    // live values differ — this is what makes settings survive client restarts.
    {
        let telemetry = telemetry.clone();
        let clients = clients_store.clone();
        let control = control.clone();
        std::thread::spawn(move || {
            loop {
                for c in telemetry.clients_summary() {
                    let key = mac_hex(c.mac);
                    let rec = clients.get_or_create(&key, c.volume, c.delay_ms);
                    if (rec.volume - c.volume).abs() > f32::EPSILON {
                        control.send(c.ip, None, Some(rec.volume), None);
                    }
                    if rec.delay_ms != c.delay_ms {
                        control.send(c.ip, None, None, Some(rec.delay_ms));
                    }
                }
                std::thread::sleep(Duration::from_millis(RECONCILE_MS));
            }
        });
    }
    std::thread::spawn(move || run_server_responder(DEFAULT_SYNC_PORT));
    {
        let entries = entries.clone();
        std::thread::spawn(move || {
            run_catalog_announcer(server_id, DEFAULT_SYNC_PORT, entries, iface)
        });
    }
    {
        let catalog = catalog_store.clone();
        std::thread::spawn(move || run_catalog_receiver(catalog, iface));
    }
    {
        let telemetry = telemetry.clone();
        std::thread::spawn(move || run_telemetry_receiver(telemetry));
    }
    // Ping clients for telemetry (unicast) while a web user is watching.
    {
        let activity = activity.clone();
        std::thread::spawn(move || run_telemetry_requester(server_id, activity, iface));
    }
    // Learn other servers' send-path stats so this UI graphs their streams too.
    {
        let telemetry = telemetry.clone();
        std::thread::spawn(move || run_stats_receiver(telemetry, server_id, iface));
    }
    // Broadcast our own sources' send-path stats to other servers.
    {
        let samplers: Vec<(u64, Arc<ServerMetrics>)> =
            prepared.iter().map(|p| (p.id, p.metrics.clone())).collect();
        std::thread::spawn(move || run_stats_broadcaster(server_id, samplers, iface));
    }
    // Sample each source's send-path metrics into the history at ~10 Hz.
    {
        let telemetry = telemetry.clone();
        let samplers: Vec<(u64, Arc<ServerMetrics>)> =
            prepared.iter().map(|p| (p.id, p.metrics.clone())).collect();
        std::thread::spawn(move || loop {
            for (id, m) in &samplers {
                telemetry.push_server(*id, m.snapshot());
            }
            std::thread::sleep(Duration::from_millis(SERVER_SAMPLE_MS));
        });
    }

    // One streaming thread per source.
    let mut handles = Vec::new();
    for p in prepared {
        let handle = std::thread::Builder::new()
            .name(format!("stream-{}", p.name))
            .spawn(move || {
                run_source_stream(p.id, p.name, p.group, p.wire_fmt, iface, p.kind, p.metrics)
            })
            .expect("spawn source stream");
        handles.push(handle);
    }

    // Park: keep the process alive as long as any source stream is running.
    for h in handles {
        let _ = h.join();
    }
    eprintln!("all source streams ended; exiting.");
}

/// The nominal (channels, sample_rate) advertised for a source. Pipe takes it
/// from config; Spotify decodes at a fixed 44.1 kHz stereo.
fn nominal_format(kind: &SourceKind) -> (u16, u32) {
    match kind {
        SourceKind::Pipe {
            channels,
            sample_rate,
            ..
        } => (*channels, *sample_rate),
        SourceKind::Spotify { .. } => (2, 44_100),
    }
}

/// A per-process random id, so two servers with identical configs don't collide
/// in the catalog. Ephemeral (re-advertised continuously) so it needn't persist.
fn gen_server_id() -> u64 {
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        let mut b = [0u8; 8];
        if f.read_exact(&mut b).is_ok() {
            return u64::from_le_bytes(b);
        }
    }
    now_epoch_ms() ^ ((std::process::id() as u64) << 32)
}
