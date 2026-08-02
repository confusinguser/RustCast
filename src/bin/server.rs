//! RustCast server: read the sources listed in a YAML config and multicast each
//! as timestamped PCM on its own group. Announces its catalog, answers time-sync,
//! receives client telemetry (only while a web user is present), and serves the
//! control UI.
//!
//! Usage: `server [--config config.yaml]`  (default: `rustcast.yaml`)
//! Shell completions: `server completions <bash|zsh|fish|...>`

use std::io::Read;
use std::net::Ipv4Addr;
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{Shell, generate};

use rustcast::api::{self, ControlSender, LocalClientCtl};
use rustcast::catalog::{
    CatalogStore, EntriesProvider, run_catalog_announcer, run_catalog_receiver,
    run_catalog_responder,
};
use rustcast::clients::ClientStore;
use rustcast::config::Config;
use rustcast::metrics::{
    SamplerProvider, TelemetryStore, run_stats_broadcaster, run_stats_receiver,
    run_telemetry_receiver,
};
use rustcast::supervisor::SourceRegistry;
use rustcast::sync::{Listeners, run_server_responder};
use rustcast::wire::{DEFAULT_SYNC_PORT, now_epoch_ms};

/// Port for the HTTP control API + web UI.
const HTTP_PORT: u16 = 8080;
/// How often each source's send-path metrics are sampled into history (~10 Hz).
const SERVER_SAMPLE_MS: u64 = 100;
/// Where per-client settings are persisted.
const CLIENTS_JSON: &str = "clients.json";
const GROUPS_JSON: &str = "groups.json";
/// How often the reconciler enforces persisted settings onto clients.
const RECONCILE_MS: u64 = 500;

/// Command-line options for the server.
#[derive(Parser)]
#[command(name = "server", about = "RustCast streaming server")]
struct Cli {
    /// Path to the YAML config file.
    #[arg(short, long, default_value = "rustcast.yaml")]
    config: String,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Print a shell completion script to stdout (e.g. `completions fish`).
    Completions { shell: Shell },
}

fn main() {
    let cli = Cli::parse();

    // Completion generation is a standalone mode: print the script and exit.
    if let Some(Command::Completions { shell }) = cli.command {
        let mut cmd = Cli::command();
        let name = cmd.get_name().to_string();
        generate(shell, &mut cmd, name, &mut std::io::stdout());
        return;
    }

    let config_path = cli.config;
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

    // Who's listening to each source (learned from time-sync requests); gates
    // idle sources and targets unicast mode.
    let listeners = Arc::new(Listeners::new());

    // The live source set. Spawn each configured source through it; add/remove at
    // runtime from the web UI go through the same registry (hot-reload).
    let registry = Arc::new(SourceRegistry::new(server_id, iface, listeners.clone()));
    for s in &config.sources {
        if let Err(e) = registry.spawn(s) {
            eprintln!("source '{}': {e}", s.name);
        }
    }

    // A playback client inside this process, if configured (the server machine
    // also plays). Reaches its own server over loopback multicast. The ctl lets
    // the web UI enable one live; `running` starts true when config already has
    // one so the API won't double-spawn it.
    let local_client = config.local_client.clone();
    let local_ctl = Arc::new(LocalClientCtl::new(iface, local_client.is_some()));

    // Config is shared so the API can persist source + send-timing edits.
    let config = Arc::new(Mutex::new(config));

    // Shared state.
    let catalog_store = Arc::new(CatalogStore::new());
    let telemetry = Arc::new(TelemetryStore::new());
    let control = Arc::new(ControlSender::new(iface).expect("bind control socket"));
    // Durable per-client settings, keyed by device id.
    let clients_store = Arc::new(ClientStore::load(CLIENTS_JSON));
    // Durable client groups.
    let groups_store = Arc::new(rustcast::groups::GroupStore::load(GROUPS_JSON));

    // Live providers: read the current source set fresh each time, so the
    // announcer / stats threads reflect hot-added/removed sources.
    let entries_provider: EntriesProvider = {
        let r = registry.clone();
        Arc::new(move || r.entries())
    };
    let sampler_provider: SamplerProvider = {
        let r = registry.clone();
        Arc::new(move || r.samplers())
    };

    println!("RustCast server {server_id:016x}: UI on http://0.0.0.0:{HTTP_PORT}");

    // Always-on service threads.
    {
        let catalog = catalog_store.clone();
        let telemetry = telemetry.clone();
        let clients = clients_store.clone();
        let groups = groups_store.clone();
        let control = control.clone();
        let registry = registry.clone();
        let config = config.clone();
        let config_path = config_path.clone();
        let local_ctl = local_ctl.clone();
        std::thread::spawn(move || {
            api::run(
                server_id,
                catalog,
                telemetry,
                clients,
                groups,
                control,
                registry,
                config,
                config_path,
                local_ctl,
                HTTP_PORT,
            )
        });
    }
    // Reconcile persisted settings onto clients: adopt a new client's reported
    // values, and push the stored volume/delay/source to a (re)connecting client
    // whose live values differ — this is what makes settings survive restarts.
    {
        let telemetry = telemetry.clone();
        let clients = clients_store.clone();
        let control = control.clone();
        let registry = registry.clone();
        std::thread::spawn(move || {
            // Clients whose stored source we've already tried to restore this
            // session, so we don't fight the user's later selections.
            let mut restored: std::collections::HashSet<String> = std::collections::HashSet::new();
            loop {
                for c in telemetry.clients_summary() {
                    let rec = clients.get_or_create(&c.id, c.volume, c.delay_ms);
                    if (rec.volume - c.volume).abs() > f32::EPSILON {
                        control.send(c.ip, None, Some(rec.volume), None);
                    }
                    if rec.delay_ms != c.delay_ms {
                        control.send(c.ip, None, None, Some(rec.delay_ms));
                    }
                    // Restore a non-default channel map the client isn't yet using.
                    if !rec.channel_map.is_empty() && rec.channel_map != c.channel_map {
                        control.send_channel_map(c.ip, rec.channel_map.clone());
                    }

                    if restored.insert(c.id.clone()) {
                        // First sight this session: restore the stored source (by
                        // name → current id), if this server still hosts it.
                        if let Some(name) = &rec.source
                            && let Some(id) = registry.id_for_name(name)
                            && c.selected_source_id != id
                        {
                            control.send(c.ip, Some(id), None, None);
                        }
                    } else {
                        // Steady state: track what the client is playing. Store the
                        // source name only while it's one of *ours*; None when off
                        // or playing another server's source.
                        let cur = if c.selected_source_id == 0 {
                            None
                        } else {
                            registry.name(c.selected_source_id)
                        };
                        if cur != rec.source {
                            clients.set_source_name(&c.id, cur);
                        }
                    }
                }
                std::thread::sleep(Duration::from_millis(RECONCILE_MS));
            }
        });
    }
    {
        let listeners = listeners.clone();
        std::thread::spawn(move || run_server_responder(DEFAULT_SYNC_PORT, listeners));
    }
    {
        let entries = entries_provider.clone();
        std::thread::spawn(move || {
            run_catalog_announcer(server_id, DEFAULT_SYNC_PORT, entries, iface)
        });
    }
    // Answer unicast catalog requests (for clients started with --server <ip>).
    {
        let entries = entries_provider.clone();
        std::thread::spawn(move || run_catalog_responder(server_id, DEFAULT_SYNC_PORT, entries));
    }
    {
        let catalog = catalog_store.clone();
        std::thread::spawn(move || run_catalog_receiver(catalog, iface));
    }
    // Accept client telemetry over TCP (clients connect to us directly).
    {
        let telemetry = telemetry.clone();
        std::thread::spawn(move || run_telemetry_receiver(telemetry));
    }
    // Learn other servers' send-path stats so this UI graphs their streams too.
    {
        let telemetry = telemetry.clone();
        std::thread::spawn(move || run_stats_receiver(telemetry, server_id, iface));
    }
    // Broadcast our own sources' send-path stats to other servers.
    {
        let samplers = sampler_provider.clone();
        std::thread::spawn(move || run_stats_broadcaster(server_id, samplers, iface));
    }
    // Sample each source's send-path metrics into the history at ~10 Hz.
    {
        let telemetry = telemetry.clone();
        let samplers = sampler_provider.clone();
        std::thread::spawn(move || {
            loop {
                for (id, m) in samplers() {
                    telemetry.push_server(id, m.snapshot());
                }
                std::thread::sleep(Duration::from_millis(SERVER_SAMPLE_MS));
            }
        });
    }

    // Optionally run a playback client in-process (server also plays).
    if let Some(lc) = local_client {
        let device_id = lc
            .id
            .or_else(|| Some(format!("{}-local", rustcast::client::hostname())));
        std::thread::Builder::new()
            .name("local-client".into())
            .spawn(move || rustcast::client::run_client(iface, None, device_id, lc.name))
            .expect("spawn local client");
    }

    // The source streams run in the registry's own threads; keep the process
    // alive (the registry can be mutated at runtime from the web UI).
    loop {
        std::thread::sleep(Duration::from_secs(3600));
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
