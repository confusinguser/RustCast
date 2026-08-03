//! HTTP control plane (Poem). Lists clients and the source catalog, serves the
//! React UI at `/`, and turns UI edits into multicast [`ControlCommand`]s.
//! Client state (volume/delay/source) lives on the clients and is read here from
//! their telemetry, not stored. Same-origin, so no CORS.

use std::net::{Ipv4Addr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use poem::http::StatusCode;
use poem::listener::TcpListener;
use poem::web::sse::{Event, SSE};
use poem::web::{Data, Html, Json, Path};
use poem::{EndpointExt, Route, Server, delete, get, handler, post, put};
use serde::{Deserialize, Serialize};

use crate::catalog::CatalogStore;
use crate::clients::ClientStore;
use crate::config::{Config, LocalClientConfig, SourceConfig};
use crate::groups::GroupStore;
use crate::metrics::{SourceMeta, TelemetryStore};
use crate::net::set_multicast_if;
use crate::supervisor::SourceRegistry;
use crate::wire::{CONTROL_GROUP, CONTROL_PORT, ControlCommand, MAX_LEAD_MS, WireFormat};

/// The single-page UI, compiled into the binary. Built from the Vite project in
/// `web/`; the committed artifact self-contains all JS + CSS, so `cargo build`
/// needs no Node toolchain.
const INDEX_HTML: &str = include_str!("../web/dist/index.html");

/// This server's own id, so `/api/stats` can mark which streams are remote
/// (hosted by another server).
pub struct LocalServerId(pub u64);

/// Runtime handle for the in-process playback client (see
/// [`crate::config::LocalClientConfig`]). Enables it live; a running client
/// can't be cleanly stopped, so disabling/renaming persists to the config and
/// takes effect on the next restart.
pub struct LocalClientCtl {
    iface: Ipv4Addr,
    running: AtomicBool,
}

impl LocalClientCtl {
    pub fn new(iface: Ipv4Addr, running: bool) -> Self {
        Self {
            iface,
            running: AtomicBool::new(running),
        }
    }

    /// Spawn the in-process client if it isn't already running. `swap` makes the
    /// check-and-set atomic, so concurrent PUTs can't double-spawn.
    fn ensure_running(&self, name: Option<String>) {
        if self.running.swap(true, Ordering::SeqCst) {
            return; // already running
        }
        let iface = self.iface;
        let device_id = Some(format!("{}-local", crate::client::hostname()));
        let _ = std::thread::Builder::new()
            .name("local-client".into())
            .spawn(move || crate::client::run_client(iface, None, device_id, name));
    }
}

/// Multicasts control commands to clients. One per server; shared by handlers.
pub struct ControlSender {
    sock: UdpSocket,
    next_id: AtomicU64,
}

impl ControlSender {
    pub fn new(iface: Ipv4Addr) -> std::io::Result<Self> {
        let sock = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
        sock.set_multicast_ttl_v4(1).ok();
        sock.set_multicast_loop_v4(true).ok();
        if iface != Ipv4Addr::UNSPECIFIED {
            let _ = set_multicast_if(&sock, iface);
        }
        Ok(Self {
            sock,
            next_id: AtomicU64::new(1),
        })
    }

    /// Multicast a command a few times (lossy; re-applying the same value on the
    /// client is a no-op).
    pub fn send(
        &self,
        target: Ipv4Addr,
        source_id: Option<u64>,
        volume: Option<f32>,
        delay_ms: Option<u32>,
    ) {
        let cmd = ControlCommand {
            target_ip: target.octets(),
            cmd_id: self.next_id.fetch_add(1, Ordering::Relaxed),
            set_source_id: source_id,
            set_volume: volume,
            set_delay_ms: delay_ms,
            set_channel_map: None,
        };
        self.emit(&cmd);
    }

    /// Multicast a channel-map change to one client.
    pub fn send_channel_map(&self, target: Ipv4Addr, map: Vec<i16>) {
        let cmd = ControlCommand {
            target_ip: target.octets(),
            cmd_id: self.next_id.fetch_add(1, Ordering::Relaxed),
            set_source_id: None,
            set_volume: None,
            set_delay_ms: None,
            set_channel_map: Some(map),
        };
        self.emit(&cmd);
    }

    /// Serialize and multicast a command a few times (multicast is lossy).
    fn emit(&self, cmd: &ControlCommand) {
        if let Ok(bytes) = bincode::serialize(cmd) {
            for _ in 0..3 {
                let _ = self.sock.send_to(&bytes, (CONTROL_GROUP, CONTROL_PORT));
            }
        }
    }
}

/// A short, stable format label for the UI.
fn format_str(f: WireFormat) -> &'static str {
    match f {
        WireFormat::S16Le => "s16",
        WireFormat::F32Le => "f32",
    }
}

#[derive(Serialize)]
struct ClientDto {
    /// Stable identity (`--id` value, else MAC hex); the UI keys clients by this.
    id: String,
    ip: String,
    /// Display name: the override from clients.json, else the device hostname.
    name: String,
    seconds_ago: f64,
    connected: bool,
    volume: f32,
    delay_ms: u32,
    /// Selected source id as a string; "" means off. (u64 exceeds JS safe ints.)
    selected_source_id: String,
    /// Output device channel count (for the routing matrix).
    output_channels: u16,
    /// Output channel map (one source-channel index per output channel, `-1` =
    /// silence). Empty = default identity mapping.
    channel_map: Vec<i16>,
    /// Group this client belongs to (`null` = ungrouped).
    group_id: Option<String>,
}

/// A client group, for the Clients board. `source_id` is resolved from the
/// stored source name to the current id ("" = none / not in the catalog).
#[derive(Serialize)]
struct GroupDto {
    id: String,
    name: Option<String>,
    source_id: String,
}

#[derive(Serialize)]
struct SourceDto {
    /// Source id as a string (see note above).
    source_id: String,
    name: String,
    source_type: String,
    sample_rate: u32,
    channels: u16,
    format: String,
    /// Send lead (ms) — caps a client's delay slider when it plays this.
    lead_ms: u32,
}

#[derive(Deserialize)]
struct VolumeBody {
    volume: f32,
}

#[derive(Deserialize)]
struct DelayBody {
    delay_ms: u32,
}

#[derive(Deserialize)]
struct SourceBody {
    /// Source id string; `null` or "" selects Off.
    source_id: Option<String>,
}

#[derive(Deserialize)]
struct NameBody {
    /// New display name; `null` or "" clears the override (back to hostname).
    name: Option<String>,
}

#[derive(Deserialize)]
struct ChannelMapBody {
    /// One source-channel index per output channel (`-1` = silence).
    map: Vec<i16>,
}

#[derive(Deserialize)]
struct SendBody {
    /// Any subset; omitted fields are left unchanged.
    #[serde(default)]
    lead_ms: Option<u64>,
    #[serde(default)]
    redundancy: Option<u32>,
    #[serde(default)]
    last_lead_ms: Option<u64>,
    #[serde(default)]
    unicast: Option<bool>,
}

#[handler]
fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

/// The client-meta list (from telemetry) with display names from the store.
fn client_dtos(store: &TelemetryStore, clients_store: &ClientStore) -> Vec<ClientDto> {
    store
        .clients_summary()
        .into_iter()
        .map(|c| {
            let rec = clients_store.get(&c.id);
            // Stored override, else the reported hostname.
            let name = rec
                .as_ref()
                .and_then(|r| r.name.clone())
                .unwrap_or(c.hostname);
            let group_id = rec.and_then(|r| r.group);
            ClientDto {
                id: c.id,
                ip: c.ip.to_string(),
                name,
                seconds_ago: (c.seconds_ago * 10.0).round() / 10.0,
                connected: c.connected,
                volume: c.volume,
                delay_ms: c.delay_ms,
                selected_source_id: if c.selected_source_id == 0 {
                    String::new()
                } else {
                    c.selected_source_id.to_string()
                },
                output_channels: c.output_channels,
                channel_map: c.channel_map,
                group_id,
            }
        })
        .collect()
}

/// The group list, with each group's stored source name resolved to a current
/// source id string ("" when off / not in the catalog).
fn group_dtos(groups: &GroupStore, catalog: &CatalogStore) -> Vec<GroupDto> {
    groups
        .list()
        .into_iter()
        .map(|(id, r)| {
            let source_id = r
                .source
                .as_deref()
                .and_then(|n| source_id_by_name(catalog, n))
                .map(|i| i.to_string())
                .unwrap_or_default();
            GroupDto {
                id,
                name: r.name,
                source_id,
            }
        })
        .collect()
}

/// Look up a catalog source id by name (first match).
fn source_id_by_name(catalog: &CatalogStore, name: &str) -> Option<u64> {
    catalog
        .snapshot()
        .into_iter()
        .find(|r| r.entry.name == name)
        .map(|r| r.entry.source_id)
}

/// Look up a catalog source name by id.
fn source_name_by_id(catalog: &CatalogStore, id: u64) -> Option<String> {
    catalog
        .snapshot()
        .into_iter()
        .find(|r| r.entry.source_id == id)
        .map(|r| r.entry.name)
}

/// The global source catalog for the UI dropdown.
fn catalog_dtos(catalog: &CatalogStore) -> Vec<SourceDto> {
    catalog
        .snapshot()
        .into_iter()
        .map(|r| SourceDto {
            source_id: r.entry.source_id.to_string(),
            name: r.entry.name,
            source_type: r.entry.source_type,
            sample_rate: r.entry.sample_rate,
            channels: r.entry.channels,
            format: format_str(r.entry.format).to_string(),
            lead_ms: r.entry.lead_ms,
        })
        .collect()
}

/// Send-path source metadata (local + remote), for the per-source cards.
fn source_metas(catalog: &CatalogStore, registry: &SourceRegistry, my_id: u64) -> Vec<SourceMeta> {
    catalog
        .snapshot()
        .into_iter()
        .map(|r| {
            let local = registry.params(r.entry.source_id);
            SourceMeta {
                id: r.entry.source_id,
                name: r.entry.name,
                sample_rate: r.entry.sample_rate,
                channels: r.entry.channels,
                lead_ms: r.entry.lead_ms,
                redundancy: local.as_ref().map(|p| p.redundancy()).unwrap_or(0),
                last_lead_ms: local.as_ref().map(|p| p.last_lead()).unwrap_or(0),
                unicast: local.as_ref().map(|p| p.unicast()).unwrap_or(false),
                remote: r.server_id != my_id,
            }
        })
        .collect()
}

/// One SSE payload. `kind` is "snapshot" (full history, applied fresh) or "delta"
/// (only samples newer than the client's cursor, appended). Meta (client list +
/// catalog) is small and included every time.
#[derive(Serialize)]
struct EventPayload {
    #[serde(rename = "type")]
    kind: &'static str,
    now_ms: u64,
    server: Vec<StatsSnapshotServer>,
    clients_hist: Vec<StatsSnapshotClient>,
    clients: Vec<ClientDto>,
    catalog: Vec<SourceDto>,
    groups: Vec<GroupDto>,
}
// Aliases so the SSE payload reuses the store's history structs.
type StatsSnapshotServer = crate::metrics::ServerSourceStats;
type StatsSnapshotClient = crate::metrics::ClientStats;

/// Live stats subscription (Server-Sent Events). On connect, pushes one
/// `snapshot` with the full ~60 s history, then `delta` events every ~200 ms
/// carrying only new samples (per-source/per-client cursors) plus fresh meta.
#[handler]
fn events(
    Data(store): Data<&Arc<TelemetryStore>>,
    Data(catalog): Data<&Arc<CatalogStore>>,
    Data(clients_store): Data<&Arc<ClientStore>>,
    Data(groups_store): Data<&Arc<GroupStore>>,
    Data(registry): Data<&Arc<SourceRegistry>>,
    Data(sid): Data<&Arc<LocalServerId>>,
) -> SSE {
    struct St {
        store: Arc<TelemetryStore>,
        catalog: Arc<CatalogStore>,
        clients_store: Arc<ClientStore>,
        groups_store: Arc<GroupStore>,
        registry: Arc<SourceRegistry>,
        my_id: u64,
        first: bool,
        // cursors: highest sample `t` already sent, per source id / per client id.
        server_cur: std::collections::HashMap<String, u64>,
        client_cur: std::collections::HashMap<String, u64>,
    }
    let st = St {
        store: store.clone(),
        catalog: catalog.clone(),
        clients_store: clients_store.clone(),
        groups_store: groups_store.clone(),
        registry: registry.clone(),
        my_id: sid.0,
        first: true,
        server_cur: std::collections::HashMap::new(),
        client_cur: std::collections::HashMap::new(),
    };

    let stream = futures::stream::unfold(st, |mut st| async move {
        if !st.first {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        let metas = source_metas(&st.catalog, &st.registry, st.my_id);
        let snap = st.store.snapshot(&metas);

        // Keep only samples past each cursor, then advance the cursors.
        let mut server = snap.server;
        for s in &mut server {
            let cur = st.server_cur.entry(s.source_id.clone()).or_insert(0);
            s.samples.retain(|x| x.t > *cur);
            if let Some(last) = s.samples.last() {
                *cur = last.t;
            }
        }
        let mut clients_hist = snap.clients;
        for c in &mut clients_hist {
            let cur = st.client_cur.entry(c.id.clone()).or_insert(0);
            c.samples.retain(|x| x.t > *cur);
            if let Some(last) = c.samples.last() {
                *cur = last.t;
            }
        }

        let payload = EventPayload {
            kind: if st.first { "snapshot" } else { "delta" },
            now_ms: snap.now_ms,
            server,
            clients_hist,
            clients: client_dtos(&st.store, &st.clients_store),
            catalog: catalog_dtos(&st.catalog),
            groups: group_dtos(&st.groups_store, &st.catalog),
        };
        st.first = false;
        let json = serde_json::to_string(&payload).unwrap_or_else(|_| "{}".into());
        Some((Event::message(json), st))
    });

    SSE::new(stream).keep_alive(std::time::Duration::from_secs(15))
}

#[handler]
fn set_volume(
    Path(id): Path<String>,
    Data(store): Data<&Arc<TelemetryStore>>,
    Data(clients_store): Data<&Arc<ClientStore>>,
    Data(control): Data<&Arc<ControlSender>>,
    Json(body): Json<VolumeBody>,
) -> poem::Result<StatusCode> {
    let v = body.volume.clamp(0.0, 1.0);
    clients_store.set_volume(&id, v); // persist (authoritative)
    if let Some(ip) = store.ip_for_id(&id) {
        control.send(ip, None, Some(v), None); // push to the client
    }
    Ok(StatusCode::OK)
}

#[handler]
fn set_delay(
    Path(id): Path<String>,
    Data(store): Data<&Arc<TelemetryStore>>,
    Data(clients_store): Data<&Arc<ClientStore>>,
    Data(control): Data<&Arc<ControlSender>>,
    Json(body): Json<DelayBody>,
) -> poem::Result<StatusCode> {
    // Sanity ceiling only; the client re-clamps to its selected source's lead.
    let d = body.delay_ms.min(MAX_LEAD_MS as u32);
    clients_store.set_delay(&id, d);
    if let Some(ip) = store.ip_for_id(&id) {
        control.send(ip, None, None, Some(d));
    }
    Ok(StatusCode::OK)
}

#[handler]
fn set_source(
    Path(id): Path<String>,
    Data(store): Data<&Arc<TelemetryStore>>,
    Data(control): Data<&Arc<ControlSender>>,
    Json(body): Json<SourceBody>,
) -> poem::Result<StatusCode> {
    // Empty / null / absent => Off (0).
    let source_id: u64 = match body.source_id.as_deref() {
        None | Some("") => 0,
        Some(s) => s
            .parse()
            .map_err(|_| poem::Error::from_status(StatusCode::BAD_REQUEST))?,
    };
    if let Some(ip) = store.ip_for_id(&id) {
        control.send(ip, Some(source_id), None, None);
    }
    Ok(StatusCode::OK)
}

#[handler]
fn set_name(
    Path(id): Path<String>,
    Data(clients_store): Data<&Arc<ClientStore>>,
    Json(body): Json<NameBody>,
) -> poem::Result<StatusCode> {
    clients_store.set_name(&id, body.name);
    Ok(StatusCode::OK)
}

#[handler]
fn set_channel_map(
    Path(id): Path<String>,
    Data(store): Data<&Arc<TelemetryStore>>,
    Data(clients_store): Data<&Arc<ClientStore>>,
    Data(control): Data<&Arc<ControlSender>>,
    Json(body): Json<ChannelMapBody>,
) -> poem::Result<StatusCode> {
    clients_store.set_channel_map(&id, body.map.clone()); // persist (authoritative)
    if let Some(ip) = store.ip_for_id(&id) {
        control.send_channel_map(ip, body.map); // push to the client
    }
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct GroupBody {
    /// Group id to join; `null`/"" leaves the current group.
    #[serde(default)]
    group_id: Option<String>,
}

#[derive(Serialize)]
struct CreatedGroup {
    id: String,
}

/// Create a new, empty group and return its id.
#[handler]
fn create_group(Data(groups): Data<&Arc<GroupStore>>) -> Json<CreatedGroup> {
    Json(CreatedGroup {
        id: groups.create(),
    })
}

/// Delete a group and drop all its members back to ungrouped.
#[handler]
fn delete_group(
    Path(gid): Path<String>,
    Data(groups): Data<&Arc<GroupStore>>,
    Data(clients_store): Data<&Arc<ClientStore>>,
) -> poem::Result<StatusCode> {
    clients_store.clear_group(&gid);
    if groups.delete(&gid) {
        Ok(StatusCode::OK)
    } else {
        Err(poem::Error::from_status(StatusCode::NOT_FOUND))
    }
}

/// Rename a group (`null`/"" clears the name).
#[handler]
fn set_group_name(
    Path(gid): Path<String>,
    Data(groups): Data<&Arc<GroupStore>>,
    Json(body): Json<NameBody>,
) -> poem::Result<StatusCode> {
    if !groups.exists(&gid) {
        return Err(poem::Error::from_status(StatusCode::NOT_FOUND));
    }
    groups.set_name(&gid, body.name);
    Ok(StatusCode::OK)
}

/// Point a group at a source (empty/null = nothing): persist the choice by name
/// and push the source to every current member.
#[handler]
fn set_group_source(
    Path(gid): Path<String>,
    Data(groups): Data<&Arc<GroupStore>>,
    Data(clients_store): Data<&Arc<ClientStore>>,
    Data(catalog): Data<&Arc<CatalogStore>>,
    Data(store): Data<&Arc<TelemetryStore>>,
    Data(control): Data<&Arc<ControlSender>>,
    Json(body): Json<SourceBody>,
) -> poem::Result<StatusCode> {
    if !groups.exists(&gid) {
        return Err(poem::Error::from_status(StatusCode::NOT_FOUND));
    }
    let source_id: u64 = match body.source_id.as_deref() {
        None | Some("") => 0,
        Some(s) => s
            .parse()
            .map_err(|_| poem::Error::from_status(StatusCode::BAD_REQUEST))?,
    };
    // Persist id-independently (see groups module note).
    let name = if source_id == 0 {
        None
    } else {
        source_name_by_id(catalog, source_id)
    };
    groups.set_source(&gid, name);
    for mac in clients_store.members(&gid) {
        if let Some(ip) = store.ip_for_id(&mac) {
            control.send(ip, Some(source_id), None, None);
        }
    }
    Ok(StatusCode::OK)
}

/// Move a client into a group (empty/null = out of its group). Joining a group
/// that already has a source makes the client adopt it immediately.
#[handler]
fn set_client_group(
    Path(id): Path<String>,
    Data(groups): Data<&Arc<GroupStore>>,
    Data(clients_store): Data<&Arc<ClientStore>>,
    Data(catalog): Data<&Arc<CatalogStore>>,
    Data(store): Data<&Arc<TelemetryStore>>,
    Data(control): Data<&Arc<ControlSender>>,
    Json(body): Json<GroupBody>,
) -> poem::Result<StatusCode> {
    let gid = body.group_id.filter(|s| !s.is_empty());
    if let Some(g) = &gid
        && !groups.exists(g)
    {
        return Err(poem::Error::from_status(StatusCode::NOT_FOUND));
    }
    clients_store.set_group(&id, gid.clone());
    // A grouped client plays the group's source, or falls silent (id 0) if the
    // group has none — rather than keeping its old source.
    if let Some(g) = &gid
        && let Some(ip) = store.ip_for_id(&id)
    {
        let sid = groups
            .get(g)
            .and_then(|r| r.source)
            .and_then(|name| source_id_by_name(catalog, &name))
            .unwrap_or(0);
        control.send(ip, Some(sid), None, None);
    }
    Ok(StatusCode::OK)
}

/// Adjust a local source's send timing (lead / redundancy / last-copy lead),
/// applying it live and persisting to the yaml config. 404 for a source id this
/// server doesn't host.
#[handler]
fn set_send(
    Path(id): Path<String>,
    Data(registry): Data<&Arc<SourceRegistry>>,
    Data(config): Data<&Arc<Mutex<Config>>>,
    Data(cfg_path): Data<&Arc<String>>,
    Json(body): Json<SendBody>,
) -> poem::Result<StatusCode> {
    let id: u64 = id
        .parse()
        .map_err(|_| poem::Error::from_status(StatusCode::BAD_REQUEST))?;
    let params = registry
        .params(id)
        .ok_or_else(|| poem::Error::from_status(StatusCode::NOT_FOUND))?;
    if let Some(v) = body.lead_ms {
        params.set_lead(v);
    }
    if let Some(v) = body.redundancy {
        params.set_redundancy(v);
    }
    if let Some(v) = body.last_lead_ms {
        params.set_last_lead(v);
    }
    if let Some(v) = body.unicast {
        params.set_unicast(v);
    }
    persist_sources(config, cfg_path, registry);
    Ok(StatusCode::OK)
}

/// Manually re-anchor a local source's send timeline (resets `start_ms` to now
/// on its next packet, resyncing to real time). 404 for a source id this server
/// doesn't host.
#[handler]
fn reanchor_source(
    Path(id): Path<String>,
    Data(registry): Data<&Arc<SourceRegistry>>,
) -> poem::Result<StatusCode> {
    let id: u64 = id
        .parse()
        .map_err(|_| poem::Error::from_status(StatusCode::BAD_REQUEST))?;
    let params = registry
        .params(id)
        .ok_or_else(|| poem::Error::from_status(StatusCode::NOT_FOUND))?;
    params.request_reanchor();
    Ok(StatusCode::OK)
}

/// Rebuild the config's source list from the live registry (with current
/// send-timing values) and write it to disk. Other fields (interface, local
/// client) are preserved.
fn persist_sources(config: &Arc<Mutex<Config>>, cfg_path: &str, registry: &SourceRegistry) {
    let mut cfg = config.lock().unwrap();
    cfg.sources = registry.configs();
    if let Err(e) = cfg.save(cfg_path) {
        eprintln!("could not persist config: {e}");
    }
}

/// The full server config, for the web-UI config editor. Each source is its flat
/// config plus its derived `id` (a string, since ids exceed JS safe ints) so the
/// editor can target the endpoints.
#[handler]
fn get_config(
    Data(config): Data<&Arc<Mutex<Config>>>,
    Data(sid): Data<&Arc<LocalServerId>>,
) -> Json<serde_json::Value> {
    let cfg = config.lock().unwrap();
    let sources: Vec<serde_json::Value> = cfg
        .sources
        .iter()
        .map(|s| {
            let mut v = serde_json::to_value(s).unwrap_or(serde_json::Value::Null);
            let id = crate::catalog::source_id(sid.0, &s.name);
            if let Some(map) = v.as_object_mut() {
                map.insert("id".into(), serde_json::Value::String(id.to_string()));
            }
            v
        })
        .collect();
    Json(serde_json::json!({
        "interface": cfg.interface.map(|i| i.to_string()),
        "sources": sources,
        // Present (with a name) when an in-process playback client is enabled.
        "local_client": cfg.local_client,
    }))
}

#[derive(Deserialize)]
struct LocalClientBody {
    /// Whether the in-process playback client should run.
    enabled: bool,
    /// Display name for it; `null`/absent falls back to the hostname.
    #[serde(default)]
    name: Option<String>,
}

/// Enable/disable the in-process playback client and set its name, persisting to
/// the config. Enabling starts a player immediately; disabling or renaming takes
/// effect on the next server restart (a running client can't be stopped cleanly).
#[handler]
fn set_local_client(
    Data(registry): Data<&Arc<SourceRegistry>>,
    Data(config): Data<&Arc<Mutex<Config>>>,
    Data(cfg_path): Data<&Arc<String>>,
    Data(local): Data<&Arc<LocalClientCtl>>,
    Json(body): Json<LocalClientBody>,
) -> poem::Result<StatusCode> {
    let name = body.name.filter(|s| !s.trim().is_empty());
    {
        let mut cfg = config.lock().unwrap();
        cfg.local_client = body.enabled.then(|| LocalClientConfig {
            id: None,
            name: name.clone(),
        });
    }
    persist_sources(config, cfg_path, registry);
    if body.enabled {
        local.ensure_running(name);
    }
    Ok(StatusCode::OK)
}

/// Add a new source at runtime (spawns it and persists the config).
#[handler]
fn add_source(
    Data(registry): Data<&Arc<SourceRegistry>>,
    Data(config): Data<&Arc<Mutex<Config>>>,
    Data(cfg_path): Data<&Arc<String>>,
    Json(body): Json<SourceConfig>,
) -> poem::Result<StatusCode> {
    registry
        .spawn(&body)
        .map_err(|e| poem::Error::from_string(e, StatusCode::BAD_REQUEST))?;
    persist_sources(config, cfg_path, registry);
    Ok(StatusCode::OK)
}

/// Replace a source at runtime: remove the old one and spawn the new config.
#[handler]
fn update_source(
    Path(id): Path<String>,
    Data(registry): Data<&Arc<SourceRegistry>>,
    Data(config): Data<&Arc<Mutex<Config>>>,
    Data(cfg_path): Data<&Arc<String>>,
    Json(body): Json<SourceConfig>,
) -> poem::Result<StatusCode> {
    let id: u64 = id
        .parse()
        .map_err(|_| poem::Error::from_status(StatusCode::BAD_REQUEST))?;
    if !registry.contains(id) {
        return Err(poem::Error::from_status(StatusCode::NOT_FOUND));
    }
    registry.remove(id);
    registry
        .spawn(&body)
        .map_err(|e| poem::Error::from_string(e, StatusCode::BAD_REQUEST))?;
    persist_sources(config, cfg_path, registry);
    Ok(StatusCode::OK)
}

/// Remove a source at runtime (stops it and persists the config).
#[handler]
fn delete_source(
    Path(id): Path<String>,
    Data(registry): Data<&Arc<SourceRegistry>>,
    Data(config): Data<&Arc<Mutex<Config>>>,
    Data(cfg_path): Data<&Arc<String>>,
) -> poem::Result<StatusCode> {
    let id: u64 = id
        .parse()
        .map_err(|_| poem::Error::from_status(StatusCode::BAD_REQUEST))?;
    if !registry.remove(id) {
        return Err(poem::Error::from_status(StatusCode::NOT_FOUND));
    }
    persist_sources(config, cfg_path, registry);
    Ok(StatusCode::OK)
}

/// Run the HTTP server on its own tokio runtime. Blocks; intended for a thread.
#[allow(clippy::too_many_arguments)]
pub fn run(
    server_id: u64,
    catalog: Arc<CatalogStore>,
    telemetry: Arc<TelemetryStore>,
    clients_store: Arc<ClientStore>,
    groups_store: Arc<GroupStore>,
    control: Arc<ControlSender>,
    registry: Arc<SourceRegistry>,
    config: Arc<Mutex<Config>>,
    config_path: String,
    local_client: Arc<LocalClientCtl>,
    port: u16,
) {
    let rt = tokio::runtime::Runtime::new().expect("build api runtime");
    rt.block_on(async move {
        let app = Route::new()
            .at("/", get(index))
            .at("/api/events", get(events))
            .at("/api/config", get(get_config))
            .at("/api/config/local_client", put(set_local_client))
            .at("/api/clients/:id/volume", put(set_volume))
            .at("/api/clients/:id/delay", put(set_delay))
            .at("/api/clients/:id/source", put(set_source))
            .at("/api/clients/:id/name", put(set_name))
            .at("/api/clients/:id/channelmap", put(set_channel_map))
            .at("/api/clients/:id/group", put(set_client_group))
            .at("/api/groups", post(create_group))
            .at("/api/groups/:id", delete(delete_group))
            .at("/api/groups/:id/name", put(set_group_name))
            .at("/api/groups/:id/source", put(set_group_source))
            .at("/api/sources", post(add_source))
            .at("/api/sources/:id", put(update_source).delete(delete_source))
            .at("/api/sources/:id/send", put(set_send))
            .at("/api/sources/:id/reanchor", post(reanchor_source))
            .data(catalog)
            .data(telemetry)
            .data(clients_store)
            .data(groups_store)
            .data(control)
            .data(registry)
            .data(config)
            .data(Arc::new(config_path))
            .data(local_client)
            .data(Arc::new(LocalServerId(server_id)));

        println!("HTTP API + UI on http://0.0.0.0:{port}");
        if let Err(e) = Server::new(TcpListener::bind(format!("0.0.0.0:{port}")))
            .run(app)
            .await
        {
            eprintln!("api server error: {e}");
        }
    });
}
