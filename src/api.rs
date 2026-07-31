//! HTTP control plane (Poem). Lists connected clients and the global source
//! catalog, serves the React UI at `/`, and turns UI edits into multicast
//! [`ControlCommand`]s. All client state (volume/delay/selected source) is owned
//! by the clients and read here from their telemetry; this server stores none of
//! it. Same-origin as the API, so no CORS is needed.

use std::net::{Ipv4Addr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use poem::http::StatusCode;
use poem::listener::TcpListener;
use poem::web::{Data, Html, Json, Path};
use poem::{EndpointExt, Route, Server, get, handler, put};
use serde::{Deserialize, Serialize};

use crate::catalog::CatalogStore;
use crate::clients::{ClientStore, mac_hex, parse_mac_hex};
use crate::metrics::{SourceMeta, StatsSnapshot, TelemetryStore, WebActivity};
use crate::net::set_multicast_if;
use crate::wire::{CONTROL_GROUP, CONTROL_PORT, ControlCommand, MAX_DELAY_MS, WireFormat};

/// The single-page UI, compiled into the binary.
const INDEX_HTML: &str = include_str!("../web/index.html");

/// This server's own id, injected so `/api/stats` can mark which streams are
/// remote (hosted by another server).
pub struct LocalServerId(pub u64);

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

    /// Multicast a command a few times (multicast is lossy; the client dedups by
    /// the fact that re-applying the same value is a no-op).
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
        };
        if let Ok(bytes) = bincode::serialize(&cmd) {
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
    /// Stable identity, colon hex; the UI keys and addresses clients by this.
    mac: String,
    ip: String,
    /// Display name: the override from clients.json, else the device hostname.
    name: String,
    seconds_ago: f64,
    connected: bool,
    volume: f32,
    delay_ms: u32,
    /// Selected source id as a string; "" means off. (u64 exceeds JS safe ints.)
    selected_source_id: String,
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
}

#[derive(Serialize)]
struct ClientsResponse {
    clients: Vec<ClientDto>,
    catalog: Vec<SourceDto>,
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

#[handler]
fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

/// The client list (from telemetry) plus the global source catalog for the
/// per-client dropdown. Doubles as the web-user heartbeat.
#[handler]
fn list_clients(
    Data(store): Data<&Arc<TelemetryStore>>,
    Data(catalog): Data<&Arc<CatalogStore>>,
    Data(clients_store): Data<&Arc<ClientStore>>,
    Data(activity): Data<&Arc<WebActivity>>,
) -> Json<ClientsResponse> {
    activity.touch();
    let clients = store
        .clients_summary()
        .into_iter()
        .map(|c| {
            let mac = mac_hex(c.mac);
            // Display name: stored override, else the reported hostname.
            let name = clients_store
                .get(&mac)
                .and_then(|r| r.name)
                .unwrap_or(c.hostname);
            ClientDto {
                mac,
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
            }
        })
        .collect();
    let catalog = catalog
        .snapshot()
        .into_iter()
        .map(|r| SourceDto {
            source_id: r.entry.source_id.to_string(),
            name: r.entry.name,
            source_type: r.entry.source_type,
            sample_rate: r.entry.sample_rate,
            channels: r.entry.channels,
            format: format_str(r.entry.format).to_string(),
        })
        .collect();
    Json(ClientsResponse { clients, catalog })
}

/// Live telemetry for the graphs: per-source server send-path history plus each
/// device's recent buffer/sample history. Also a heartbeat.
#[handler]
fn stats(
    Data(store): Data<&Arc<TelemetryStore>>,
    Data(catalog): Data<&Arc<CatalogStore>>,
    Data(sid): Data<&Arc<LocalServerId>>,
    Data(activity): Data<&Arc<WebActivity>>,
) -> Json<StatsSnapshot> {
    activity.touch();
    // Every source in the catalog (local + remote) gets a send-path card; the
    // history comes from local sampling for our own sources and from the stats
    // broadcast for others.
    let sources: Vec<SourceMeta> = catalog
        .snapshot()
        .into_iter()
        .map(|r| SourceMeta {
            id: r.entry.source_id,
            name: r.entry.name,
            sample_rate: r.entry.sample_rate,
            channels: r.entry.channels,
            lead_ms: r.entry.lead_ms,
            remote: r.server_id != sid.0,
        })
        .collect();
    Json(store.snapshot(&sources))
}

/// Parse a MAC path param into its normalized key form.
fn mac_key(mac: &str) -> poem::Result<[u8; 6]> {
    parse_mac_hex(mac).ok_or_else(|| poem::Error::from_status(StatusCode::BAD_REQUEST))
}

#[handler]
fn set_volume(
    Path(mac): Path<String>,
    Data(store): Data<&Arc<TelemetryStore>>,
    Data(clients_store): Data<&Arc<ClientStore>>,
    Data(control): Data<&Arc<ControlSender>>,
    Json(body): Json<VolumeBody>,
) -> poem::Result<StatusCode> {
    let m = mac_key(&mac)?;
    let key = mac_hex(m);
    let v = body.volume.clamp(0.0, 1.0);
    clients_store.set_volume(&key, v); // persist (authoritative)
    if let Some(ip) = store.ip_for_mac(m) {
        control.send(ip, None, Some(v), None); // push to the client now
    }
    Ok(StatusCode::OK)
}

#[handler]
fn set_delay(
    Path(mac): Path<String>,
    Data(store): Data<&Arc<TelemetryStore>>,
    Data(clients_store): Data<&Arc<ClientStore>>,
    Data(control): Data<&Arc<ControlSender>>,
    Json(body): Json<DelayBody>,
) -> poem::Result<StatusCode> {
    let m = mac_key(&mac)?;
    let key = mac_hex(m);
    let d = body.delay_ms.min(MAX_DELAY_MS);
    clients_store.set_delay(&key, d);
    if let Some(ip) = store.ip_for_mac(m) {
        control.send(ip, None, None, Some(d));
    }
    Ok(StatusCode::OK)
}

#[handler]
fn set_source(
    Path(mac): Path<String>,
    Data(store): Data<&Arc<TelemetryStore>>,
    Data(control): Data<&Arc<ControlSender>>,
    Json(body): Json<SourceBody>,
) -> poem::Result<StatusCode> {
    let m = mac_key(&mac)?;
    // Empty / null / absent => Off (0). Source selection is not persisted (ids
    // change when a server restarts).
    let id: u64 = match body.source_id.as_deref() {
        None | Some("") => 0,
        Some(s) => s
            .parse()
            .map_err(|_| poem::Error::from_status(StatusCode::BAD_REQUEST))?,
    };
    if let Some(ip) = store.ip_for_mac(m) {
        control.send(ip, Some(id), None, None);
    }
    Ok(StatusCode::OK)
}

#[handler]
fn set_name(
    Path(mac): Path<String>,
    Data(clients_store): Data<&Arc<ClientStore>>,
    Json(body): Json<NameBody>,
) -> poem::Result<StatusCode> {
    let key = mac_hex(mac_key(&mac)?);
    clients_store.set_name(&key, body.name);
    Ok(StatusCode::OK)
}

/// Run the HTTP server on its own tokio runtime. Blocks; intended for a thread.
#[allow(clippy::too_many_arguments)]
pub fn run(
    server_id: u64,
    catalog: Arc<CatalogStore>,
    telemetry: Arc<TelemetryStore>,
    clients_store: Arc<ClientStore>,
    activity: Arc<WebActivity>,
    control: Arc<ControlSender>,
    port: u16,
) {
    let rt = tokio::runtime::Runtime::new().expect("build api runtime");
    rt.block_on(async move {
        let app = Route::new()
            .at("/", get(index))
            .at("/api/clients", get(list_clients))
            .at("/api/stats", get(stats))
            .at("/api/clients/:mac/volume", put(set_volume))
            .at("/api/clients/:mac/delay", put(set_delay))
            .at("/api/clients/:mac/source", put(set_source))
            .at("/api/clients/:mac/name", put(set_name))
            .data(catalog)
            .data(telemetry)
            .data(clients_store)
            .data(activity)
            .data(control)
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
