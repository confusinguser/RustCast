//! HTTP control plane (Poem): lists connected clients and lets the UI set each
//! client's volume. Serves the React UI at `/` and JSON under `/api/*` from the
//! same origin, so no CORS is needed.

use std::net::Ipv4Addr;
use std::sync::Arc;

use poem::http::StatusCode;
use poem::listener::TcpListener;
use poem::web::{Data, Html, Json, Path};
use poem::{EndpointExt, Route, Server, get, handler, put};
use serde::{Deserialize, Serialize};

use crate::metrics::{ServerMetrics, StatsSnapshot, TelemetryStore};
use crate::sync::ClientRegistry;

/// The single-page UI, compiled into the binary.
const INDEX_HTML: &str = include_str!("../web/index.html");

#[derive(Serialize)]
struct ClientDto {
    ip: String,
    seconds_ago: f64,
    volume: f32,
    delay_ms: u32,
    connected: bool,
}

#[derive(Deserialize)]
struct VolumeBody {
    volume: f32,
}

#[derive(Deserialize)]
struct DelayBody {
    delay_ms: u32,
}

#[handler]
fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

#[handler]
fn list_clients(Data(reg): Data<&Arc<ClientRegistry>>) -> Json<Vec<ClientDto>> {
    let clients = reg
        .snapshot()
        .into_iter()
        .map(|c| ClientDto {
            ip: c.ip.to_string(),
            // Round to a tenth of a second for a tidy display.
            seconds_ago: (c.seconds_ago * 10.0).round() / 10.0,
            volume: c.volume,
            delay_ms: c.delay_ms,
            connected: c.connected,
        })
        .collect();
    Json(clients)
}

/// Live telemetry for the graphs: the server send-path history plus each
/// device's recent buffer/sample history. Polled ~1 Hz; each response carries
/// the full ~60 s window (the client reports at 10 Hz, so the graphs stay smooth
/// even though the poll is slow).
#[handler]
fn stats(
    Data(store): Data<&Arc<TelemetryStore>>,
    Data(meta): Data<&Arc<ServerMetrics>>,
) -> Json<StatsSnapshot> {
    Json(store.snapshot(meta))
}

#[handler]
fn set_volume(
    Path(ip): Path<String>,
    Data(reg): Data<&Arc<ClientRegistry>>,
    Json(body): Json<VolumeBody>,
) -> poem::Result<StatusCode> {
    let addr: Ipv4Addr = ip
        .parse()
        .map_err(|_| poem::Error::from_status(StatusCode::BAD_REQUEST))?;
    if reg.set_volume(addr, body.volume) {
        Ok(StatusCode::OK)
    } else {
        Err(poem::Error::from_status(StatusCode::NOT_FOUND))
    }
}

#[handler]
fn set_delay(
    Path(ip): Path<String>,
    Data(reg): Data<&Arc<ClientRegistry>>,
    Json(body): Json<DelayBody>,
) -> poem::Result<StatusCode> {
    let addr: Ipv4Addr = ip
        .parse()
        .map_err(|_| poem::Error::from_status(StatusCode::BAD_REQUEST))?;
    if reg.set_delay(addr, body.delay_ms) {
        Ok(StatusCode::OK)
    } else {
        Err(poem::Error::from_status(StatusCode::NOT_FOUND))
    }
}

/// Run the HTTP server on its own tokio runtime. Blocks; intended for a thread.
pub fn run(
    registry: Arc<ClientRegistry>,
    telemetry: Arc<TelemetryStore>,
    server_metrics: Arc<ServerMetrics>,
    port: u16,
) {
    let rt = tokio::runtime::Runtime::new().expect("build api runtime");
    rt.block_on(async move {
        let app = Route::new()
            .at("/", get(index))
            .at("/api/clients", get(list_clients))
            .at("/api/stats", get(stats))
            .at("/api/clients/:ip/volume", put(set_volume))
            .at("/api/clients/:ip/delay", put(set_delay))
            .data(registry)
            .data(telemetry)
            .data(server_metrics);

        println!("HTTP API + UI on http://0.0.0.0:{port}");
        if let Err(e) = Server::new(TcpListener::bind(format!("0.0.0.0:{port}")))
            .run(app)
            .await
        {
            eprintln!("api server error: {e}");
        }
    });
}
