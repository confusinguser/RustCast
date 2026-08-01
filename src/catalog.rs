//! Source catalog: servers advertise the sources they host; clients and other
//! servers listen to build a global, live view of every source on the LAN.
//!
//! Everything is multicast on [`ANNOUNCE_GROUP`]. A server sends its own catalog
//! with `set_multicast_loop_v4(true)`, so it also receives its own announcement
//! and its sources land in the same [`CatalogStore`] as remote ones — the web UI
//! then renders local and remote sources uniformly.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::net::{bind_reuse, set_multicast_if};
use crate::stream::SendParams;
use crate::wire::{
    ANNOUNCE_GROUP, ANNOUNCE_PORT, CATALOG_REQ_PORT, CatalogAnnounce, CatalogEntry, CatalogRequest,
    now_epoch_ms,
};

/// How often a server re-advertises its catalog.
const ANNOUNCE_INTERVAL_MS: u64 = 3000;

/// Supplies the current catalog entries (with live send params). Read fresh each
/// time so a hot-added/removed source is reflected without a restart.
pub type EntriesProvider = Arc<dyn Fn() -> Vec<(CatalogEntry, Arc<SendParams>)> + Send + Sync>;
/// Drop a server's sources if we haven't heard an announcement in this long
/// (~5 missed announcements).
const ANNOUNCE_STALE: Duration = Duration::from_secs(15);

/// FNV-1a 64-bit hash — small, dependency-free, good enough for stable ids.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Stable, globally-unique source id from the server id + source name. Never 0
/// (0 is reserved to mean "off / no source selected").
pub fn source_id(server_id: u64, name: &str) -> u64 {
    let mut key = server_id.to_le_bytes().to_vec();
    key.extend_from_slice(name.as_bytes());
    let id = fnv1a(&key);
    if id == 0 { 1 } else { id }
}

/// Auto-derive a source's multicast group from its id, within 239.255.128.0/17
/// — kept clear of the fixed 239.255.42.x control/announce/telemetry groups.
pub fn auto_group(source_id: u64) -> Ipv4Addr {
    let b = source_id.to_le_bytes();
    Ipv4Addr::new(239, 255, 0x80 | (b[0] & 0x7f), b[1])
}

/// A resolved source: the advertised entry plus how to reach its owning server
/// for time-sync.
#[derive(Clone)]
pub struct ResolvedSource {
    pub entry: CatalogEntry,
    pub server_id: u64,
    pub server_ip: Ipv4Addr,
    pub sync_port: u16,
}

struct RemoteServer {
    announce: CatalogAnnounce,
    last_seen: Instant,
}

/// Live, merged catalog keyed by server id. Shared by the announce receiver and
/// read by the web UI and (on clients) the selection logic.
pub struct CatalogStore {
    remote: Mutex<HashMap<u64, RemoteServer>>,
}

impl CatalogStore {
    pub fn new() -> Self {
        Self {
            remote: Mutex::new(HashMap::new()),
        }
    }

    /// Record (or refresh) one server's advertised catalog.
    pub fn merge(&self, announce: CatalogAnnounce) {
        let mut m = self.remote.lock().unwrap();
        m.insert(
            announce.server_id,
            RemoteServer {
                announce,
                last_seen: Instant::now(),
            },
        );
    }

    /// Every currently-live source across all servers, sorted for stable UI order.
    pub fn snapshot(&self) -> Vec<ResolvedSource> {
        let mut m = self.remote.lock().unwrap();
        m.retain(|_, r| r.last_seen.elapsed() < ANNOUNCE_STALE);
        let mut out: Vec<ResolvedSource> = Vec::new();
        for r in m.values() {
            let ip = Ipv4Addr::from(r.announce.server_ip);
            for e in &r.announce.sources {
                out.push(ResolvedSource {
                    entry: e.clone(),
                    server_id: r.announce.server_id,
                    server_ip: ip,
                    sync_port: r.announce.sync_port,
                });
            }
        }
        out.sort_by(|a, b| {
            a.entry
                .name
                .cmp(&b.entry.name)
                .then(a.entry.source_id.cmp(&b.entry.source_id))
        });
        out
    }

    /// How to reach one source id, if it is currently advertised.
    pub fn resolve(&self, id: u64) -> Option<ResolvedSource> {
        self.snapshot()
            .into_iter()
            .find(|r| r.entry.source_id == id)
    }

    /// IPs of every server currently heard (for the client's TCP telemetry fan-out).
    pub fn server_ips(&self) -> Vec<Ipv4Addr> {
        let mut m = self.remote.lock().unwrap();
        m.retain(|_, r| r.last_seen.elapsed() < ANNOUNCE_STALE);
        m.values()
            .map(|r| Ipv4Addr::from(r.announce.server_ip))
            .collect()
    }
}

impl Default for CatalogStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Build this server's current [`CatalogAnnounce`], reading each source's live
/// `lead_ms` from its [`SendParams`]. `server_ip` is left 0; the receiver fills
/// it from the datagram's source address. Shared by the announcer and the
/// unicast catalog responder.
pub fn build_announce(
    server_id: u64,
    sync_port: u16,
    sources: &[(CatalogEntry, Arc<SendParams>)],
) -> CatalogAnnounce {
    let entries: Vec<CatalogEntry> = sources
        .iter()
        .map(|(e, p)| {
            let mut e = e.clone();
            e.lead_ms = p.lead() as u32;
            e
        })
        .collect();
    CatalogAnnounce {
        server_id,
        server_ip: [0, 0, 0, 0],
        sync_port,
        sent_ms: now_epoch_ms(),
        sources: entries,
    }
}

/// Server: multicast this server's catalog every [`ANNOUNCE_INTERVAL_MS`]. Each
/// source's `lead_ms` is read live from its [`SendParams`], so a lead adjusted
/// from the UI propagates to clients on the next announcement. Runs forever;
/// intended for its own thread.
pub fn run_catalog_announcer(
    server_id: u64,
    sync_port: u16,
    sources: EntriesProvider,
    iface: Ipv4Addr,
) {
    let sock = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("catalog announcer: could not bind: {e}");
            return;
        }
    };
    sock.set_multicast_ttl_v4(1).ok();
    // Loop back so our own announce (and any co-located server) is received too.
    sock.set_multicast_loop_v4(true).ok();
    if iface != Ipv4Addr::UNSPECIFIED {
        let _ = set_multicast_if(&sock, iface);
    }
    let dest = (ANNOUNCE_GROUP, ANNOUNCE_PORT);
    loop {
        let announce = build_announce(server_id, sync_port, &sources());
        if let Ok(bytes) = bincode::serialize(&announce) {
            let _ = sock.send_to(&bytes, dest);
        }
        std::thread::sleep(Duration::from_millis(ANNOUNCE_INTERVAL_MS));
    }
}

/// Server: answer unicast [`CatalogRequest`]s on [`CATALOG_REQ_PORT`] with this
/// server's current catalog, so a client started with `--server <ip>` can learn
/// the sources without multicast. Runs forever; intended for its own thread.
pub fn run_catalog_responder(server_id: u64, sync_port: u16, sources: EntriesProvider) {
    let sock = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, CATALOG_REQ_PORT)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("catalog responder: could not bind {CATALOG_REQ_PORT}: {e}");
            return;
        }
    };
    let mut buf = [0u8; 2048];
    loop {
        match sock.recv_from(&mut buf) {
            Ok((n, src)) => {
                if bincode::deserialize::<CatalogRequest>(&buf[..n]).is_err() {
                    continue; // ignore junk
                }
                let announce = build_announce(server_id, sync_port, &sources());
                if let Ok(bytes) = bincode::serialize(&announce) {
                    let _ = sock.send_to(&bytes, src);
                }
            }
            Err(e) => {
                eprintln!("catalog responder: recv error: {e}");
                return;
            }
        }
    }
}

/// Client: when started with `--server <ip>`, periodically fetch that server's
/// catalog by unicast and merge it into `store`, overriding the announce's
/// `server_ip` with the known server address. Complements multicast discovery
/// (both feed the same [`CatalogStore`]). Runs forever; intended for its thread.
pub fn run_unicast_catalog_client(server_ip: Ipv4Addr, store: Arc<CatalogStore>) {
    let sock = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("unicast catalog client: could not bind: {e}");
            return;
        }
    };
    let _ = sock.set_read_timeout(Some(Duration::from_millis(1000)));
    let dest = (server_ip, CATALOG_REQ_PORT);
    let mut nonce: u64 = 1;
    let mut buf = [0u8; 65536];
    loop {
        let req = CatalogRequest { nonce };
        nonce = nonce.wrapping_add(1);
        if let Ok(bytes) = bincode::serialize(&req) {
            let _ = sock.send_to(&bytes, dest);
        }
        if let Ok((n, _)) = sock.recv_from(&mut buf)
            && let Ok(mut announce) = bincode::deserialize::<CatalogAnnounce>(&buf[..n])
        {
            announce.server_ip = server_ip.octets();
            store.merge(announce);
        }
        std::thread::sleep(Duration::from_millis(2000));
    }
}

/// Server + client: receive catalog announcements and merge them into `store`.
/// The announcing server's IP is taken from the datagram's source address (not
/// the struct field), so it's always the real reachable address. Runs forever.
pub fn run_catalog_receiver(store: Arc<CatalogStore>, iface: Ipv4Addr) {
    let sock = match bind_reuse(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, ANNOUNCE_PORT)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("catalog receiver: could not bind {ANNOUNCE_PORT}: {e}");
            return;
        }
    };
    if let Err(e) = sock.join_multicast_v4(&ANNOUNCE_GROUP, &iface) {
        eprintln!("catalog receiver: could not join {ANNOUNCE_GROUP}: {e}");
    }
    let mut buf = [0u8; 65536];
    loop {
        match sock.recv_from(&mut buf) {
            Ok((n, SocketAddr::V4(src))) => {
                if let Ok(mut announce) = bincode::deserialize::<CatalogAnnounce>(&buf[..n]) {
                    announce.server_ip = src.ip().octets();
                    store.merge(announce);
                }
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("catalog receiver: recv error: {e}");
                return;
            }
        }
    }
}
