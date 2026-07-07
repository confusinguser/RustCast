//! RustCast client: join the multicast group and play the stream, aligned to
//! each packet's play-at timestamp so all clients stay in sync.
//!
//! Usage: `client` (uses the default group/port from the wire protocol).

use std::net::{Ipv4Addr, UdpSocket};

use rustcast::player;
use rustcast::source::network::NetworkSource;
use rustcast::wire::{DEFAULT_GROUP, DEFAULT_PORT};

fn main() {
    // Optional local interface IP to receive multicast on (for multi-homed
    // hosts); "0.0.0.0" lets the kernel choose.
    let iface: Ipv4Addr = std::env::args()
        .nth(1)
        .map(|s| s.parse().expect("interface must be an IPv4 address"))
        .unwrap_or(Ipv4Addr::UNSPECIFIED);

    // Bind to the multicast port on all interfaces, then join the group. On
    // Linux this lets us receive datagrams sent to DEFAULT_GROUP:DEFAULT_PORT.
    let socket =
        UdpSocket::bind((Ipv4Addr::UNSPECIFIED, DEFAULT_PORT)).expect("bind receive socket");
    socket
        .join_multicast_v4(&DEFAULT_GROUP, &iface)
        .expect("join multicast group");

    println!("Listening on {DEFAULT_GROUP}:{DEFAULT_PORT} (interface {iface}) ...");

    let source = NetworkSource::new(socket).expect("start network source");
    println!("Stream started; playing.");
    player::play(source);
}
