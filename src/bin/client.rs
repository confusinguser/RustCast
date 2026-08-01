//! RustCast client: discover sources from the multicast catalog, play the one
//! selected for it from the web UI, aligned to each packet's play-at timestamp
//! so all clients stay in sync. Reports its telemetry (and its own settings) to
//! every server; applies control commands addressed to it.
//!
//! Usage: `client [--interface IP] [--server IP] [--id NAME]`
//! Shell completions: `client completions <bash|zsh|fish|...>`
//!
//! The playback stack lives in `rustcast::client` so it can also run in-process
//! inside a server (`local_client` in the config); this binary is a thin wrapper.

use std::net::Ipv4Addr;

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{Shell, generate};

/// Command-line options for the client.
#[derive(Parser)]
#[command(name = "client", about = "RustCast playback client")]
struct Cli {
    /// Local interface IP for multicast on multi-homed hosts (default: kernel chooses).
    #[arg(short, long)]
    interface: Option<Ipv4Addr>,
    /// Also fetch a server's catalog directly by unicast from this IP, for
    /// networks where multicast discovery doesn't route.
    #[arg(short, long)]
    server: Option<Ipv4Addr>,
    /// Stable device id reported to servers (default: the primary NIC MAC in hex).
    #[arg(long)]
    id: Option<String>,
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

    let iface = cli.interface.unwrap_or(Ipv4Addr::UNSPECIFIED);
    rustcast::client::run_client(iface, cli.server, cli.id, None);
}
