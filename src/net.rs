//! Small UDP socket helpers that `std::net` doesn't expose: pinning the
//! multicast egress interface, and binding with address reuse so several
//! processes on one host can share a multicast port (needed to run multiple
//! servers, or a co-located server and client, on one box for testing).

use std::mem::size_of;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::os::fd::{AsRawFd, FromRawFd};

/// Set the outgoing interface for multicast (`IP_MULTICAST_IF`). `std::net` does
/// not expose this, so we call `setsockopt` directly.
pub fn set_multicast_if(socket: &UdpSocket, iface: Ipv4Addr) -> std::io::Result<()> {
    let addr = libc::in_addr {
        // s_addr is in network byte order; octets() are already network order.
        s_addr: u32::from_ne_bytes(iface.octets()),
    };
    let ret = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_IP,
            libc::IP_MULTICAST_IF,
            &addr as *const libc::in_addr as *const libc::c_void,
            size_of::<libc::in_addr>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Bind a UDP socket with `SO_REUSEADDR`/`SO_REUSEPORT` set first, so multiple
/// listeners can share a multicast group/port on one host. `std::net` binds
/// immediately and gives no hook to set these beforehand, so we build the
/// socket by hand.
pub fn bind_reuse(addr: SocketAddrV4) -> std::io::Result<UdpSocket> {
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let one: libc::c_int = 1;
        for opt in [libc::SO_REUSEADDR, libc::SO_REUSEPORT] {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                opt,
                &one as *const libc::c_int as *const libc::c_void,
                size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        let sa = libc::sockaddr_in {
            sin_family: libc::AF_INET as libc::sa_family_t,
            sin_port: addr.port().to_be(),
            sin_addr: libc::in_addr {
                s_addr: u32::from_ne_bytes(addr.ip().octets()),
            },
            sin_zero: [0; 8],
        };
        let ret = libc::bind(
            fd,
            &sa as *const libc::sockaddr_in as *const libc::sockaddr,
            size_of::<libc::sockaddr_in>() as libc::socklen_t,
        );
        if ret < 0 {
            let e = std::io::Error::last_os_error();
            libc::close(fd);
            return Err(e);
        }
        Ok(UdpSocket::from_raw_fd(fd))
    }
}
