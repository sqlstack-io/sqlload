//! Whether a tunnel went away under its connections.

use crate::ssh_tunnel::_helpers::session::session_closed;
use crate::ssh_tunnel::models::SshTunnel;

/// Whether the session with the bastion of `tunnel` that this run opened has closed, so a connection through it that failed (a replication stream, say) failed because the tunnel went, not the database.
pub fn tunnel_closed(tunnel: &SshTunnel) -> bool {
    session_closed(tunnel)
}
