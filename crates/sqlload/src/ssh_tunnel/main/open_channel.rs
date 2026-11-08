//! Open a connection through a bastion.

use crate::ssh_tunnel::_helpers::session::shared_session;
use crate::ssh_tunnel::errors::SshError;
use crate::ssh_tunnel::models::{SshTunnel, TunnelStream};

/// A connection to `host:port` as the bastion of `tunnel` sees them, over its one session, which opens on first use and again after it closes. No local port is opened, so no other process can use the tunnel.
pub async fn open_channel(
    tunnel: &SshTunnel,
    host: &str,
    port: u16,
) -> Result<TunnelStream, SshError> {
    let route = tunnel.route();
    let failed = |reason: String| SshError {
        route: route.clone(),
        reason,
    };
    let session = shared_session(tunnel).await.map_err(failed)?;
    let channel = session
        .channel_open_direct_tcpip(host, u32::from(port), "127.0.0.1", 0)
        .await
        .map_err(|error| failed(format!("the bastion cannot open {host}:{port}: {error}")))?;
    Ok(TunnelStream {
        stream: channel.into_stream(),
        session,
        route,
    })
}
