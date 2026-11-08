//! Open a connection through a bastion.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use russh::client::Handle;

use crate::ssh_tunnel::_helpers::session::open_session;
use crate::ssh_tunnel::errors::SshError;
use crate::ssh_tunnel::models::{HostCheck, SshTunnel, TunnelStream};

/// The open sessions, one per bastion and sign-in, shared by every connection through it while it stays open.
type Sessions = tokio::sync::Mutex<HashMap<String, Arc<Handle<HostCheck>>>>;

static SESSIONS: OnceLock<Sessions> = OnceLock::new();

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
    let key = format!(
        "{route}|{:?}|{:?}",
        tunnel.private_key_file, tunnel.known_hosts
    );
    let session = {
        let mut sessions = SESSIONS.get_or_init(Sessions::default).lock().await;
        match sessions.get(&key).filter(|session| !session.is_closed()) {
            Some(session) => session.clone(),
            None => {
                let session = Arc::new(open_session(tunnel).await.map_err(failed)?);
                sessions.insert(key, session.clone());
                session
            }
        }
    };
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
