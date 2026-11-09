//! Open an SFTP session with an SSH server.

use std::sync::Arc;

use russh::client::Handle;
use russh_sftp::client::{Config, SftpSession};

use crate::ssh_tunnel::_helpers::session::shared_session;
use crate::ssh_tunnel::constants::SFTP_REQUEST_TIMEOUT;
use crate::ssh_tunnel::models::{HostCheck, SshTunnel};

/// An SFTP session with `server`, as the `sftp` subsystem of the one SSH session SQLLoad keeps with it (shared with tunnels through the same server and sign-in), and that session, which must outlive it.
pub(crate) async fn open_sftp(
    server: &SshTunnel,
) -> Result<(SftpSession, Arc<Handle<HostCheck>>), String> {
    let session = shared_session(server).await?;
    let channel = session
        .channel_open_session()
        .await
        .map_err(|error| format!("cannot open a channel: {error}"))?;
    channel
        .request_subsystem(true, "sftp")
        .await
        .map_err(|error| format!("cannot start SFTP: {error}"))?;
    let config = Config {
        request_timeout_secs: SFTP_REQUEST_TIMEOUT.as_secs(),
        ..Config::default()
    };
    let sftp = SftpSession::new_with_config(channel.into_stream(), config)
        .await
        .map_err(|error| format!("the server does not speak SFTP: {error}"))?;
    Ok((sftp, session))
}
