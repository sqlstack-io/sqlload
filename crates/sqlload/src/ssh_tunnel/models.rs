use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use russh::ChannelStream;
use russh::client::{Handle, Msg};
use russh::keys::PublicKeyOrCertificate;
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::auth::models::Secret;
use crate::ssh_tunnel::constants::SSH_PORT;

/// The bastion a database connection is reached through: `ssh = { host, user, ... }` on the connection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SshTunnel {
    pub host: String,
    /// 22 when unset.
    #[serde(default)]
    pub port: Option<u16>,
    pub user: String,
    /// The private key to sign in with (`~/` is the home folder); ssh-agent's keys when unset.
    #[serde(default)]
    pub private_key_file: Option<PathBuf>,
    #[serde(default)]
    pub private_key_passphrase: Option<Secret>,
    /// The file the bastion's host key must be listed in; `~/.ssh/known_hosts` when unset.
    #[serde(default)]
    pub known_hosts: Option<PathBuf>,
}

impl SshTunnel {
    /// The bastion's port.
    pub fn port(&self) -> u16 {
        self.port.unwrap_or(SSH_PORT)
    }

    /// The bastion, as `ssh user@host:port`.
    pub fn route(&self) -> String {
        format!("ssh {}@{}:{}", self.user, self.host, self.port())
    }
}

/// Checks the bastion's host key against `known_hosts`, keeping why it refused one.
#[derive(Debug)]
pub(crate) struct HostCheck {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) known_hosts: PathBuf,
    pub(crate) refusal: Arc<Mutex<Option<String>>>,
}

impl russh::client::Handler for HostCheck {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        key: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        let refusal = crate::ssh_tunnel::_helpers::session::host_key_refusal(self, key);
        let known = refusal.is_none();
        *self.refusal.lock().unwrap_or_else(PoisonError::into_inner) = refusal;
        Ok(known)
    }
}

/// One connection through a tunnel, to the host and port the bastion opened it to; it keeps the tunnel open while it lives.
pub struct TunnelStream {
    pub(crate) stream: ChannelStream<Msg>,
    pub(crate) session: Arc<Handle<HostCheck>>,
    pub(crate) route: String,
    /// The tunnel's session slot, where it notes that it saw the session close.
    pub(crate) key: String,
}

/// The route only: the channel and session have no `Debug` of their own.
impl std::fmt::Debug for TunnelStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TunnelStream")
            .field("route", &self.route)
            .finish_non_exhaustive()
    }
}

impl TunnelStream {
    /// `result`, or the tunnel's closing when it `ended` (the stream ended or failed) because the session closed: then the bastion went away, not the database.
    fn checked<T>(&self, result: io::Result<T>, ended: bool) -> io::Result<T> {
        match ended && self.session.is_closed() {
            true => Err({
                crate::ssh_tunnel::_helpers::session::mark_dropped(&self.key);
                io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    format!("the SSH tunnel through {} closed", self.route),
                )
            }),
            false => result,
        }
    }
}

impl AsyncRead for TunnelStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buffer.filled().len();
        let wanted = buffer.remaining() > 0;
        match Pin::new(&mut self.stream).poll_read(context, buffer) {
            Poll::Ready(Ok(())) => {
                let ended = wanted && buffer.filled().len() == before;
                Poll::Ready(self.checked(Ok(()), ended))
            }
            Poll::Ready(Err(error)) => Poll::Ready(self.checked(Err(error), true)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for TunnelStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.stream).poll_write(context, buffer) {
            Poll::Ready(Ok(written)) => Poll::Ready(self.checked(Ok(written), written == 0)),
            Poll::Ready(Err(error)) => Poll::Ready(self.checked(Err(error), true)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.stream).poll_flush(context) {
            Poll::Ready(Err(error)) => Poll::Ready(self.checked(Err(error), true)),
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(context)
    }
}
