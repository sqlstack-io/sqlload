//! tokio-postgres's TLS hooks, by rustls: the client asks for TLS itself and calls these only when the server agrees.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use rustls::ClientConfig;
use rustls::pki_types::{InvalidDnsNameError, ServerName};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_postgres::tls::{ChannelBinding, MakeTlsConnect, TlsConnect, TlsStream};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream as RustlsStream;

use crate::postgres_source::_helpers::tls::server_end_point;

/// Makes the handshake for each host: `encrypted` tells whether the last one completed, `refused` whether it failed.
#[derive(Clone)]
pub(crate) struct RustlsConnect {
    pub(crate) config: Arc<ClientConfig>,
    pub(crate) encrypted: Arc<AtomicBool>,
    pub(crate) refused: Arc<AtomicBool>,
}

impl<S> MakeTlsConnect<S> for RustlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = Encrypted<S>;
    type TlsConnect = Handshake;
    type Error = io::Error;

    /// Called for every host before asking for TLS, a Unix socket with an empty `domain` included: a name that cannot be checked fails the handshake, should the server agree to one, not the connection.
    fn make_tls_connect(&mut self, domain: &str) -> Result<Handshake, io::Error> {
        self.encrypted.store(false, Ordering::Relaxed);
        self.refused.store(false, Ordering::Relaxed);
        Ok(Handshake {
            connect: self.clone(),
            name: ServerName::try_from(domain.to_owned()),
        })
    }
}

/// The TLS handshake with one host.
pub(crate) struct Handshake {
    connect: RustlsConnect,
    name: Result<ServerName<'static>, InvalidDnsNameError>,
}

impl<S> TlsConnect<S> for Handshake
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = Encrypted<S>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<Encrypted<S>>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        let connector = TlsConnector::from(self.connect.config);
        let RustlsConnect {
            encrypted, refused, ..
        } = self.connect;
        Box::pin(async move {
            let name = self
                .name
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            let session = connector
                .connect(name, stream)
                .await
                .inspect_err(|_| refused.store(true, Ordering::Relaxed))?;
            encrypted.store(true, Ordering::Relaxed);
            Ok(Encrypted(session))
        })
    }
}

/// A TLS session over the client's socket.
pub(crate) struct Encrypted<S>(RustlsStream<S>);

impl<S: AsyncRead + AsyncWrite + Unpin> TlsStream for Encrypted<S> {
    /// `tls-server-end-point`, as libpq's default `channel_binding = prefer` sends, so an attacker who only relays SCRAM fails even when the certificate is not checked (one who changes how the client signs in does not; only `verify-full` stops that); none when the certificate's hash is not one this binds to.
    fn channel_binding(&self) -> ChannelBinding {
        self.0
            .get_ref()
            .1
            .peer_certificates()
            .and_then(<[_]>::first)
            .and_then(|certificate| server_end_point(certificate))
            .map_or_else(ChannelBinding::none, ChannelBinding::tls_server_end_point)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for Encrypted<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(context, buffer)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for Encrypted<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(context)
    }
}
