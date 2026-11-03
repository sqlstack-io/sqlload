//! Read the exporter's metrics endpoint.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

/// The text of `GET /metrics` from the endpoint listening on `address`.
pub async fn read_metrics(address: SocketAddr) -> Result<String, String> {
    tokio::task::spawn_blocking(move || {
        let mut stream = TcpStream::connect(address).map_err(|error| error.to_string())?;
        stream
            .write_all(b"GET /metrics HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .map_err(|error| error.to_string())?;
        let mut text = String::new();
        stream
            .read_to_string(&mut text)
            .map_err(|error| error.to_string())?;
        Ok(text)
    })
    .await
    .map_err(|error| error.to_string())?
}
