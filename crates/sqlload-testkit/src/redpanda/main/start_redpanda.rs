//! Start Redpanda.

use std::time::Duration;

use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use crate::redpanda::models::Redpanda;

const IMAGE: &str = "redpandadata/redpanda";
const TAG: &str = "v25.1.4";

/// A one-node broker advertised on a free host port, with topic auto-creation on.
pub async fn start_redpanda() -> Result<Redpanda, String> {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .map_err(|error| error.to_string())?;
    let advertised = format!("127.0.0.1:{port}");
    let container = GenericImage::new(IMAGE, TAG)
        .with_wait_for(WaitFor::message_on_either_std(
            "Successfully started Redpanda!",
        ))
        .with_mapped_port(port, ContainerPort::Tcp(9092))
        .with_cmd([
            "redpanda".to_owned(),
            "start".to_owned(),
            "--mode=dev-container".to_owned(),
            "--smp=1".to_owned(),
            "--memory=512M".to_owned(),
            "--kafka-addr=0.0.0.0:9092".to_owned(),
            format!("--advertise-kafka-addr={advertised}"),
        ])
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .map_err(|error| format!("cannot start Redpanda: {error}"))?;
    Ok(Redpanda {
        brokers: advertised,
        container,
    })
}
