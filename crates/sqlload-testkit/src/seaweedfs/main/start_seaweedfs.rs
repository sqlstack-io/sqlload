//! Start SeaweedFS with its S3 API.

use std::time::Duration;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use crate::seaweedfs::constants::{IMAGE, S3_PORT, TAG};
use crate::seaweedfs::models::SeaweedFs;

/// A SeaweedFS server whose S3 API accepts any credentials, on a free host port, once its S3 API and its volume server (which writes need) have started.
pub async fn start_seaweedfs() -> Result<SeaweedFs, String> {
    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(S3_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Start Seaweed S3 API Server"))
        .with_wait_for(WaitFor::message_on_stderr("Start Seaweed volume server"))
        .with_cmd(["server", "-s3", "-dir=/data"])
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .map_err(|error| format!("cannot start SeaweedFS: {error}"))?;
    let port = container
        .get_host_port_ipv4(S3_PORT.tcp())
        .await
        .map_err(|error| error.to_string())?;
    Ok(SeaweedFs {
        endpoint: format!("http://127.0.0.1:{port}"),
        container,
    })
}
