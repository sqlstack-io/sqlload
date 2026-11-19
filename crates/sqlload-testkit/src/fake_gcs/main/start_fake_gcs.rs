//! Start a fake GCS server.

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use crate::fake_gcs::constants::{BUCKET, IMAGE, PORT, TAG};
use crate::fake_gcs::models::FakeGcs;

/// A fake GCS server over plain HTTP on a free host port, holding [`BUCKET`] (seeded with one `.keep` object outside every test's folder). It answers the XML API only for requests to its public host, `127.0.0.1`, which it matches whatever the port.
pub async fn start_fake_gcs() -> Result<FakeGcs, String> {
    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr("server started at"))
        .with_copy_to(format!("/data/{BUCKET}/.keep"), b"keep".to_vec())
        .with_cmd([
            "-scheme",
            "http",
            "-backend",
            "memory",
            "-data",
            "/data",
            "-public-host",
            "127.0.0.1",
        ])
        .start()
        .await
        .map_err(|error| format!("cannot start the fake GCS server: {error}"))?;
    let port = container
        .get_host_port_ipv4(PORT.tcp())
        .await
        .map_err(|error| error.to_string())?;
    Ok(FakeGcs {
        endpoint: format!("http://127.0.0.1:{port}"),
        container,
    })
}
