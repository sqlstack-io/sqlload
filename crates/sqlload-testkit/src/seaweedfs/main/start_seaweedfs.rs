//! Start SeaweedFS with its S3 API.

use std::time::Duration;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use crate::seaweedfs::constants::{ACCESS_KEY, IDENTITIES_PATH, IMAGE, PROFILE_KEY, S3_PORT, TAG};
use crate::seaweedfs::models::SeaweedFs;

/// A SeaweedFS server whose S3 API accepts only [`ACCESS_KEY`] and [`PROFILE_KEY`], so a request signed with other keys, or not signed, fails as on S3, on a free host port, once its S3 API and its volume server (which writes need) have started. Its volumes take only what is written, not the seven 1 GiB volumes it preallocates by default for each bucket.
pub async fn start_seaweedfs() -> Result<SeaweedFs, String> {
    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(S3_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Start Seaweed S3 API Server"))
        .with_wait_for(WaitFor::message_on_stderr("Start Seaweed volume server"))
        .with_copy_to(IDENTITIES_PATH, identities().into_bytes())
        .with_cmd([
            "server".to_owned(),
            "-s3".to_owned(),
            format!("-s3.config={IDENTITIES_PATH}"),
            "-dir=/data".to_owned(),
            "-master.volumePreallocate=false".to_owned(),
            "-master.volumeSizeLimitMB=256".to_owned(),
        ])
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

/// SeaweedFS's S3 identities: one that may do anything, with both keys.
fn identities() -> String {
    let credentials = [ACCESS_KEY, PROFILE_KEY]
        .map(|(access, secret)| serde_json::json!({ "accessKey": access, "secretKey": secret }));
    serde_json::json!({
        "identities": [{ "name": "sqlload", "credentials": credentials, "actions": ["Admin", "Read", "Write", "List", "Tagging"] }]
    })
    .to_string()
}
