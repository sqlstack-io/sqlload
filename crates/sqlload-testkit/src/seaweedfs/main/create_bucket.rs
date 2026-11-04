//! Create a bucket.

use testcontainers::core::ExecCommand;

use crate::seaweedfs::constants::{BUCKET_ATTEMPTS, BUCKET_RETRY_DELAY};
use crate::seaweedfs::models::SeaweedFs;

/// Create `bucket` through `weed shell` inside the container, retrying while the filer finishes starting.
pub async fn create_bucket(server: &SeaweedFs, bucket: &str) -> Result<(), String> {
    let script = format!(
        "echo 's3.bucket.create -name {bucket}' | weed shell 2>&1; echo 's3.bucket.list' | weed shell 2>&1"
    );
    let listed = format!(" {bucket}\tsize:");
    let mut last_output = String::new();
    for _ in 0..BUCKET_ATTEMPTS {
        let mut result = server
            .container
            .exec(ExecCommand::new(["sh", "-c", script.as_str()]))
            .await
            .map_err(|error| format!("cannot create bucket {bucket}: {error}"))?;
        let output = result
            .stdout_to_vec()
            .await
            .map_err(|error| error.to_string())?;
        last_output = String::from_utf8_lossy(&output).into_owned();
        if last_output.contains(&listed) {
            return Ok(());
        }
        tokio::time::sleep(BUCKET_RETRY_DELAY).await;
    }
    Err(format!("creating bucket {bucket} failed: {last_output}"))
}
