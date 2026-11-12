//! Create a bucket.

use std::time::Duration;

use testcontainers::core::ExecCommand;

use crate::seaweedfs::constants::{BUCKET_ATTEMPT_SECONDS, BUCKET_DEADLINE, BUCKET_RETRY_DELAY};
use crate::seaweedfs::models::SeaweedFs;

/// Create `bucket` through `weed shell` inside the container, retrying while the filer finishes starting; each attempt is cut off after [`BUCKET_ATTEMPT_SECONDS`], and all of them after [`BUCKET_DEADLINE`].
pub async fn create_bucket(server: &SeaweedFs, bucket: &str) -> Result<(), String> {
    let shell = format!("timeout {BUCKET_ATTEMPT_SECONDS} weed shell 2>&1");
    let script = format!(
        "echo 's3.bucket.create -name {bucket}' | {shell}; echo 's3.bucket.list' | {shell}"
    );
    let listed = format!(" {bucket}\tsize:");
    let limit = Duration::from_secs(2 * BUCKET_ATTEMPT_SECONDS + 5);
    let deadline = tokio::time::Instant::now() + BUCKET_DEADLINE;
    let mut last_output = String::new();
    while tokio::time::Instant::now() < deadline {
        let attempt = async {
            let mut result = server
                .container
                .exec(ExecCommand::new(["sh", "-c", script.as_str()]))
                .await
                .map_err(|error| format!("cannot create bucket {bucket}: {error}"))?;
            let output = result
                .stdout_to_vec()
                .await
                .map_err(|error| error.to_string())?;
            Ok::<String, String>(String::from_utf8_lossy(&output).into_owned())
        };
        last_output = match tokio::time::timeout(limit, attempt).await {
            Ok(output) => output?,
            Err(_) => format!("weed shell gave no answer within {} s", limit.as_secs()),
        };
        if last_output.contains(&listed) {
            return Ok(());
        }
        tokio::time::sleep(BUCKET_RETRY_DELAY).await;
    }
    Err(format!(
        "creating bucket {bucket} failed within {} s: {last_output}",
        BUCKET_DEADLINE.as_secs()
    ))
}
