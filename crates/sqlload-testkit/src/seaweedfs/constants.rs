pub const IMAGE: &str = "chrislusf/seaweedfs";
pub const TAG: &str = "3.97";
/// Any region works; SeaweedFS ignores it.
pub const REGION: &str = "us-east-1";
pub const S3_PORT: u16 = 8333;
/// How long creating a bucket may take in all, retrying while the filer finishes starting.
pub const BUCKET_DEADLINE: std::time::Duration = std::time::Duration::from_secs(120);
/// How long one `weed shell` attempt may take, inside the container (`timeout`) and around the exec, so a shell that never answers is retried instead of hanging the test.
pub const BUCKET_ATTEMPT_SECONDS: u64 = 15;
/// What one attempt may take beyond its two shells' `timeout`, for the exec itself.
pub const BUCKET_EXEC_MARGIN: std::time::Duration = std::time::Duration::from_secs(5);
pub const BUCKET_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);
