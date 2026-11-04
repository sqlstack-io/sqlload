pub const IMAGE: &str = "chrislusf/seaweedfs";
pub const TAG: &str = "3.97";
/// Any region works; SeaweedFS ignores it.
pub const REGION: &str = "us-east-1";
pub const S3_PORT: u16 = 8333;
/// Bucket creation attempts while the filer finishes starting.
pub const BUCKET_ATTEMPTS: usize = 30;
pub const BUCKET_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);
