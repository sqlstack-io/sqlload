pub const IMAGE: &str = "chrislusf/seaweedfs";
pub const TAG: &str = "3.97";
/// Any region works; SeaweedFS ignores it.
pub const REGION: &str = "us-east-1";
pub const S3_PORT: u16 = 8333;
/// The keys every test signs S3 requests with (`make test-containers` exports them as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`).
pub const ACCESS_KEY: (&str, &str) = ("test", "test");
/// Other keys SeaweedFS accepts, for tests that must tell keys from a credentials file apart from everything else in their output.
pub const PROFILE_KEY: (&str, &str) = ("AKIAPROFILEKEYID", "profile-secret-key");
/// Where SeaweedFS reads its S3 identities from.
pub const IDENTITIES_PATH: &str = "/etc/seaweedfs/s3.json";
/// How long creating a bucket may take in all, retrying while the filer finishes starting.
pub const BUCKET_DEADLINE: std::time::Duration = std::time::Duration::from_secs(120);
/// How long one `weed shell` attempt may take, inside the container (`timeout`) and around the exec, so a shell that never answers is retried instead of hanging the test.
pub const BUCKET_ATTEMPT_SECONDS: u64 = 15;
/// What one attempt may take beyond its two shells' `timeout`, for the exec itself.
pub const BUCKET_EXEC_MARGIN: std::time::Duration = std::time::Duration::from_secs(5);
pub const BUCKET_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);
