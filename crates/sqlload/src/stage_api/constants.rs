pub const SESSION_EXPIRED: &str = "390112";
pub const QUERY_IN_PROGRESS: [&str; 2] = ["333333", "333334"];
pub const S3_LOCATION: &str = "S3";
/// Starts the error for a stage not on S3, as on an account in another cloud.
pub const NON_S3_STAGE: &str = "only S3-backed internal stages are supported";
pub const SUPPORTED_CIPHER: &str = "AES_CBC";
pub const EXPIRED_TOKEN_CODE: &str = "ExpiredToken";
pub const CREDENTIAL_MARGIN_MS: i64 = 5 * 60 * 1000;
pub const HOST_HEADER: &str = "host";
pub const BAD_REQUEST: u16 = 400;
pub const NOT_FOUND: u16 = 404;
/// The S3 metadata header holding an internal-stage file's wrapped key.
pub const KEY_HEADER: &str = "x-amz-meta-x-amz-key";
/// The S3 metadata header holding an internal-stage file's IV.
pub const IV_HEADER: &str = "x-amz-meta-x-amz-iv";
/// The S3 metadata header holding an internal-stage file's material description.
pub const MATDESC_HEADER: &str = "x-amz-meta-x-amz-matdesc";
