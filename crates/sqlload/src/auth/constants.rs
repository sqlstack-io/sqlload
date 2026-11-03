pub const PROGRAMMATIC_ACCESS_TOKEN: &str = "PROGRAMMATIC_ACCESS_TOKEN";
pub const KEYPAIR_JWT: &str = "KEYPAIR_JWT";
/// Snowflake accepts key-pair JWTs for at most one hour.
pub const JWT_LIFETIME_SECS: u64 = 3540;
pub const JWT_REFRESH_MARGIN_SECS: u64 = 300;
