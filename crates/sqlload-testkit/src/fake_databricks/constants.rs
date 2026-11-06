/// The staging volume every fake workspace has, as a connection names it.
pub const FAKE_VOLUME: &str = "main.raw.stage";
/// The same volume as the Files API addresses it.
pub const FAKE_VOLUME_PATH: &str = "/Volumes/main/raw/stage";
/// The catalog the fake's tables live in.
pub const FAKE_CATALOG: &str = "main";
/// Where the fake hands out OAuth tokens.
pub const TOKEN_PATH: &str = "/oidc/v1/token";
/// How every request the fake accepts is authorized; the token's own text follows.
pub const TOKEN_PREFIX: &str = "Bearer fake-token-";
