/// The project the fake's key belongs to, and so the one tables load into.
pub const FAKE_PROJECT: &str = "fake-project";
/// Where the fake hands out OAuth tokens.
pub const TOKEN_PATH: &str = "/token";
/// How every request the fake accepts is authorized; the token's own text follows.
pub const TOKEN_PREFIX: &str = "Bearer fake-token-";
/// The Storage Write API's gRPC service.
pub const WRITE_SERVICE: &str = "google.cloud.bigquery.storage.v1.BigQueryWrite";
/// What a stream committed before it was finalized is refused with: `INVALID_STREAM_STATE`.
pub const INVALID_STREAM_STATE: i32 = 5;
/// What an append at the wrong offset is refused with: `OUT_OF_RANGE`.
pub const OUT_OF_RANGE: i32 = 11;
/// What a commit naming a stream the fake does not hold is refused with: `STREAM_NOT_FOUND`.
pub const STREAM_NOT_FOUND: i32 = 3;
/// The Storage Read API's gRPC service.
pub const READ_SERVICE: &str = "google.cloud.bigquery.storage.v1.BigQueryRead";
/// How many streams a read session with rows is split into.
pub const READ_STREAMS: usize = 2;
