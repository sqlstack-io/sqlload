use std::sync::Arc;

use crate::auth::models::Secret;

/// What the Databricks REST APIs need: the shared HTTP pool, the workspace, the SQL warehouse, and OAuth credentials with the token they last gave.
#[derive(Debug, Clone)]
pub struct DatabricksClient {
    pub(crate) http: reqwest::Client,
    /// The workspace URL, with its scheme and no trailing `/`.
    pub(crate) host: String,
    pub(crate) warehouse_id: String,
    pub(crate) client_id: String,
    pub(crate) client_secret: Secret,
    /// The current access token and when it expires, in milliseconds since the epoch.
    pub(crate) token: Arc<tokio::sync::Mutex<Option<(Secret, i64)>>>,
}

impl DatabricksClient {
    /// A client for `host` (a scheme is added when it has none); nothing is sent until the first request.
    pub fn new(
        http: reqwest::Client,
        host: &str,
        warehouse_id: &str,
        (client_id, client_secret): (&str, Secret),
    ) -> Self {
        let host = host.trim().trim_end_matches('/');
        let host = match host.contains("://") {
            true => host.to_owned(),
            false => format!("https://{host}"),
        };
        Self {
            http,
            host,
            warehouse_id: warehouse_id.to_owned(),
            client_id: client_id.to_owned(),
            client_secret,
            token: Arc::default(),
        }
    }
}

/// One entry of a volume directory listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryEntry {
    /// The full path, `/Volumes/...`.
    pub path: String,
    pub is_directory: bool,
    /// When the file last changed, in milliseconds since the epoch; `None` for directories.
    pub last_modified_ms: Option<i64>,
}
