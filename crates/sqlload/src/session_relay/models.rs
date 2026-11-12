/// Where the client reaches a relayed session, and the application name its startup message must carry there.
#[derive(Debug)]
pub struct RelayEndpoint {
    /// The relay's socket folder (Unix) or `127.0.0.1` (Windows).
    pub host: String,
    pub port: u16,
    /// `sqlload` on Unix. On Windows `sqlload-<secret of this relay>`, which the relay checks and replaces with `sqlload` before the server sees it.
    pub application_name: crate::auth::models::Secret,
}
