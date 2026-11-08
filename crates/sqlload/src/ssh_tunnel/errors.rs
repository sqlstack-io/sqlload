/// Why a tunnel through a bastion could not be opened.
#[derive(Debug, thiserror::Error)]
#[error("cannot open the SSH tunnel through {route}: {reason}")]
pub struct SshError {
    /// The bastion, as `ssh user@host:port`.
    pub route: String,
    pub reason: String,
}
