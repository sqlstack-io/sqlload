use std::sync::Arc;

use object_store::ObjectStore;

use crate::store_paths::models::Location;

/// An opened folder: the store client and the folder within it.
#[derive(Debug, Clone)]
pub struct OpenFolder {
    pub store: Arc<dyn ObjectStore>,
    pub location: Location,
}

/// One data file under a folder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredFile {
    /// The store path as listed, used for every read so keys are never escaped twice.
    pub path: object_store::path::Path,
    /// The key relative to the folder, such as `date=2026-11-04/part-0.parquet`.
    pub relative_key: String,
    pub size: u64,
    /// The store's version tag, used to notice a file rewritten in place.
    pub etag: Option<String>,
    pub modified_ms: i64,
}

impl StoredFile {
    /// The file's version: its ETag, or its size and modification time when the store has none.
    pub fn version(&self) -> String {
        match &self.etag {
            Some(etag) => etag.trim_matches('"').to_owned(),
            None => format!("{}-{}", self.size, self.modified_ms),
        }
    }
}

/// The files on an SFTP server, read over one SFTP session opened on first use: `root` is `/`, or empty for the user's login folder. Reads only: SQLLoad never writes to, renames or deletes from an SFTP server.
pub struct SftpStore {
    pub server: crate::ssh_tunnel::models::SshTunnel,
    pub root: String,
    pub(crate) link: Arc<tokio::sync::OnceCell<SftpLink>>,
}

/// The server only: the SFTP session has no `Debug` of its own.
impl std::fmt::Debug for SftpStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "SftpStore({}, root {:?})",
            self.server.route(),
            self.root
        )
    }
}

impl std::fmt::Display for SftpStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "SFTP({})", self.server.route())
    }
}

/// An open SFTP session, and the SSH session it runs on, which must outlive it.
pub(crate) struct SftpLink {
    pub(crate) sftp: russh_sftp::client::SftpSession,
    pub(crate) _session: Arc<russh::client::Handle<crate::ssh_tunnel::models::HostCheck>>,
}
