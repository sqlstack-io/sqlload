//! Files on an SFTP server, read through the object-store interface the other stores use.

use std::io::SeekFrom;

use async_trait::async_trait;
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use russh_sftp::client::SftpSession;
use russh_sftp::client::error::Error as SftpError;
use russh_sftp::protocol::{FileAttributes, StatusCode};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::OnceCell;

use crate::file_store::constants::HIDDEN_PREFIXES;
use crate::file_store::models::{SftpLink, SftpStore};
use crate::ssh_tunnel::main::open_sftp::open_sftp;
use crate::ssh_tunnel::models::SshTunnel;

/// The store's name in object-store errors.
const STORE: &str = "SFTP";

/// The most bytes one chunk of a streamed read holds: about one SFTP read reply.
const READ_CHUNK: u64 = 256 * 1024;

/// The session in `link`, opened on first use; the host key is checked against `known_hosts` before anything is listed.
async fn link<'a>(
    link: &'a OnceCell<SftpLink>,
    server: &SshTunnel,
) -> Result<&'a SftpLink, object_store::Error> {
    link.get_or_try_init(|| async {
        let (sftp, session) =
            open_sftp(server)
                .await
                .map_err(|reason| object_store::Error::Generic {
                    store: STORE,
                    source: format!("cannot open SFTP with {}: {reason}", server.route()).into(),
                })?;
        Ok(SftpLink {
            sftp,
            _session: session,
        })
    })
    .await
}

/// The server's path for store path `key`: absolute below `/`, or relative to the login folder.
fn remote(root: &str, key: &str) -> String {
    match (root, key) {
        ("/", key) => format!("/{key}"),
        (_, "") => ".".to_owned(),
        (_, key) => key.to_owned(),
    }
}

/// `error` from the server about `key`, as an object-store error.
fn failed(key: &str, error: SftpError) -> object_store::Error {
    let code = match &error {
        SftpError::Status(status) => Some(status.status_code),
        _ => None,
    };
    match code {
        Some(StatusCode::NoSuchFile) => object_store::Error::NotFound {
            path: key.to_owned(),
            source: Box::new(error),
        },
        Some(StatusCode::PermissionDenied) => object_store::Error::PermissionDenied {
            path: key.to_owned(),
            source: Box::new(error),
        },
        _ => object_store::Error::Generic {
            store: STORE,
            source: format!("{key}: {error}").into(),
        },
    }
}

/// Why a change to the server is refused.
fn read_only(operation: &str) -> object_store::Error {
    object_store::Error::NotSupported {
        source: format!("SQLLoad only reads from SFTP servers; it does not {operation}").into(),
    }
}

/// The listing entry of the file `key`: its size and modification time, which make its version (SFTP has no ETag), so a server that gives no modification time is refused.
fn object_meta(key: &str, attributes: &FileAttributes) -> Result<ObjectMeta, object_store::Error> {
    let mtime = attributes.mtime.ok_or_else(|| object_store::Error::Generic {
        store: STORE,
        source: format!(
            "{key}: the server gives no modification time, which SQLLoad needs to notice a rewritten file"
        )
        .into(),
    })?;
    Ok(ObjectMeta {
        location: Path::parse(key)?,
        last_modified: DateTime::<Utc>::from_timestamp(i64::from(mtime), 0).unwrap_or_default(),
        size: attributes.len(),
        e_tag: None,
        version: None,
    })
}

/// The regular files and the folders directly in folder `key` (store paths), or nothing when it does not exist, as for the other stores. Links, other special files and hidden (`_`, `.`) files and folders are skipped, so a hidden folder is never walked.
async fn read_folder(
    sftp: &SftpSession,
    root: &str,
    key: &str,
) -> Result<(Vec<ObjectMeta>, Vec<String>), object_store::Error> {
    let entries = match sftp
        .read_dir(remote(root, key))
        .await
        .map_err(|error| failed(key, error))
    {
        Err(object_store::Error::NotFound { .. }) => return Ok((Vec::new(), Vec::new())),
        listed => listed?,
    };
    let mut files: Vec<ObjectMeta> = Vec::new();
    let mut folders: Vec<String> = Vec::new();
    for entry in entries {
        if entry.file_name().starts_with(HIDDEN_PREFIXES) {
            continue;
        }
        let child = match key.is_empty() {
            true => entry.file_name(),
            false => format!("{key}/{}", entry.file_name()),
        };
        let file_type = entry.file_type();
        if file_type.is_dir() {
            folders.push(child);
        } else if file_type.is_file() {
            files.push(object_meta(&child, &entry.metadata())?);
        }
    }
    Ok((files, folders))
}

/// Every regular file below folder `key`, walking its folders.
async fn walk(
    sftp: &SftpSession,
    root: &str,
    key: String,
) -> Result<Vec<ObjectMeta>, object_store::Error> {
    let mut pending = vec![key];
    let mut found: Vec<ObjectMeta> = Vec::new();
    while let Some(folder) = pending.pop() {
        let (files, folders) = read_folder(sftp, root, &folder).await?;
        found.extend(files);
        pending.extend(folders);
    }
    Ok(found)
}

#[async_trait]
impl ObjectStore for SftpStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(read_only("write files"))
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(read_only("write files"))
    }

    /// The file, or the range of it `options` asks for, streamed as it arrives, so a large file is never held whole; a file that ends before the range does (rewritten while being read) fails the read.
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let sftp = &link(&self.link, &self.server).await?.sftp;
        let key = location.as_ref();
        let mut file = sftp
            .open(remote(&self.root, key))
            .await
            .map_err(|error| failed(key, error))?;
        let attributes = file.metadata().await.map_err(|error| failed(key, error))?;
        let meta = object_meta(key, &attributes)?;
        options.check_preconditions(&meta)?;
        let range = match &options.range {
            Some(range) => {
                range
                    .as_range(meta.size)
                    .map_err(|error| object_store::Error::Generic {
                        store: STORE,
                        source: Box::new(error),
                    })?
            }
            None => 0..meta.size,
        };
        let owned_key = key.to_owned();
        let io_failed = move |error: std::io::Error| object_store::Error::Generic {
            store: STORE,
            source: format!("{owned_key}: {error}").into(),
        };
        let payload = match options.head {
            true => stream::empty().boxed(),
            false => {
                file.seek(SeekFrom::Start(range.start))
                    .await
                    .map_err(io_failed.clone())?;
                stream::try_unfold(
                    (file, range.end - range.start),
                    |(mut file, remaining)| async move {
                        if remaining == 0 {
                            return Ok(None);
                        }
                        let mut chunk = BytesMut::with_capacity(
                            usize::try_from(remaining.min(READ_CHUNK)).unwrap_or(usize::MAX),
                        );
                        match file.read_buf(&mut chunk).await? {
                            0 => Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                format!("the file ended {remaining} bytes early; was it rewritten while being read?"),
                            )),
                            read => Ok(Some((chunk.freeze(), (file, remaining - read as u64)))),
                        }
                    },
                )
                .map_err(io_failed)
                .boxed()
            }
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta,
            range,
            attributes: Attributes::default(),
            extensions: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        locations
            .map(|location| location.and_then(|_| Err(read_only("delete files"))))
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let cell = self.link.clone();
        let server = self.server.clone();
        let root = self.root.clone();
        let key = prefix.map(Path::to_string).unwrap_or_default();
        stream::once(async move {
            let sftp = &link(&cell, &server).await?.sftp;
            walk(sftp, &root, key).await
        })
        .flat_map(|listed| {
            stream::iter(match listed {
                Ok(found) => found.into_iter().map(Ok).collect::<Vec<_>>(),
                Err(error) => vec![Err(error)],
            })
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let sftp = &link(&self.link, &self.server).await?.sftp;
        let key = prefix.map(Path::to_string).unwrap_or_default();
        let (objects, folders) = read_folder(sftp, &self.root, &key).await?;
        Ok(ListResult {
            common_prefixes: folders.iter().map(Path::parse).collect::<Result<_, _>>()?,
            objects,
            extensions: Default::default(),
        })
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(read_only("copy files"))
    }
}
