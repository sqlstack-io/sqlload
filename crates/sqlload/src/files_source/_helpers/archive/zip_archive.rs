//! Read the members of ZIP archives with range requests: the index at an archive's end, then one member, never the whole archive.

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::runtime::Handle;
use zip::ZipArchive;

use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::_helpers::plan::encoding::member_fits;
use crate::files_source::constants::{MAX_WHOLE_FILE_BYTES, ZIP_READ_BLOCK_BYTES};
use crate::files_source::models::FilesEncoding;

/// A stored file read as it is sought and read, a block per range request, from a blocking thread.
struct RangedFile {
    store: Arc<dyn ObjectStore>,
    path: Path,
    size: u64,
    position: u64,
    /// The last block read and where it starts.
    block: (u64, Bytes),
    handle: Handle,
}

impl RangedFile {
    fn new(folder: &OpenFolder, path: &Path, size: u64) -> Self {
        Self {
            store: folder.store.clone(),
            path: path.clone(),
            size,
            position: 0,
            block: (0, Bytes::new()),
            handle: Handle::current(),
        }
    }
}

impl Read for RangedFile {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let (start, block) = &self.block;
        let held = self.position >= *start && self.position < start + block.len() as u64;
        if !held && self.position < self.size {
            let end = self.size.min(self.position + ZIP_READ_BLOCK_BYTES);
            let bytes = self
                .handle
                .block_on(self.store.get_range(&self.path, self.position..end))
                .map_err(std::io::Error::other)?;
            self.block = (self.position, bytes);
        }
        let (start, block) = &self.block;
        let offset = usize::try_from(self.position.saturating_sub(*start))
            .unwrap_or(usize::MAX)
            .min(block.len());
        let count = out.len().min(block.len() - offset);
        out[..count].copy_from_slice(&block[offset..offset + count]);
        self.position += count as u64;
        Ok(count)
    }
}

impl Seek for RangedFile {
    fn seek(&mut self, to: SeekFrom) -> std::io::Result<u64> {
        let position = match to {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::End(offset) => self.size.checked_add_signed(offset),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
        };
        self.position = position.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek before the start of the file",
            )
        })?;
        Ok(self.position)
    }
}

/// The members of `archive`, a listed ZIP archive, that are `encoding`'s files, each a file of its own keyed by the archive's key, `/` and its name, in the archive's order; only the archive's index is read.
pub(crate) async fn archive_members(
    folder: &OpenFolder,
    archive: &StoredFile,
    encoding: &FilesEncoding,
) -> Result<Vec<StoredFile>, String> {
    let file = RangedFile::new(folder, &archive.path, archive.size);
    let entries = tokio::task::spawn_blocking(move || member_sizes(file))
        .await
        .map_err(|error| error.to_string())??;
    let mut members: Vec<StoredFile> = Vec::new();
    for (name, size) in entries {
        if member_fits(&name, encoding)? {
            members.push(StoredFile {
                path: archive.path.clone(),
                relative_key: format!("{}/{name}", archive.relative_key),
                size,
                etag: archive.etag.clone(),
                modified_ms: archive.modified_ms,
                member: Some(name),
            });
        }
    }
    Ok(members)
}

/// Member `name` of the ZIP archive `file` is in, decompressed; only the archive's index and that member are read.
pub(crate) async fn member_bytes(
    folder: &OpenFolder,
    file: &StoredFile,
    name: &str,
) -> Result<Vec<u8>, String> {
    let size = folder
        .store
        .head(&file.path)
        .await
        .map_err(|error| format!("cannot read the file: {error}"))?
        .size;
    let ranged = RangedFile::new(folder, &file.path, size);
    let name = name.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut archive = open_archive(ranged)?;
        let mut member = archive
            .by_name(&name)
            .map_err(|error| format!("member {name} of the ZIP archive: {error}"))?;
        if member.size() > MAX_WHOLE_FILE_BYTES {
            return Err(too_large(&name, member.size()));
        }
        let mut data: Vec<u8> = Vec::new();
        member
            .read_to_end(&mut data)
            .map_err(|error| format!("member {name} of the ZIP archive: {error}"))?;
        Ok(data)
    })
    .await
    .map_err(|error| error.to_string())?
}

/// Each file member's name and decompressed size, folders left out; an archive naming a member twice, or a member over the size SQLLoad reads, is an error.
fn member_sizes(file: RangedFile) -> Result<Vec<(String, u64)>, String> {
    let archive = open_archive(file)?;
    if archive.has_duplicate_names() {
        return Err("the ZIP archive holds two members with the same name".to_owned());
    }
    let mut entries: Vec<(String, u64)> = Vec::new();
    for index in 0..archive.len() {
        let member = archive
            .by_index_data(index)
            .map_err(|error| format!("cannot read the ZIP archive: {error}"))?;
        if member.is_file() {
            let name = member
                .name()
                .map_err(|error| format!("cannot read the ZIP archive: {error}"))?;
            if member.size() > MAX_WHOLE_FILE_BYTES {
                return Err(too_large(&name, member.size()));
            }
            entries.push((name.into_owned(), member.size()));
        }
    }
    Ok(entries)
}

fn too_large(name: &str, size: u64) -> String {
    format!(
        "member {name} of the ZIP archive is {size} bytes decompressed; SQLLoad reads members whole, up to {MAX_WHOLE_FILE_BYTES} bytes"
    )
}

fn open_archive(file: RangedFile) -> Result<ZipArchive<RangedFile>, String> {
    ZipArchive::new(file).map_err(|error| format!("cannot read the ZIP archive: {error}"))
}
