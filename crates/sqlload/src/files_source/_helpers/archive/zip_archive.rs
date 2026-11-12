//! Read the members of ZIP archives.

use std::io::{Cursor, Read};

use bytes::Bytes;
use object_store::ObjectStoreExt;
use zip::ZipArchive;

use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::_helpers::plan::encoding::member_fits;
use crate::files_source::constants::MAX_WHOLE_FILE_BYTES;
use crate::files_source::models::FilesEncoding;

/// The whole of `file`, which is read whole (a ZIP archive keeps its index at its end, a workbook is itself a ZIP archive); a file over the limit is an error.
pub(crate) async fn whole_file(folder: &OpenFolder, file: &StoredFile) -> Result<Bytes, String> {
    if file.size > MAX_WHOLE_FILE_BYTES {
        return Err(format!(
            "the file is {} bytes; SQLLoad reads ZIP archives, workbooks and XML documents whole, up to {MAX_WHOLE_FILE_BYTES} bytes",
            file.size
        ));
    }
    folder
        .store
        .get(&file.path)
        .await
        .map_err(|error| format!("cannot read the file: {error}"))?
        .bytes()
        .await
        .map_err(|error| format!("cannot read the file: {error}"))
}

/// The members of `archive`, a listed ZIP archive, that are `encoding`'s files, each a file of its own keyed by the archive's key, `/` and its name, in the archive's order.
pub(crate) async fn archive_members(
    folder: &OpenFolder,
    archive: &StoredFile,
    encoding: &FilesEncoding,
) -> Result<Vec<StoredFile>, String> {
    let bytes = whole_file(folder, archive).await?;
    let entries = tokio::task::spawn_blocking(move || member_sizes(bytes))
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

/// Member `file.member` of the ZIP archive `file` names, decompressed.
pub(crate) async fn member_bytes(
    folder: &OpenFolder,
    file: &StoredFile,
    name: &str,
) -> Result<Vec<u8>, String> {
    let bytes = whole_file(folder, file).await?;
    let name = name.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut archive = open_archive(bytes)?;
        let mut member = archive
            .by_name(&name)
            .map_err(|error| format!("member {name} of the ZIP archive: {error}"))?;
        if member.size() > MAX_WHOLE_FILE_BYTES {
            return Err(format!(
                "member {name} of the ZIP archive is {} bytes decompressed; SQLLoad reads members whole, up to {MAX_WHOLE_FILE_BYTES} bytes",
                member.size()
            ));
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

/// Each file member's name and decompressed size, folders left out.
fn member_sizes(bytes: Bytes) -> Result<Vec<(String, u64)>, String> {
    let mut archive = open_archive(bytes)?;
    let mut entries: Vec<(String, u64)> = Vec::new();
    for index in 0..archive.len() {
        let member = archive
            .by_index_raw(index)
            .map_err(|error| format!("cannot read the ZIP archive: {error}"))?;
        if member.is_file() {
            let name = member
                .name()
                .map_err(|error| format!("cannot read the ZIP archive: {error}"))?;
            entries.push((name.into_owned(), member.size()));
        }
    }
    Ok(entries)
}

fn open_archive(bytes: Bytes) -> Result<ZipArchive<Cursor<Bytes>>, String> {
    ZipArchive::new(Cursor::new(bytes))
        .map_err(|error| format!("cannot read the ZIP archive: {error}"))
}
