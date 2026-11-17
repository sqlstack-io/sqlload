//! Record the source files a commit from a files source loaded.

use std::collections::{BTreeMap, BTreeSet};

use crate::adapters::files::_helpers::layout::loaded::{folder_of, put_record};
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::models::FilesWriter;
use crate::file_store::models::OpenFolder;
use crate::files_source::models::LoadedFile;

/// Put the record of the source files (`reading`) whose rows commit `writer`'s files hold, by the folder each file's rows went to; a group counts only while the table holds that commit's files in that folder, so a commit that never finished, or a folder whose files were replaced, claims nothing.
pub async fn write_loaded_files(
    folder: &OpenFolder,
    writer: &FilesWriter,
    reading: &[LoadedFile],
) -> Result<(), FilesDestinationError> {
    let commits = BTreeMap::from([(writer.file_id.clone(), commit_groups(writer, reading))]);
    put_record(folder, &writer.file_id, &commits).await
}

/// Each file of `reading` in every folder its rows went to, by the rows' source file columns; a file with no rows seen (empty, or the columns are not written) in every folder the commit wrote, so it counts while any of them is held.
fn commit_groups(
    writer: &FilesWriter,
    reading: &[LoadedFile],
) -> BTreeMap<String, Vec<(String, String)>> {
    let folders: BTreeSet<&str> = writer.written.iter().map(|key| folder_of(key)).collect();
    let mut groups: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    for file in reading {
        let source = (file.name.clone(), file.version.clone());
        let mut into: Vec<&str> = Vec::new();
        for (folder, sources) in &writer.sources {
            if folders.contains(folder.as_str()) && sources.contains(&source) {
                into.push(folder);
            }
        }
        if into.is_empty() {
            into.extend(folders.iter().copied());
        }
        for written in into {
            groups
                .entry(written.to_owned())
                .or_default()
                .push(source.clone());
        }
    }
    groups
}
