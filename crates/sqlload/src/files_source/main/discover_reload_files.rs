//! List a files source's folder and the folder `after_load` moved its loaded files to, for a `--reload`.

use crate::file_store::models::OpenFolder;
use crate::files_source::_helpers::plan::folder::{listed_files, listed_table, unreadable};
use crate::files_source::errors::FilesError;
use crate::files_source::models::{FilesEncoding, FilesTable, PathValues};

/// The `encoding` files under `folder` and under `archive` (where `after_load` moved the files it loaded, keeping their keys), as [`discover_files`](crate::files_source::main::discover_files::discover_files) lists one folder: sorted by key, with the schema of the newest ones. An archive that does not exist yet, or holds no file, adds none; no file in either is `FilesError::NoFiles`.
pub async fn discover_reload_files(
    (folder, archive): (&OpenFolder, &OpenFolder),
    values: &PathValues,
    encoding: &FilesEncoding,
) -> Result<FilesTable, FilesError> {
    let archived = match unreadable(&archive.location) {
        Some(_) => Err(FilesError::NoFiles {
            key: archive.location.to_string(),
            reason: "the folder does not exist".to_owned(),
        }),
        None => listed_files(archive, values, encoding).await,
    };
    let mut listed = match (listed_files(folder, values, encoding).await, archived) {
        (Ok(mut listed), Ok(archived)) => {
            listed.files.extend(archived.files);
            listed.skipped += archived.skipped;
            listed.unmatched += archived.unmatched;
            listed.problems.extend(archived.problems);
            listed
        }
        (Ok(listed), Err(FilesError::NoFiles { .. }))
        | (Err(FilesError::NoFiles { .. }), Ok(listed)) => listed,
        (Err(error), _) | (_, Err(error)) => return Err(error),
    };
    listed
        .files
        .sort_by(|(left, _), (right, _)| left.file.relative_key.cmp(&right.file.relative_key));
    listed_table(folder, listed, values, encoding).await
}
