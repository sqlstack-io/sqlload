use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};

use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::_helpers::parquet::object_reader::ObjectFileReader;
use crate::files_source::constants::SAMPLED_FOOTERS_KEPT;
use crate::files_source::errors::FilesError;

/// A reader for one listed Parquet file; its size is known, so no request is spent finding it.
pub(crate) fn object_reader(folder: &OpenFolder, file: &StoredFile) -> ObjectFileReader {
    ObjectFileReader {
        store: folder.store.clone(),
        path: file.path.clone(),
        size: file.size,
    }
}

/// Footers `plan` read for its schema sample, by folder, path and version, until a batch of the same load takes them; at most `SAMPLED_FOOTERS_KEPT`, so a long-lived process that plans without loading cannot grow it.
static SAMPLED: Mutex<Option<HashMap<String, ArrowReaderMetadata>>> = Mutex::new(None);

/// `file`'s footer, with its Arrow and Parquet schemas (a range read, not the whole file), kept for the batch that loads `file` so it is not read twice.
pub(crate) async fn parquet_footer(
    folder: &OpenFolder,
    file: &StoredFile,
) -> Result<ArrowReaderMetadata, FilesError> {
    let mut reader = object_reader(folder, file);
    let footer = ArrowReaderMetadata::load_async(&mut reader, ArrowReaderOptions::new())
        .await
        .map_err(|error| FilesError::File {
            key: file.relative_key.clone(),
            reason: format!("cannot read the Parquet footer: {error}"),
        })?;
    let mut sampled = SAMPLED.lock().unwrap_or_else(PoisonError::into_inner);
    let kept = sampled.get_or_insert_with(HashMap::new);
    if kept.len() >= SAMPLED_FOOTERS_KEPT {
        kept.clear();
    }
    kept.insert(sampled_key(folder, file), footer.clone());
    Ok(footer)
}

/// The footer `plan` read for `file` at this version, taken so it is used once; `None` when it was not sampled.
pub(crate) fn take_sampled_footer(
    folder: &OpenFolder,
    file: &StoredFile,
) -> Option<ArrowReaderMetadata> {
    let mut sampled = SAMPLED.lock().unwrap_or_else(PoisonError::into_inner);
    sampled.as_mut()?.remove(&sampled_key(folder, file))
}

fn sampled_key(folder: &OpenFolder, file: &StoredFile) -> String {
    format!("{}{}@{}", folder.location, file.path, file.version())
}
