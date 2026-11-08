//! Add a batch of rows to a load's files.

use arrow_array::RecordBatch;

use crate::adapters::files::_helpers::write::data_files::{
    add_rows, new_file, next_key, open_bytes, store_file,
};
use crate::adapters::files::_helpers::write::spill::spill_order;
use crate::adapters::files::_helpers::write::split::partition_rows;
use crate::adapters::files::constants::{
    FILE_MAX_BYTES, FILE_MAX_ROWS, OPEN_FILES_MAX, OPEN_FILES_MAX_BYTES,
};
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::models::FilesWriter;

/// Write `batch` into its partitions' open files; full files are stored, and the largest while too many files are open or they hold too much; with `single_file` each partition keeps its one file open until the commit.
pub async fn write_rows(
    writer: &mut FilesWriter,
    batch: RecordBatch,
) -> Result<&mut FilesWriter, FilesDestinationError> {
    let parts = partition_rows(writer, &batch).map_err(|reason| FilesDestinationError::Write {
        key: writer.folder.location.to_string(),
        reason,
    })?;
    for (value, part) in parts {
        let opens = !writer.open.contains_key(&value);
        let max_files = match opens {
            true => OPEN_FILES_MAX - 1,
            false => OPEN_FILES_MAX,
        };
        spill(writer, max_files).await?;
        add_part(writer, value, part).await?;
    }
    spill(writer, OPEN_FILES_MAX).await
}

/// Store the largest open files until at most `max_files` stay open, holding at most the memory cap; with `single_file` a file is never stored early, so more open files than the cap fail instead.
async fn spill(
    writer: &mut FilesWriter,
    max_files: usize,
) -> Result<&mut FilesWriter, FilesDestinationError> {
    if writer.encoding.single_file {
        return match writer.open.len() > max_files {
            true => Err(FilesDestinationError::Write {
                key: writer.folder.location.to_string(),
                reason: format!(
                    "single_file keeps one file open per partition, and this commit writes more than {OPEN_FILES_MAX} partitions; load fewer partitions at once (--start-cursor/--end-cursor) or drop single_file"
                ),
            }),
            false => Ok(writer),
        };
    }
    let sizes: Vec<(String, usize)> = writer
        .open
        .iter()
        .map(|(value, file)| (value.clone(), open_bytes(file)))
        .collect();
    for value in spill_order(&sizes, max_files, OPEN_FILES_MAX_BYTES) {
        let Some(file) = writer.open.remove(&value) else {
            continue;
        };
        let key = next_key(writer, &value);
        store_file(&writer.folder, &key, file).await?;
        writer.written.push(key);
    }
    Ok(writer)
}

/// Add one partition's rows to its open file, storing the file once it is full.
async fn add_part(
    writer: &mut FilesWriter,
    value: String,
    part: RecordBatch,
) -> Result<&mut FilesWriter, FilesDestinationError> {
    let failed = |reason: String| FilesDestinationError::Write {
        key: writer.folder.location.to_string(),
        reason,
    };
    let file = match writer.open.remove(&value) {
        Some(file) => file,
        None => new_file(&writer.schema, &writer.encoding).map_err(failed)?,
    };
    let file = add_rows(file, &part, &writer.encoding).map_err(failed)?;
    writer.rows += part.num_rows() as u64;
    writer.partitions.insert(value.clone());
    let full = file.rows >= FILE_MAX_ROWS || open_bytes(&file) >= FILE_MAX_BYTES;
    match full && !writer.encoding.single_file {
        true => {
            let key = next_key(writer, &value);
            store_file(&writer.folder, &key, file).await?;
            writer.written.push(key);
        }
        false => {
            writer.open.insert(value, file);
        }
    }
    Ok(writer)
}
