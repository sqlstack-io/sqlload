//! Add a batch of rows to a load's files.

use arrow_array::RecordBatch;

use crate::files_destination::_helpers::parquet_files::{
    new_file, next_key, open_bytes, store_file,
};
use crate::files_destination::_helpers::split::partition_rows;
use crate::files_destination::constants::{FILE_MAX_BYTES, FILE_MAX_ROWS, OPEN_FILES_MAX_BYTES};
use crate::files_destination::errors::FilesDestinationError;
use crate::files_destination::models::FilesWriter;

/// Write `batch` into its partitions' open files; full files are stored, and the largest while all open files hold too much.
pub async fn write_rows(
    writer: &mut FilesWriter,
    batch: RecordBatch,
) -> Result<&mut FilesWriter, FilesDestinationError> {
    let parts = partition_rows(writer, &batch).map_err(|reason| FilesDestinationError::Write {
        key: writer.folder.location.to_string(),
        reason,
    })?;
    for (value, part) in parts {
        add_part(writer, value, part).await?;
    }
    while writer.open.values().map(open_bytes).sum::<usize>() > OPEN_FILES_MAX_BYTES {
        let largest = writer
            .open
            .iter()
            .max_by_key(|(_, file)| open_bytes(file))
            .map(|(value, _)| value.clone())
            .unwrap_or_default();
        let Some(file) = writer.open.remove(&largest) else {
            break;
        };
        let key = next_key(writer, &largest);
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
    let mut file = match writer.open.remove(&value) {
        Some(file) => file,
        None => new_file(&writer.schema).map_err(failed)?,
    };
    file.writer
        .write(&part)
        .map_err(|error| failed(error.to_string()))?;
    file.rows += part.num_rows();
    writer.rows += part.num_rows() as u64;
    writer.partitions.insert(value.clone());
    match file.rows >= FILE_MAX_ROWS || open_bytes(&file) >= FILE_MAX_BYTES {
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
