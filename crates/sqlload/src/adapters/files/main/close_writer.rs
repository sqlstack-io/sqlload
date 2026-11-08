//! Store a load's last open files.

use crate::adapters::files::_helpers::write::data_files::{new_file, next_key, store_file};
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::models::FilesWriter;

/// Store every open file; with `keep_schema`, a load of no rows still stores one empty file so the table keeps its columns.
pub async fn close_writer(
    writer: &mut FilesWriter,
    keep_schema: bool,
) -> Result<&mut FilesWriter, FilesDestinationError> {
    let open = std::mem::take(&mut writer.open);
    for (value, file) in open {
        let key = next_key(writer, &value);
        store_file(&writer.folder, &key, file).await?;
        writer.written.push(key);
    }
    if keep_schema && writer.written.is_empty() && writer.partition.is_none() {
        let file = new_file(&writer.schema, &writer.encoding).map_err(|reason| {
            FilesDestinationError::Write {
                key: writer.folder.location.to_string(),
                reason,
            }
        })?;
        let key = next_key(writer, "");
        store_file(&writer.folder, &key, file).await?;
        writer.written.push(key);
    }
    Ok(writer)
}
