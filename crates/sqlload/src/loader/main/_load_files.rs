//! Load one table into a files target: write new Parquet files, then delete the files they replace.

use crate::column_types::models::ColumnType;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::open_folder::open_folder;
use crate::files_destination::main::close_writer::close_writer;
use crate::files_destination::main::list_table_files::list_table_files;
use crate::files_destination::main::open_writer::open_writer;
use crate::files_destination::main::partition_in_window::partition_in_window;
use crate::files_destination::main::write_rows::write_rows;
use crate::files_destination::models::{FilesWriter, TableFiles, WriterRequest};
use crate::loader::_helpers::clock::now_utc_text;
use crate::loader::_helpers::sources::read_source;
use crate::loader::errors::LoaderError;
use crate::loader::models::{FilesOutput, SourceReader, TablePlan};
use crate::masking::main::mask_batch::mask_batch;
use crate::write_sql::models::WriteMode;

/// Write the table's rows as new files, then delete the older files the strategy replaces; a failed write deletes its new files.
pub(in crate::loader) async fn load_files_table(
    reader: &SourceReader,
    table: &TablePlan,
    output: &FilesOutput,
) -> Result<u64, LoaderError> {
    let spec = &table.spec;
    let folder = open_folder(&output.path.folder)?;
    let cursor_type = spec.cursor.as_ref().and_then(|cursor| {
        table
            .schema
            .write_columns
            .iter()
            .find(|column| column.name == cursor.column)
            .map(|column| column.column_type.clone())
    });
    let before = list_table_files(&folder, &output.path, cursor_type.as_ref()).await?;
    if let Some(problem) = before.problems.first() {
        return Err(LoaderError::Invalid(problem.clone()));
    }
    let load_id = uuid::Uuid::new_v4().to_string();
    let loaded_at = now_utc_text();
    let mut writer = open_writer(WriterRequest {
        folder: folder.clone(),
        path: &output.path,
        cursor_column: spec.cursor.as_ref().map(|cursor| cursor.column.as_str()),
        columns: &table.schema.write_columns,
        load_id: &load_id,
        loaded_at: &loaded_at,
    })
    .map_err(LoaderError::Invalid)?;
    let keep_schema = matches!(table.mode, WriteMode::Replace | WriteMode::Recreate);
    let written = async {
        let rows = read_source(reader, table, async |batch| {
            let batch = mask_batch(batch, &spec.masked)?;
            write_rows(&mut writer, batch)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
        .await?;
        close_writer(&mut writer, keep_schema).await?;
        Ok::<u64, LoaderError>(rows)
    }
    .await;
    let rows = match written {
        Ok(rows) => rows,
        Err(error) => {
            if let Err(cleanup) = delete_files(&folder, &writer.written).await {
                tracing::warn!(%cleanup, folder = %output.path.folder, "cannot delete the files of a failed load");
            }
            return Err(error);
        }
    };
    let replaced = replaced_files(&table.mode, &before, &writer, cursor_type.as_ref())
        .map_err(LoaderError::Invalid)?;
    delete_files(&folder, &replaced).await?;
    Ok(rows)
}

/// The existing files this load replaces: none for append, all for replace, every file of a partition written or in the window for delete_insert.
fn replaced_files(
    mode: &WriteMode,
    before: &TableFiles,
    writer: &FilesWriter,
    cursor_type: Option<&ColumnType>,
) -> Result<Vec<String>, String> {
    let (from, to) = match mode {
        WriteMode::Append => return Ok(Vec::new()),
        WriteMode::Replace | WriteMode::Recreate => return Ok(before.keys()),
        WriteMode::Merge { .. } => {
            return Err("merge is not supported for files targets".to_owned());
        }
        WriteMode::DeleteInsert { from, to, .. } => (from.as_deref(), to.as_deref()),
    };
    let (Some(partition), Some(cursor_type)) = (&writer.partition, cursor_type) else {
        return Err("delete_insert into files needs a partition folder by the cursor".to_owned());
    };
    let mut replaced: Vec<String> = Vec::new();
    for existing in &before.partitions {
        let value = existing.value.as_deref().unwrap_or_default();
        let touched = writer.partitions.contains(value)
            || partition_in_window(value, partition.by, cursor_type, (from, to))?;
        if touched {
            replaced.extend(existing.files.iter().map(|file| file.relative_key.clone()));
        }
    }
    Ok(replaced)
}
