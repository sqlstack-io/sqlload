//! Load one table into a files target: repair unfinished loads, write new Parquet files, then delete the files they replace.

use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::models::OpenFolder;
use crate::files_destination::main::clear_pending::clear_pending;
use crate::files_destination::main::close_writer::close_writer;
use crate::files_destination::main::mark_pending::mark_pending;
use crate::files_destination::main::open_writer::open_writer;
use crate::files_destination::main::write_rows::write_rows;
use crate::files_destination::models::{FilesWriter, PendingPhase, WriterRequest};
use crate::loader::_helpers::clock::now_utc_text;
use crate::loader::_helpers::sources::read_source;
use crate::loader::errors::LoaderError;
use crate::loader::main::_files_repair::files_to_replace;
use crate::loader::models::{FilesOutput, SourceReader, TablePlan};
use crate::masking::main::mask_batch::mask_batch;
use crate::write_sql::models::WriteMode;

/// Repair unfinished loads, then write the table's rows as new files and delete the older files they replace; a pending marker records each phase, so an interrupted load is repaired by the next plan or load.
pub(in crate::loader) async fn load_files_table(
    reader: &SourceReader,
    table: &TablePlan,
    output: &FilesOutput,
) -> Result<u64, LoaderError> {
    let spec = &table.spec;
    let folder = open_folder(&output.path.folder)?;
    let replaced = files_to_replace(&folder, table, output).await?;
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
    mark_pending(&folder, &load_id, PendingPhase::Writing, &[]).await?;
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
            abandon(&folder, &writer).await;
            return Err(error);
        }
    };
    replace_files(&folder, &load_id, &replaced.keys(&writer.partitions)).await?;
    Ok(rows)
}

/// Record the replaced files in the marker, delete them, then the marker, so a stop in between is finished by the next plan or load.
async fn replace_files(
    folder: &OpenFolder,
    load_id: &str,
    keys: &[String],
) -> Result<(), LoaderError> {
    mark_pending(folder, load_id, PendingPhase::Replacing, keys).await?;
    delete_files(folder, keys).await?;
    Ok(clear_pending(folder, load_id).await?)
}

/// After a failed write: delete the files written so far, then the marker; if they cannot all be deleted the marker stays, so the next plan or load deletes them.
async fn abandon(folder: &OpenFolder, writer: &FilesWriter) {
    let cleaned = match delete_files(folder, &writer.written).await {
        Ok(()) => clear_pending(folder, &writer.load_id).await,
        Err(error) => Err(error.into()),
    };
    if let Err(error) = cleaned {
        tracing::warn!(%error, folder = %folder.location, "cannot delete the files of a failed load; the next plan or load deletes them");
    }
}
