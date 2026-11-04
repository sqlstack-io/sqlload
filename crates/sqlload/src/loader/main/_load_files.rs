//! Load one table into a files target: repair unfinished loads, write new Parquet files, then delete the files they replace.

use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::files_destination::main::clear_pending::clear_pending;
use crate::files_destination::main::close_writer::close_writer;
use crate::files_destination::main::mark_pending::mark_pending;
use crate::files_destination::main::open_writer::open_writer;
use crate::files_destination::main::write_rows::write_rows;
use crate::files_destination::models::{FilesWriter, PendingPhase, PendingRun, WriterRequest};
use crate::loader::_helpers::run_log::{fold_maximum, pending_run};
use crate::loader::_helpers::sources::read_source;
use crate::loader::errors::LoaderError;
use crate::loader::main::_files_repair::files_to_replace;
use crate::loader::models::{FilesOutput, RunEntry, SourceReader, TablePlan};
use crate::masking::main::mask_batch::mask_batch;
use crate::run_log::models::RunOutcome;
use crate::store_paths::models::Location;
use crate::time_text::main::now_utc_text::now_utc_text;
use crate::write_sql::models::WriteMode;
use bytes::Bytes;

/// Repair unfinished loads, then write the table's rows as new files and delete the older files they replace; a pending marker records each phase, so an interrupted load is repaired by the next plan or load.
pub(in crate::loader) async fn load_files_table(
    reader: &SourceReader,
    table: &TablePlan,
    output: &FilesOutput,
    run: Option<&RunEntry>,
) -> Result<u64, LoaderError> {
    let spec = &table.spec;
    let folder = open_folder(&output.path.folder)?;
    let replaced = files_to_replace(&folder, table, output).await?;
    let load_id = uuid::Uuid::new_v4().to_string();
    let loaded_at = now_utc_text();
    let writer = open_writer(WriterRequest {
        folder: folder.clone(),
        path: &output.path,
        cursor_column: spec.cursor.as_ref().map(|cursor| cursor.column.as_str()),
        columns: &table.schema.write_columns,
        load_id: &load_id,
        loaded_at: &loaded_at,
    })
    .map_err(LoaderError::Invalid)?;
    let (writer, rows, loaded_maximum) = write_new_files(reader, table, writer, run).await?;
    let outcome = RunOutcome {
        run_id: load_id.clone(),
        recorded_at: now_utc_text(),
        rows,
        loaded_maximum,
        files: writer.written.clone(),
    };
    let pending_run = pending_run(run, outcome, &output.path.folder)?;
    let keys = replaced.keys(&writer.partitions);
    replace_files(&folder, &load_id, (&keys, pending_run.as_ref())).await?;
    Ok(rows)
}

/// Record the replaced files (and a logged table's run record) in the marker, which commits the load; write the run record, delete the replaced files, then the marker, so a stop in between is finished by the next plan or load.
async fn replace_files(
    folder: &OpenFolder,
    load_id: &str,
    (keys, run): (&[String], Option<&PendingRun>),
) -> Result<(), LoaderError> {
    mark_pending(folder, load_id, PendingPhase::Replacing, (keys, run)).await?;
    if let Some(run) = run {
        let records = OpenFolder {
            store: folder.store.clone(),
            location: Location {
                store: folder.location.store.clone(),
                prefix: run.prefix.clone(),
            },
        };
        write_file(&records, &run.name, Bytes::from(run.json.clone())).await?;
    }
    delete_files(folder, keys).await?;
    Ok(clear_pending(folder, load_id).await?)
}

/// Mark the load as writing, then write the rows as new files (folding each batch's cursor maximum for a logged table); after a failure, delete the files known to be written, best effort, and keep the `writing` marker, so the next plan or load deletes by load id any file a failed put still committed.
async fn write_new_files(
    reader: &SourceReader,
    table: &TablePlan,
    mut writer: FilesWriter,
    run: Option<&RunEntry>,
) -> Result<(FilesWriter, u64, Option<String>), LoaderError> {
    let folder = writer.folder.clone();
    mark_pending(&folder, &writer.load_id, PendingPhase::Writing, (&[], None)).await?;
    let keep_schema = matches!(table.mode, WriteMode::Replace | WriteMode::Recreate);
    let mut loaded_maximum: Option<String> = None;
    let written = async {
        let rows = read_source(reader, table, async |batch| {
            loaded_maximum = fold_maximum(run, &batch, loaded_maximum.take())?;
            let batch = mask_batch(batch, &table.spec.masked)?;
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
    match written {
        Ok(rows) => Ok((writer, rows, loaded_maximum)),
        Err(error) => {
            if let Err(cleanup) = delete_files(&folder, &writer.written).await {
                tracing::warn!(error = %cleanup, folder = %folder.location, "cannot delete the files of a failed load; the next plan or load deletes them");
            }
            Err(error)
        }
    }
}
