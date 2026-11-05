use std::collections::BTreeSet;

use arrow_array::RecordBatch;
use bytes::Bytes;

use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::files_destination::main::clear_pending::clear_pending;
use crate::files_destination::main::close_writer::close_writer;
use crate::files_destination::main::mark_pending::mark_pending;
use crate::files_destination::main::open_writer::open_writer;
use crate::files_destination::main::write_rows::write_rows;
use crate::files_destination::models::{
    FilesWriter, PartitionFolder, PendingPhase, PendingRun, ReplacedFiles, WriterRequest,
};
use crate::loader::errors::LoaderError;
use crate::loader::models::{FilesSink, Publisher};
use crate::run_log::main::record_object_name::record_object_name;
use crate::run_log::main::runs_location::runs_location;
use crate::store_paths::models::Location;

/// The replaced files a commit deletes: those of the partitions it wrote, and, at the last commit, every other file the load replaces (everything for `replace`, the window's partitions for `delete_insert`); never one already deleted.
fn commit_deletes(
    replaced: &ReplacedFiles,
    partition: Option<&PartitionFolder>,
    (written, last): (&BTreeSet<String>, bool),
    deleted: &BTreeSet<String>,
) -> Vec<String> {
    let folders: Vec<String> = match partition {
        Some(partition) => written
            .iter()
            .map(|value| format!("{}/", partition.name(value)))
            .collect(),
        None => Vec::new(),
    };
    let mut keys: Vec<String> = written
        .iter()
        .flat_map(|value| replaced.by_partition.get(value).into_iter().flatten())
        .cloned()
        .collect();
    keys.extend(
        replaced
            .always
            .iter()
            .filter(|key| last || folders.iter().any(|folder| key.starts_with(folder)))
            .cloned(),
    );
    keys.retain(|key| !deleted.contains(key));
    keys.sort();
    keys.dedup();
    keys
}

impl Publisher {
    /// Add a batch to the commit's files, starting them (and marking the commit as writing) on its first batch.
    pub(crate) async fn stage_files(&mut self, batch: RecordBatch) -> Result<(), LoaderError> {
        let writer = match self.files.as_mut().and_then(|sink| sink.writer.take()) {
            Some(writer) => writer,
            None => self.open_files().await?,
        };
        let mut writer = writer;
        let written = write_rows(&mut writer, batch).await.map(|_| ());
        if let Some(sink) = self.files.as_mut() {
            sink.writer = Some(writer);
        }
        Ok(written?)
    }

    /// A writer for the next commit's files, whose pending marker says they are being written.
    async fn open_files(&mut self) -> Result<FilesWriter, LoaderError> {
        let sink = self.sink()?;
        let file_id = format!("{}.{:05}", self.load_id, self.commits + 1);
        let spec = &self.table.spec;
        let writer = open_writer(WriterRequest {
            folder: sink.folder.clone(),
            path: &sink.output.path,
            cursor_column: spec.cursor.as_ref().map(|cursor| cursor.column.as_str()),
            columns: &self.table.schema.write_columns,
            load_id: &self.load_id,
            file_id: &file_id,
            loaded_at: &self.loaded_at,
        })
        .map_err(LoaderError::Invalid)?;
        mark_pending(&sink.folder, &file_id, PendingPhase::Writing, (&[], None)).await?;
        Ok(writer)
    }

    /// Commit the files written since the last commit: store the open ones, record the files they replace (and a logged table's record) in the marker, which commits them; then write the record, delete the replaced files and the marker. The last commit of a `replace` with no rows still stores one empty file, so the table keeps its columns.
    pub(crate) async fn commit_files(&mut self, last: bool) -> Result<(), LoaderError> {
        let writer = match (
            self.files.as_mut().and_then(|sink| sink.writer.take()),
            last && self.at_end,
        ) {
            (Some(writer), _) => Some(writer),
            (None, true) if self.committed_rows == 0 => Some(self.open_files().await?),
            (None, _) => None,
        };
        let Some(mut writer) = writer else {
            return self.delete_leftovers(last).await;
        };
        let keep_schema = self.at_end && self.committed_rows == 0;
        close_writer(&mut writer, keep_schema).await?;
        let sink = self.sink()?;
        let keys = commit_deletes(
            &sink.replaced,
            sink.output.path.partition.as_ref(),
            (&writer.partitions, last),
            &sink.deleted,
        );
        let run = match &self.run {
            Some(_) => {
                let mut record = self.record(self.waiting_rows)?;
                record.files = writer.written.clone();
                Some(PendingRun {
                    prefix: runs_location(&self.sink()?.output.path.folder).prefix,
                    name: record_object_name(&record),
                    json: serde_json::to_string(&record)
                        .map_err(|error| LoaderError::Invalid(error.to_string()))?,
                })
            }
            None => None,
        };
        let sink = self.sink()?;
        let folder = sink.folder.clone();
        replace_files(&folder, &writer.file_id, (&keys, run.as_ref())).await?;
        if let Some(sink) = self.files.as_mut() {
            sink.deleted.extend(keys);
        }
        self.committed_rows += self.waiting_rows;
        self.waiting_rows = 0;
        self.commits += 1;
        self.committed_state = self.state.clone();
        Ok(())
    }

    /// Whether the last commit still has replaced files to delete that no partition written took with it.
    pub(crate) fn leftover_deletes(&self) -> bool {
        self.files.as_ref().is_some_and(|sink| {
            !commit_deletes(
                &sink.replaced,
                None,
                (&BTreeSet::new(), true),
                &sink.deleted,
            )
            .is_empty()
        })
    }

    /// At the last commit, delete the replaced files no commit deleted, under a marker of their own.
    async fn delete_leftovers(&mut self, last: bool) -> Result<(), LoaderError> {
        let sink = self.sink()?;
        let keys = commit_deletes(
            &sink.replaced,
            None,
            (&BTreeSet::new(), last),
            &sink.deleted,
        );
        if keys.is_empty() {
            return Ok(());
        }
        let file_id = format!("{}.{:05}", self.load_id, self.commits + 1);
        replace_files(&sink.folder.clone(), &file_id, (&keys, None)).await?;
        if let Some(sink) = self.files.as_mut() {
            sink.deleted.extend(keys);
        }
        Ok(())
    }

    /// After a failure: delete the files of the commit being written, best effort, keeping its `writing` marker so the next plan or load deletes any file a failed put still stored.
    pub(crate) async fn abandon_files(&mut self) {
        let Some(sink) = self.files.as_mut() else {
            return;
        };
        let Some(writer) = sink.writer.take() else {
            return;
        };
        if let Err(error) = delete_files(&sink.folder, &writer.written).await {
            tracing::warn!(error = %error, folder = %sink.folder.location, "cannot delete the files of a failed load; the next plan or load deletes them");
        }
    }

    fn sink(&self) -> Result<&FilesSink, LoaderError> {
        self.files.as_ref().ok_or_else(|| {
            LoaderError::Invalid(format!(
                "{}: a files target table has no files planned",
                self.table.spec.destination.table
            ))
        })
    }
}

/// Record the replaced files (and a logged table's run record) in the marker, which commits the files; write the run record, delete the replaced files, then the marker, so a stop in between is finished by the next plan or load.
async fn replace_files(
    folder: &OpenFolder,
    file_id: &str,
    (keys, run): (&[String], Option<&PendingRun>),
) -> Result<(), LoaderError> {
    mark_pending(folder, file_id, PendingPhase::Replacing, (keys, run)).await?;
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
    Ok(clear_pending(folder, file_id).await?)
}
