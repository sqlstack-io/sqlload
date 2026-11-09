//! A files batch loaded by DuckDB reading the source's own Parquet files.

use crate::adapters::contract::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT};
use crate::adapters::contract::main::_batch_direct_problem::batch_direct_problem;
use crate::adapters::contract::main::_footer_rows::footer_rows;
use crate::adapters::contract::models::{DirectBatch, DirectLoaded};
use crate::adapters::duckdb::_helpers::pull_sql::{PulledFile, pull_insert};
use crate::adapters::duckdb::main::_direct_problem::direct_problem;
use crate::adapters::duckdb::main::_execute_statements::execute_statements;
use crate::adapters::duckdb::main::_pull_files::pull_files;
use crate::adapters::duckdb::models::DuckDbWriter;
use crate::files_source::constants::{SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN};
use crate::files_source::models::{BatchFooters, DataFile};
use crate::loader::errors::LoaderError;
use crate::loader::models::FilesSource;
use crate::store_paths::models::{Location, StoreKind};
use parquet::arrow::arrow_reader::ArrowReaderMetadata;

impl DuckDbWriter {
    /// Load `batch` by having DuckDB read its Parquet files itself, where Arrow batches would go: the load's staging table, the build table, or the table itself, inside a transaction that commits here when it goes straight into the table or a build table (the build stays invisible until the load swaps it in). A batch whose own footers hold a column a cast would not keep exactly fails; files never fall back to SQLLoad. `None` when the table is not a files table.
    pub(in crate::adapters) async fn copy_files(
        &mut self,
        batch: &DirectBatch<'_>,
    ) -> Result<Option<DirectLoaded>, LoaderError> {
        let Some(source) = batch.spec.files.as_ref() else {
            return Ok(None);
        };
        let problem = batch_direct_problem(batch, source, direct_problem(batch.spec, batch.read));
        if let Some(problem) = problem {
            return Err(LoaderError::Invalid(format!(
                "DuckDB cannot load this batch's files exactly by reading them itself, and they never go through SQLLoad instead: {problem}"
            )));
        }
        let files: Vec<PulledFile> = batch
            .files
            .iter()
            .map(|data| pulled_file(source, data))
            .collect();
        let in_files: Vec<String> = batch
            .read
            .columns
            .iter()
            .filter(|column| in_any_footer(batch.footers, &column.name))
            .map(|column| column.name.clone())
            .collect();
        let target = &batch.target;
        let carried = match (target.staging, target.builds) {
            (Some(_), false) => target.columns,
            _ => batch.write_columns,
        };
        let insert = pull_insert(
            target.staging.unwrap_or(target.table),
            (carried, &in_files),
            &files,
            (target.load_id, batch.loaded_at),
        );
        let transaction = target.direct || target.builds;
        if transaction && !self.open_transaction {
            execute_statements(&self.destination, vec![BEGIN_STATEMENT.to_owned()]).await?;
            self.open_transaction = true;
        }
        let rows = pull_files(&self.destination, &source.location.store, insert).await?;
        let expected = footer_rows(batch.footers);
        if rows != expected {
            return Err(LoaderError::Invalid(format!(
                "DuckDB loaded {rows} rows from this batch's files, but their footers hold {expected}; the batch is not committed"
            )));
        }
        if transaction {
            execute_statements(&self.destination, vec![COMMIT_STATEMENT.to_owned()]).await?;
            self.open_transaction = false;
        }
        Ok(Some(match target.direct {
            true => DirectLoaded::Committed(rows),
            false => DirectLoaded::Staged(rows),
        }))
    }
}

/// Where DuckDB reads `key` under `location`: its `s3://` URL, or its absolute path on this machine.
fn file_url(location: &Location, key: &str) -> String {
    let path = format!("{location}{key}");
    match location.store {
        StoreKind::S3 { .. } | StoreKind::Sftp { .. } => path,
        StoreKind::Filesystem { .. } => {
            std::path::absolute(&path).map_or(path, |absolute| absolute.display().to_string())
        }
    }
}

/// Where DuckDB reads `data`, and the values SQLLoad gives every row of it: its name and version as the files source records them, and its value of each declared partition column (`None` when its folders lack one).
fn pulled_file(source: &FilesSource, data: &DataFile) -> PulledFile {
    let partition = |name: &String| {
        let value = data
            .partitions
            .iter()
            .find(|(partition, _)| partition == name)
            .map(|(_, value)| value.clone());
        (name.clone(), value)
    };
    let mut constants = vec![
        (
            SOURCE_FILE_COLUMN.to_owned(),
            Some(format!("{}{}", source.location, data.file.relative_key)),
        ),
        (SOURCE_VERSION_COLUMN.to_owned(), Some(data.file.version())),
    ];
    for (name, _) in &source.partition_types {
        constants.push(partition(name));
    }
    PulledFile {
        url: file_url(&source.location, &data.file.relative_key),
        constants,
    }
}

/// Whether any of the batch's Parquet footers has a column named `name`.
fn in_any_footer(footers: &BatchFooters, name: &str) -> bool {
    let has = |footer: &ArrowReaderMetadata| {
        let fields = footer.schema().fields().clone();
        fields.iter().any(|field| field.name() == name)
    };
    footers.parquet.values().any(has)
}
