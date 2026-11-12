//! A warehouse source's unloaded files loaded by DuckDB reading them itself.

use crate::adapters::contract::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT};
use crate::adapters::contract::models::{DirectLoaded, UnloadedBatch};
use crate::adapters::duckdb::_helpers::pull_sql::{UnloadedColumns, unloaded_insert};
use crate::adapters::duckdb::main::_execute_statements::execute_statements;
use crate::adapters::duckdb::main::_pull_files::pull_files;
use crate::adapters::duckdb::models::DuckDbWriter;
use crate::loader::errors::LoaderError;

impl DuckDbWriter {
    /// Load `batch` by having DuckDB read the unloaded files itself, where Arrow batches would go: the load's staging table, the build table, or the table itself, inside a transaction that commits here when it goes straight into the table or a build table. DuckDB must load as many rows as the unload reports, or the batch fails uncommitted.
    pub(in crate::adapters) async fn pull_unloaded(
        &mut self,
        batch: &UnloadedBatch<'_>,
    ) -> Result<DirectLoaded, LoaderError> {
        let target = &batch.target;
        let loaded = |rows: u64| match target.direct {
            true => DirectLoaded::Committed(rows),
            false => DirectLoaded::Staged(rows),
        };
        let files = batch.files;
        if files.keys.is_empty() {
            return Ok(loaded(0));
        }
        let carried = match (target.staging, target.builds) {
            (Some(_), false) => target.columns,
            _ => batch.write_columns,
        };
        let urls: Vec<String> = files
            .keys
            .iter()
            .map(|key| format!("{}{key}", files.folder))
            .collect();
        let insert = unloaded_insert(
            target.staging.unwrap_or(target.table),
            UnloadedColumns {
                columns: carried,
                read: batch.read,
                constants: target.constants,
                naming: target.naming,
                batch: target.batch,
            },
            &urls,
            (target.load_id, batch.loaded_at),
        );
        let transaction = target.direct || target.builds;
        if transaction && !self.open_transaction {
            execute_statements(&self.destination, vec![BEGIN_STATEMENT.to_owned()]).await?;
            self.open_transaction = true;
        }
        let rows = pull_files(&self.destination, &files.folder.store, insert).await?;
        if rows != files.rows {
            return Err(LoaderError::Invalid(format!(
                "DuckDB loaded {rows} rows from the unloaded files in {}, but the unload reports {}; the batch is not committed",
                files.folder, files.rows
            )));
        }
        if transaction {
            execute_statements(&self.destination, vec![COMMIT_STATEMENT.to_owned()]).await?;
            self.open_transaction = false;
        }
        Ok(loaded(rows))
    }
}
