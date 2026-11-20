//! DuckDB's write session: Arrow appends (or a MotherDuck `external_stage`'s chunks), in one transaction per batch when the batch goes straight into the table or a build table, and checkpoints under the memory budget.

use arrow_array::RecordBatch;

use crate::adapters::contract::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT};
use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::contract::types::TableWriter;
use crate::adapters::duckdb::constants::{
    CHECKPOINT_BUSY, CHECKPOINT_RETRY, CHECKPOINT_STATEMENT, CHECKPOINT_WAIT,
};
use crate::adapters::duckdb::main::_append_batch::append_batch;
use crate::adapters::duckdb::main::_execute_statements::execute_statements;
use crate::adapters::duckdb::main::_open_bucket::open_bucket;
use crate::adapters::duckdb::main::_pull_files::pull_files;
use crate::adapters::duckdb::models::{DuckDbSql, DuckDbWriter, ExternalStage};
use crate::adapters::motherduck_bridge::main::_chunk_parquet::chunk_parquet;
use crate::column_types::main::is_variant::is_variant;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::write_file::write_file;
use crate::loader::errors::LoaderError;
use crate::memory_budget::main::large_values::large_values;
use crate::memory_budget::models::DuckDbShare;
use std::sync::Arc;
use std::sync::atomic::Ordering;

impl TableWriter for DuckDbWriter {
    fn writes_directly(&self) -> bool {
        true
    }

    async fn write_batch(
        &mut self,
        target: &WriteTarget<'_>,
        batch: RecordBatch,
    ) -> Result<(), LoaderError> {
        if let Some(stage) = self.destination.stage.clone() {
            return self.write_chunk(&stage, target, batch).await;
        }
        let into = target.staging.unwrap_or(target.table);
        if self.destination.memory.is_some()
            && let Some(rows) = large_values(&batch)
        {
            let copies = match (target.staging, target.builds) {
                (Some(_), false) => 2,
                _ => 1,
            };
            let large: u64 = rows.iter().sum();
            self.destination
                .unflushed
                .fetch_add(large * copies, Ordering::Relaxed);
        }
        if (target.direct || target.builds) && !self.open_transaction {
            execute_statements(&self.destination, vec![BEGIN_STATEMENT.to_owned()]).await?;
            self.open_transaction = true;
        }
        let variants: Vec<&str> = target
            .write_columns
            .iter()
            .filter(|column| is_variant(&column.column_type))
            .map(|column| column.name.as_str())
            .collect();
        Ok(append_batch(&self.destination, (into, &variants), batch).await?)
    }

    fn staged_bytes(&self) -> Option<usize> {
        None
    }

    async fn finish_files(&mut self, _target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        Ok(())
    }

    async fn load_batch(&mut self, _target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        self.load_chunks().await?;
        if self.open_transaction {
            execute_statements(&self.destination, vec![COMMIT_STATEMENT.to_owned()]).await?;
            self.open_transaction = false;
            self.remove_chunks().await;
        }
        Ok(())
    }

    fn next_batch(&mut self) {}

    async fn settle_rows(&mut self) -> Result<(), LoaderError> {
        self.load_chunks().await
    }

    fn reopen_rows(&mut self) {}

    async fn rollback_batch(&mut self) {
        if std::mem::take(&mut self.open_transaction)
            && let Err(error) =
                execute_statements(&self.destination, vec!["ROLLBACK".to_owned()]).await
        {
            tracing::warn!(%error, "cannot roll back the open batch");
        }
        self.remove_chunks().await;
    }

    async fn remove_batch_files(&mut self, _target: &WriteTarget<'_>) {}

    fn memory_share(&self) -> Option<DuckDbShare> {
        self.destination.memory
    }

    fn own_memory_limit(&self) -> Option<u64> {
        self.destination.own_limit
    }

    async fn checkpoint(&mut self, force: bool) -> Result<(), LoaderError> {
        let Some(share) = self.destination.memory else {
            return Ok(());
        };
        let unflushed = self.destination.unflushed.load(Ordering::Relaxed);
        if self.destination.motherduck || !(force || unflushed >= share.checkpoint_bytes) {
            return Ok(());
        }
        let started = std::time::Instant::now();
        while let Err(error) =
            execute_statements(&self.destination, vec![CHECKPOINT_STATEMENT.to_owned()]).await
        {
            if !error.to_string().contains(CHECKPOINT_BUSY) {
                return Err(error.into());
            }
            if started.elapsed() >= CHECKPOINT_WAIT {
                tracing::warn!(%error, "DuckDB's checkpoint waited too long for other transactions; it is left for the next one");
                return Ok(());
            }
            tokio::time::sleep(CHECKPOINT_RETRY).await;
        }
        let _ =
            self.destination
                .unflushed
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |now| {
                    Some(now.saturating_sub(unflushed))
                });
        Ok(())
    }
}

impl DuckDbWriter {
    /// Write `batch` to `stage` as one zstd Parquet chunk (`VARIANT` columns typed `JSON`), which waits there for [`Self::load_chunks`]; inside a transaction when the batch goes straight into the table or a build table, opened after the bucket's secret is set.
    pub(in crate::adapters) async fn write_chunk(
        &mut self,
        stage: &Arc<ExternalStage>,
        target: &WriteTarget<'_>,
        batch: RecordBatch,
    ) -> Result<(), LoaderError> {
        let into = target.staging.unwrap_or(target.table);
        let names: Vec<String> = (batch.schema().fields().iter())
            .map(|field| DuckDbSql.render_identifier(field.name()))
            .collect();
        let insert = format!(
            "INSERT INTO {} ({list}) SELECT {list} FROM read_parquet(",
            DuckDbSql.render_qualified_name(into),
            list = names.join(", ")
        );
        if insert != self.staged.insert {
            self.load_chunks().await?;
            self.staged.insert = insert;
        }
        if !self.open_transaction {
            open_bucket(&self.destination, &stage.folder.location.store).await?;
        }
        if (target.direct || target.builds) && !self.open_transaction {
            execute_statements(&self.destination, vec![BEGIN_STATEMENT.to_owned()]).await?;
            self.open_transaction = true;
        }
        let json: Vec<String> = (target.write_columns.iter())
            .filter(|column| is_variant(&column.column_type))
            .map(|column| column.name.clone())
            .collect();
        let rows = batch.num_rows() as u64;
        let body = tokio::task::spawn_blocking(move || {
            let mut body: Vec<u8> = Vec::new();
            chunk_parquet(&mut body, &batch, &json).map(|()| body)
        })
        .await
        .map_err(|error| LoaderError::Invalid(format!("the Parquet thread failed: {error}")))?
        .map_err(LoaderError::Invalid)?;
        self.staged.written += 1;
        let key = format!(
            "{}/{}.{}/{}-{}.parquet",
            target.load_id, into.schema, into.table, target.batch, self.staged.written
        );
        write_file(&stage.folder, &key, body.into())
            .await
            .map_err(|error| LoaderError::Invalid(error.to_string()))?;
        self.staged.waiting.push((key, rows));
        Ok(())
    }

    /// Have MotherDuck's servers read the waiting chunks into their table (`MD_RUN = REMOTE`), failing unless it reports as many rows as their footers hold. Outside a transaction the rows are in, and the files go now; inside one, once it commits ([`Self::remove_chunks`]).
    pub(in crate::adapters) async fn load_chunks(&mut self) -> Result<(), LoaderError> {
        let Some(stage) = self.destination.stage.clone() else {
            return Ok(());
        };
        if self.staged.waiting.is_empty() {
            return Ok(());
        }
        if !self.open_transaction {
            open_bucket(&self.destination, &stage.folder.location.store).await?;
        }
        let waiting = std::mem::take(&mut self.staged.waiting);
        let urls: Vec<String> = (waiting.iter())
            .map(|(key, _)| {
                DuckDbSql.render_loader_value_literal(&format!("{}{key}", stage.folder.location))
            })
            .collect();
        let insert = format!(
            "{}[{}], hive_partitioning = false, MD_RUN = REMOTE)",
            self.staged.insert,
            urls.join(", ")
        );
        let keys: Vec<String> = waiting.iter().map(|(key, _)| key.clone()).collect();
        self.staged.loaded.extend(keys);
        let rows = pull_files(&self.destination, insert).await?;
        let expected: u64 = waiting.iter().map(|(_, rows)| rows).sum();
        if rows != expected {
            return Err(LoaderError::Invalid(format!(
                "MotherDuck loaded {rows} rows from the chunks staged in {}, but their footers hold {expected}; the batch is not committed",
                stage.folder.location
            )));
        }
        if !self.open_transaction {
            self.remove_chunks().await;
        }
        Ok(())
    }

    /// Remove the chunks MotherDuck read, and those still waiting after a failure, unless the stage keeps its files; a file that cannot be removed is only logged.
    pub(in crate::adapters) async fn remove_chunks(&mut self) {
        let Some(stage) = self.destination.stage.clone() else {
            return;
        };
        let mut keys = std::mem::take(&mut self.staged.loaded);
        keys.extend(self.staged.waiting.drain(..).map(|(key, _)| key));
        if stage.keep || keys.is_empty() {
            return;
        }
        if let Err(error) = delete_files(&stage.folder, &keys).await {
            tracing::warn!(stage = %stage.folder.location, %error, "staged chunks were left in the external_stage");
        }
    }
}
