//! DuckDB's write session: Arrow appends, in one transaction per batch when the batch goes straight into the table or a build table, and checkpoints under the memory budget.

use arrow_array::RecordBatch;

use crate::adapters::contract::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT};
use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::types::TableWriter;
use crate::adapters::duckdb::constants::{
    CHECKPOINT_BUSY, CHECKPOINT_RETRY, CHECKPOINT_STATEMENT, CHECKPOINT_WAIT,
};
use crate::adapters::duckdb::main::_append_batch::append_batch;
use crate::adapters::duckdb::main::_execute_statements::execute_statements;
use crate::adapters::duckdb::models::DuckDbWriter;
use crate::column_types::main::is_variant::is_variant;
use crate::loader::errors::LoaderError;
use crate::memory_budget::main::large_values::large_values;
use crate::memory_budget::models::DuckDbShare;
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
        if self.open_transaction {
            execute_statements(&self.destination, vec![COMMIT_STATEMENT.to_owned()]).await?;
            self.open_transaction = false;
        }
        Ok(())
    }

    fn next_batch(&mut self) {}

    async fn settle_rows(&mut self) -> Result<(), LoaderError> {
        Ok(())
    }

    fn reopen_rows(&mut self) {}

    async fn rollback_batch(&mut self) {
        if std::mem::take(&mut self.open_transaction)
            && let Err(error) =
                execute_statements(&self.destination, vec!["ROLLBACK".to_owned()]).await
        {
            tracing::warn!(%error, "cannot roll back the open batch");
        }
    }

    async fn remove_batch_files(&mut self, _target: &WriteTarget<'_>) {}

    fn memory_share(&self) -> Option<DuckDbShare> {
        self.destination.memory
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
