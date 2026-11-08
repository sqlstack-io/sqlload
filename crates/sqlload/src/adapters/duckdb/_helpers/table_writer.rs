//! DuckDB's write session: Arrow appends, in one transaction per batch when the batch goes straight into the table or a build table.

use arrow_array::RecordBatch;

use crate::adapters::contract::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT};
use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::types::TableWriter;
use crate::adapters::duckdb::main::_append_batch::append_batch;
use crate::adapters::duckdb::main::_execute_statements::execute_statements;
use crate::adapters::duckdb::models::DuckDbWriter;
use crate::loader::errors::LoaderError;

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
        if (target.direct || target.builds) && !self.open_transaction {
            execute_statements(&self.destination, vec![BEGIN_STATEMENT.to_owned()]).await?;
            self.open_transaction = true;
        }
        Ok(append_batch(&self.destination, into, batch).await?)
    }

    fn staged_bytes(&self) -> Option<usize> {
        None
    }

    async fn finish_files(&mut self, _target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        Ok(())
    }

    async fn load_batch(&mut self, _target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        if self.open_transaction {
            let t=std::time::Instant::now();
            execute_statements(&self.destination, vec![COMMIT_STATEMENT.to_owned()]).await?;
            eprintln!("PROF arrow_commit {:?}", t.elapsed());
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
        if self.open_transaction
            && let Err(error) =
                execute_statements(&self.destination, vec!["ROLLBACK".to_owned()]).await
        {
            tracing::warn!(%error, "cannot roll back the open batch");
        }
    }

    async fn remove_batch_files(&mut self, _target: &WriteTarget<'_>) {}
}
