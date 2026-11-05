//! Files targets in the write-session contract: their sink writes the Parquet folders, so the writer moves no batches.

use arrow_array::RecordBatch;

use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::types::TableWriter;
use crate::adapters::files::models::ParquetFiles;
use crate::loader::errors::LoaderError;

impl TableWriter for ParquetFiles {
    fn writes_directly(&self) -> bool {
        true
    }

    async fn write_batch(
        &mut self,
        _target: &WriteTarget<'_>,
        _batch: RecordBatch,
    ) -> Result<(), LoaderError> {
        Err(LoaderError::Invalid(
            "a files target writes its batches through its folder".to_owned(),
        ))
    }

    fn staged_bytes(&self) -> Option<usize> {
        None
    }

    async fn finish_files(&mut self, _target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        Ok(())
    }

    async fn load_batch(&mut self, _target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        Ok(())
    }

    fn next_batch(&mut self) {}

    async fn settle_rows(&mut self) -> Result<(), LoaderError> {
        Ok(())
    }

    fn reopen_rows(&mut self) {}

    async fn rollback_batch(&mut self) {}

    async fn remove_batch_files(&mut self, _target: &WriteTarget<'_>) {}
}
