use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;

use crate::python_sources::models::{PythonSource, TableRead};

/// What a table function's read gives next.
#[derive(Debug)]
pub enum Streamed {
    Batch(RecordBatch),
    /// A logged table's `ctx.state` (as JSON) when the rows of the batches before it crossed.
    State(String),
}

/// A table function's data, one Arrow batch at a time; `next_batch` blocks.
pub trait BatchStream: Send {
    /// The next batch or state, an error, or `None` at the end.
    fn next_batch(&mut self) -> Option<Result<Streamed, String>>;
}

/// Runs Python sources: the `sqlload` Python package provides it; the standalone executable has none.
pub trait PythonHost: Send + Sync + std::fmt::Debug {
    /// Import every module under `python_paths` (relative to `root`) and return the sources declared there.
    fn discover(&self, root: &Path, python_paths: &[PathBuf]) -> Result<Vec<PythonSource>, String>;
    /// Call a table function and stream what it yields.
    fn read_table(&self, request: &TableRead) -> Result<Box<dyn BatchStream>, String>;
    /// The position the table function last started for `table` left in `ctx.state`, as JSON text; `None` when it set none.
    fn table_state(&self, table: &str) -> Result<Option<String>, String>;
    /// Close the clients the run built.
    fn finish(&self);
}
