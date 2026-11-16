use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::python_sources::errors::ReadError;
use crate::python_sources::models::{PythonSource, TableRead, TransformCall};

/// What a table function's read gives next.
#[derive(Debug)]
pub enum Streamed {
    Batch(RecordBatch),
    /// A logged table's `ctx.state` (as JSON) when the rows of the batches before it crossed.
    State(String),
}

/// A table function's data, one Arrow batch at a time; `next_batch` blocks.
pub trait BatchStream: Send + std::fmt::Debug {
    /// The next batch or state, an error, or `None` at the end.
    fn next_batch(&mut self) -> Option<Result<Streamed, ReadError>>;
    /// Whether a `transform` returned or yielded dicts, which cross slower than Arrow or a DataFrame; known once the stream has ended. A table function's read says `false`.
    fn crossed_dicts(&self) -> bool {
        false
    }
}

/// Runs Python sources: the `sqlload` Python package provides it; the standalone executable has none.
pub trait PythonHost: Send + Sync + std::fmt::Debug {
    /// Import every module under `python_paths` (relative to `root`) and return the sources declared there.
    fn discover(&self, root: &Path, python_paths: &[PathBuf]) -> Result<Vec<PythonSource>, String>;
    /// Call a table function and stream what it yields.
    fn read_table(&self, request: &TableRead) -> Result<Box<dyn BatchStream>, String>;
    /// The position the table function last started for `table` left in `ctx.state`, as JSON text; `None` when it set none.
    fn table_state(&self, table: &str) -> Result<Option<String>, String>;
    /// Look up the `transform` that `@sl.transform` registered as `name` and check it can run: its version, `name@` and a hash of the file it is defined in, or why it cannot (no function, or two, registered as `name`).
    fn transform_version(&self, name: &str) -> Result<String, String>;
    /// Call a `transform` on `rows` (in `schema`) and stream what it returned or yielded, crossed as a table function's yields are, as it yields it.
    fn transform(
        &self,
        call: &TransformCall,
        rows: Vec<RecordBatch>,
        schema: SchemaRef,
    ) -> Result<Box<dyn BatchStream>, String>;
    /// Call the `rename` function `@sl.rename` registered as `name` on `columns` (JSON: each column's `position`, `source` and `default` name) with its `ctx` (JSON: `source`, `table`): the names it gives, by position, as a JSON object; with no `columns`, only look the function up. Fails when no function, or two, are registered as `name`, or it fails or returns something else.
    fn rename(&self, name: &str, columns: Option<&str>, context: &str) -> Result<String, String>;
    /// Close the clients the run built.
    fn finish(&self);
}
