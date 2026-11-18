//! Whether a destination loads a warehouse source's unload straight into the table.

use crate::adapters::dispatch::models::Writer;

/// Whether `writer` may load a warehouse source's unloaded files straight into the table, where it writes directly at all: not Databricks, whose `COPY`s commit one by one before their rows are checked against the unload's.
pub fn unloads_directly(writer: &Writer) -> bool {
    !matches!(writer, Writer::Databricks(_))
}
