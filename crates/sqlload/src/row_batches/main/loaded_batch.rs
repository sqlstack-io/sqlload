//! A read batch under the names its table loads as.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{FieldRef, Schema};

use crate::postgres_source::models::SourceTable;

/// `batch`, read under the source's spellings, with each field `table` renames (under `naming = "dlt"`) under the name it loads as; every other field as it is.
pub fn loaded_batch(batch: &RecordBatch, table: &SourceTable) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .map(|field| match table.loaded_as(field.name()) {
            loaded if loaded == field.name() => field.clone(),
            loaded => Arc::new(field.as_ref().clone().with_name(loaded)),
        })
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        batch.columns().to_vec(),
    )
    .map_err(|error| error.to_string())
}
