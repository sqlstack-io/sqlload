//! Mask columns of a batch.

use std::collections::BTreeMap;

use arrow_array::{ArrayRef, RecordBatch};

use crate::masking::_helpers::column::mask_column;
use crate::project::models::MaskMethod;

/// `batch` with each column named in `masks` replaced; masked columns must be text.
pub fn mask_batch(
    batch: RecordBatch,
    masks: &BTreeMap<String, MaskMethod>,
) -> Result<RecordBatch, String> {
    if masks.is_empty() {
        return Ok(batch);
    }
    let schema = batch.schema();
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| match masks.get(field.name()) {
            Some(method) => mask_column(field, column, *method),
            None => Ok(column.clone()),
        })
        .collect::<Result<_, String>>()?;
    RecordBatch::try_new(schema, columns).map_err(|error| error.to_string())
}
