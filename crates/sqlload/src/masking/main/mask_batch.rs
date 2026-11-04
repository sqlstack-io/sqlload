//! Mask columns of a batch.

use arrow_array::{ArrayRef, RecordBatch};

use crate::masking::_helpers::column::mask_column;
use crate::masking::models::MaskRules;

/// `batch` with each column named in `rules` replaced; masked columns must be text.
pub fn mask_batch(batch: RecordBatch, rules: &MaskRules) -> Result<RecordBatch, String> {
    if rules.columns.is_empty() {
        return Ok(batch);
    }
    let schema = batch.schema();
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| match rules.columns.get(field.name()) {
            Some(method) => mask_column(field, column, *method, rules.key.as_ref()),
            None => Ok(column.clone()),
        })
        .collect::<Result<_, String>>()?;
    RecordBatch::try_new(schema, columns).map_err(|error| error.to_string())
}
