//! A batch's list fields as child tables' rows.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::Field;

use crate::row_batches::_helpers::unnest::{Split, child_batches, parent_batch};
use crate::row_batches::models::{UnnestRequest, Unnested};
use crate::row_batches::types::ListChoices;

/// `batch` split by `request.unnest`: each list field it names (under `"all"`, each field `choices` takes, choosing on as values decide; see [`ListChoices`]) leaves the parent's rows, and its items become rows of the child table `<table>__<field>`, a level down for each list inside them, to `max_depth`. A list of scalars gives a `value` column; a list inside a list gives the item a `list` field; a NULL or missing list gives no rows, and a value that is not a list is a list of itself (under `"all"`, an error naming the field once it is taken as a list field). Each child row names its parent's id and its position from 0, gets the id dlt gives it from both, and carries its root's id when `request.roots` gives them.
pub fn unnest_batch(
    batch: &RecordBatch,
    request: &UnnestRequest<'_>,
    choices: &mut ListChoices,
) -> Result<Unnested, String> {
    let mut split = Split {
        unnest: request.unnest,
        max_depth: request.max_depth,
        naming: request.naming,
        choices,
        levels: Vec::new(),
    };
    let schema = batch.schema();
    let mut kept: Vec<(Arc<Field>, ArrayRef)> = Vec::with_capacity(batch.num_columns());
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        if !split.take_lists((field, column), request)? {
            kept.push((field.clone(), column.clone()));
        }
    }
    Ok(Unnested {
        parent: parent_batch(&schema, kept)?,
        children: child_batches(split)?,
    })
}
