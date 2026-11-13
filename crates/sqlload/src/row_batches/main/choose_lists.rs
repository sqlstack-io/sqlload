//! Which of a batch's columns are list fields under `unnest = "all"`.

use crate::load_naming::models::Naming;
use crate::row_batches::_helpers::unnest::Split;
use crate::row_batches::models::Unnest;
use crate::row_batches::types::ListChoices;
use arrow_array::RecordBatch;

/// Choose, from `batch`'s values, which of its columns not yet chosen are list fields under `unnest = "all"` (see [`ListChoices`]), adding them to `choices`; whether any was added. Fails, naming the column, when a column chosen as a list field holds a value that is not a list.
pub fn choose_lists(batch: &RecordBatch, choices: &mut ListChoices) -> Result<bool, String> {
    let known = choices.len();
    let mut split = Split {
        unnest: &Unnest::All,
        max_depth: 1,
        naming: Naming::Sqlload,
        choices,
        levels: Vec::new(),
    };
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        split.choose_column((field, column))?;
    }
    Ok(split.choices.len() > known)
}
