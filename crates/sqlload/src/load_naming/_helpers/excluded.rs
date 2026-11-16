//! Leaving out the fields of a batch that the table's `exclude_columns` matches.

use arrow_array::RecordBatch;
use globset::{Glob, GlobSetBuilder};

use crate::load_naming::models::NameRules;

/// `batch` without the fields `rules.excluded` matches and `rules.allowed` doesn't.
pub(crate) fn unexcluded(batch: RecordBatch, rules: &NameRules) -> Result<RecordBatch, String> {
    if rules.excluded.is_empty() {
        return Ok(batch);
    }
    let matcher = |patterns: &[String]| {
        let mut builder = GlobSetBuilder::new();
        for pattern in patterns {
            builder.add(
                Glob::new(pattern)
                    .map_err(|error| format!("invalid column pattern {pattern:?}: {error}"))?,
            );
        }
        builder.build().map_err(|error| error.to_string())
    };
    let (excluded, allowed) = (matcher(&rules.excluded)?, matcher(&rules.allowed)?);
    let kept: Vec<usize> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !excluded.is_match(field.name()) || allowed.is_match(field.name()))
        .map(|(index, _)| index)
        .collect();
    batch.project(&kept).map_err(|error| error.to_string())
}
