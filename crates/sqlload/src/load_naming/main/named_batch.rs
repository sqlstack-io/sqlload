//! A batch whose columns arrive as it loads (a Python table's), named as they load.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{FieldRef, Schema};

use crate::load_naming::_helpers::excluded::unexcluded;
use crate::load_naming::_helpers::numbering::clashes;
use crate::load_naming::main::loaded_columns::loaded_columns;
use crate::load_naming::models::{ColumnName, NameMemo, NameRules, NumberedName};
use crate::load_naming::types::RenameFunction;

/// `batch` without the fields `rules.excluded` matches by the names they arrive under (unless `rules.allowed` does too), and with each other field under the name it loads as, through the name pipeline under `rules` over every field `memo` has seen so far in this load, in the order they first came; and `memo` with the names given, which later batches keep: a field that would now load under another name (a rename function answering differently) is an error, and numbering keeps the numbers it gave.
pub fn named_batch(
    batch: RecordBatch,
    memo: NameMemo,
    rules: &NameRules,
    rename: RenameFunction<'_>,
) -> (Result<RecordBatch, String>, NameMemo) {
    let batch = match unexcluded(batch, rules) {
        Ok(batch) => batch,
        Err(problem) => return (Err(problem), memo),
    };
    let new: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .filter(|name| !memo.loaded.contains_key(name))
        .collect();
    if new.is_empty() {
        return (renamed_batch(&batch, &memo), memo);
    }
    match remembered(&memo, new, rules, rename) {
        Ok(memo) => (renamed_batch(&batch, &memo), memo),
        Err(problem) => (Err(problem), memo),
    }
}

/// `memo` with the fields `new` added and every field seen named again through the pipeline, each field named before keeping its name as numbering keeps an earlier load's (so a new field meeting it takes the next number); an error when a field already named would now load under another name.
fn remembered(
    memo: &NameMemo,
    new: Vec<String>,
    rules: &NameRules,
    rename: RenameFunction<'_>,
) -> Result<NameMemo, String> {
    let mut memo = memo.clone();
    memo.seen.extend(new);
    let columns: Vec<ColumnName> = memo
        .seen
        .iter()
        .enumerate()
        .map(|(index, name)| ColumnName {
            position: index + 1,
            source: name.clone(),
            read_as: name.clone(),
        })
        .collect();
    let mut earlier = rules.kept.clone();
    earlier.extend(memo.numbered.iter().cloned());
    let given = memo.loaded.iter().map(|(source, loaded)| NumberedName {
        column: loaded.clone(),
        source: source.clone(),
        repeat: 0,
    });
    let rules = NameRules {
        kept: earlier.iter().cloned().chain(given).collect(),
        ..rules.clone()
    };
    let names = loaded_columns(&columns, &rules, rename).map_err(|problems| problems.join("; "))?;
    for column in &names.columns {
        if let Some(earlier) = memo.loaded.get(&column.source)
            && *earlier != column.loaded
        {
            return Err(format!(
                "field {:?} loaded as {earlier} earlier in this load, and the rename now gives it {}; a rename function must give a field the same name every time",
                column.source, column.loaded
            ));
        }
        memo.loaded
            .insert(column.source.clone(), column.loaded.clone());
    }
    memo.numbered = names
        .numbered
        .into_iter()
        .filter(|name| earlier.contains(name) || clashes(name, &names.columns))
        .collect();
    memo.columns = names.columns;
    Ok(memo)
}

/// `batch` with each field under the name `memo` gives it.
fn renamed_batch(batch: &RecordBatch, memo: &NameMemo) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .map(|field| match memo.loaded.get(field.name()) {
            Some(loaded) if loaded != field.name() => {
                Arc::new(field.as_ref().clone().with_name(loaded))
            }
            _ => field.clone(),
        })
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        batch.columns().to_vec(),
    )
    .map_err(|error| error.to_string())
}
