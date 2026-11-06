//! One table's buffered changes as the batch its load stages.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{Field, Schema};

use crate::adapters::contract::models::{ChangeApply, Truncation};
use crate::cdc::_helpers::fold::{fold_changes, key_positions};
use crate::cdc::_helpers::text_values::{arrow_type, micros_text, text_array};
use crate::cdc::constants::{DELETE_OP, INSERT_OP, TRUNCATE_OP, UPDATE_OP};
use crate::cdc::models::{Change, ChangeOp, Relation, TableChanges, TupleValue};
use crate::postgres_source::models::ReadColumn;
use crate::schema_plan::constants::{
    CDC_COMMITTED_AT_COLUMN, CDC_LSN_COLUMN, CDC_OP_COLUMN, CDC_PREFIX, CDC_SEQ_COLUMN,
    CDC_UNCHANGED_COLUMN,
};

/// The rows `table`'s changes write, with the columns `read` (data columns, then the change columns) in that order, and for `merge` the newest truncate, which applies before them: a changelog keeps every change, a truncate as a row without values; `merge` keeps each key's last change after the newest truncate.
pub(crate) fn changes_batch(
    table: &TableChanges,
    read: &[ReadColumn],
    (apply, unique_key): (ChangeApply, &[String]),
) -> Result<(RecordBatch, Option<Truncation>), String> {
    let (changes, truncated) = applied_changes(table, apply, unique_key)?;
    let positions = column_positions(&table.relation, read)?;
    let data_positions: Vec<usize> = positions.iter().flatten().copied().collect();
    let mut fields: Vec<Field> = Vec::with_capacity(read.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(read.len());
    for (column, position) in read.iter().zip(&positions) {
        let array = match position {
            Some(position) => data_array(&changes, *position, column)?,
            None => metadata_array(&changes, column, &data_positions)?,
        };
        fields.push(Field::new(&column.name, array.data_type().clone(), true));
        arrays.push(array);
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| error.to_string())?;
    Ok((batch, truncated))
}

/// The changes a batch writes and, for `merge`, its newest truncate.
fn applied_changes(
    table: &TableChanges,
    apply: ChangeApply,
    unique_key: &[String],
) -> Result<(Vec<Change>, Option<Truncation>), String> {
    if apply == ChangeApply::Changelog {
        return Ok((table.changes.clone(), None));
    }
    let last_truncate = table
        .changes
        .iter()
        .rposition(|change| change.op == ChangeOp::Truncate);
    let truncated = last_truncate
        .and_then(|index| table.changes.get(index))
        .map(|change| Truncation {
            lsn: change.lsn,
            committed_at: micros_text(change.committed_at),
        });
    let after = last_truncate.map_or(0, |index| index + 1);
    let positions = key_positions(&table.relation, unique_key)?;
    let kept = table.changes.get(after..).unwrap_or_default();
    Ok((fold_changes(kept, &positions), truncated))
}

/// Each read column's position on the stream; `None` for the change columns.
fn column_positions(
    relation: &Relation,
    read: &[ReadColumn],
) -> Result<Vec<Option<usize>>, String> {
    let mut positions: Vec<Option<usize>> = Vec::with_capacity(read.len());
    for column in read {
        if column.name.starts_with(CDC_PREFIX) {
            positions.push(None);
            continue;
        }
        let Some(position) = relation
            .columns
            .iter()
            .position(|source| source.name == column.name)
        else {
            return Err(format!(
                "{}.{} no longer has column {} on the stream; schema changes while streaming come in a later release, so re-snapshot with --resnapshot",
                relation.schema, relation.table, column.name
            ));
        };
        positions.push(Some(position));
    }
    Ok(positions)
}

/// The values of the stream's column `position` as `column`'s type; an unchanged TOAST value is NULL here and flagged in `_sqlload_unchanged`.
fn data_array(
    changes: &[Change],
    position: usize,
    column: &ReadColumn,
) -> Result<ArrayRef, String> {
    let mut values: Vec<Option<&str>> = Vec::with_capacity(changes.len());
    for change in changes {
        values.push(match change.values.get(position) {
            Some(TupleValue::Text(text)) => Some(text.as_str()),
            _ => None,
        });
    }
    text_array(&values, &column.output).map_err(|problem| format!("{}: {problem}", column.name))
}

/// One change column: the commit LSN, position, operation and time; `_sqlload_unchanged` flags; NULL for the rest (`_sqlload_deleted_at`, which the write sets).
fn metadata_array(
    changes: &[Change],
    column: &ReadColumn,
    data_positions: &[usize],
) -> Result<ArrayRef, String> {
    Ok(match column.name.as_str() {
        CDC_LSN_COLUMN => Arc::new(Int64Array::from_iter_values(
            changes.iter().map(|change| change.lsn),
        )),
        CDC_SEQ_COLUMN => Arc::new(Int64Array::from_iter_values(
            changes.iter().map(|change| change.seq),
        )),
        CDC_OP_COLUMN => Arc::new(StringArray::from_iter_values(
            changes.iter().map(|change| op_name(change.op)),
        )),
        CDC_COMMITTED_AT_COLUMN => Arc::new(
            TimestampMicrosecondArray::from_iter_values(
                changes.iter().map(|change| change.committed_at),
            )
            .with_timezone("UTC"),
        ),
        CDC_UNCHANGED_COLUMN => Arc::new(StringArray::from_iter(
            changes
                .iter()
                .map(|change| unchanged_flags(change, data_positions)),
        )),
        _ => arrow_array::new_null_array(&arrow_type(&column.output)?, changes.len()),
    })
}

/// `_sqlload_op` for `op`.
fn op_name(op: ChangeOp) -> &'static str {
    match op {
        ChangeOp::Insert => INSERT_OP,
        ChangeOp::Update => UPDATE_OP,
        ChangeOp::Delete => DELETE_OP,
        ChangeOp::Truncate => TRUNCATE_OP,
    }
}

/// One `0` or `1` per data column, `1` where the change left an unchanged TOAST value out; `None` when it left none out.
fn unchanged_flags(change: &Change, data_positions: &[usize]) -> Option<String> {
    let mut flags = String::with_capacity(data_positions.len());
    let mut any = false;
    for position in data_positions {
        let unchanged = change.values.get(*position) == Some(&TupleValue::Unchanged);
        any |= unchanged;
        flags.push(if unchanged { '1' } else { '0' });
    }
    any.then_some(flags)
}
