//! One table's buffered changes as the batch its load stages.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{Field, Schema};

use crate::adapters::contract::models::{ChangeApply, Truncation};
use crate::cdc::_helpers::fold::{fold_changes, key_positions};
use crate::cdc::_helpers::text_values::{arrow_type, micros_text, text_array};
use crate::cdc::constants::{DELETE_OP, INSERT_OP, TRUNCATE_OP, UPDATE_OP};
use crate::cdc::models::{Change, ChangeOp, TableChanges, TupleValue};
use crate::postgres_source::models::ReadColumn;
use crate::schema_plan::constants::{
    CDC_COMMITTED_AT_COLUMN, CDC_LSN_COLUMN, CDC_OP_COLUMN, CDC_PREFIX, CDC_SEQ_COLUMN,
    CDC_UNCHANGED_COLUMN,
};

/// The rows `table`'s changes write, with the columns `read` (data columns, then the change columns) in that order, and for `merge` the newest truncate, which applies before them: a changelog keeps every change, a truncate as a row without values; `merge` keeps each key's last change after the newest truncate.
pub fn changes_batch(
    table: &TableChanges,
    read: &[ReadColumn],
    (apply, unique_key): (ChangeApply, &[String]),
) -> Result<(RecordBatch, Option<Truncation>), String> {
    let relation = &table.relation;
    let (changes, truncated): (Vec<Change>, Option<Truncation>) = match apply {
        ChangeApply::Changelog => (table.changes.clone(), None),
        _ => {
            let last_truncate = table
                .changes
                .iter()
                .rposition(|change| change.op == ChangeOp::Truncate);
            let after = last_truncate.map_or(0, |index| index + 1);
            let truncated = last_truncate
                .and_then(|index| table.changes.get(index))
                .map(|change| Truncation {
                    lsn: change.lsn,
                    committed_at: micros_text(change.committed_at),
                });
            let positions = key_positions(relation, unique_key)?;
            let kept = table.changes.get(after..).unwrap_or_default();
            (fold_changes(kept, &positions), truncated)
        }
    };
    let data: Vec<(&ReadColumn, Option<usize>)> = read
        .iter()
        .filter(|column| !column.name.starts_with(CDC_PREFIX))
        .map(|column| {
            let position = relation
                .columns
                .iter()
                .position(|source| source.name == column.name);
            (column, position)
        })
        .collect();
    if let Some((missing, _)) = data.iter().find(|(_, position)| position.is_none()) {
        return Err(format!(
            "{}.{} no longer has column {} on the stream; schema changes while streaming come in a later release, so re-snapshot with --resnapshot",
            relation.schema, relation.table, missing.name
        ));
    }
    let mut fields: Vec<Field> = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for column in read {
        let array: ArrayRef = match column.name.as_str() {
            CDC_LSN_COLUMN => Arc::new(Int64Array::from_iter_values(
                changes.iter().map(|change| change.lsn),
            )),
            CDC_SEQ_COLUMN => Arc::new(Int64Array::from_iter_values(
                changes.iter().map(|change| change.seq),
            )),
            CDC_OP_COLUMN => Arc::new(StringArray::from_iter_values(changes.iter().map(
                |change| match change.op {
                    ChangeOp::Insert => INSERT_OP,
                    ChangeOp::Update => UPDATE_OP,
                    ChangeOp::Delete => DELETE_OP,
                    ChangeOp::Truncate => TRUNCATE_OP,
                },
            ))),
            CDC_COMMITTED_AT_COLUMN => Arc::new(
                TimestampMicrosecondArray::from_iter_values(
                    changes.iter().map(|change| change.committed_at),
                )
                .with_timezone("UTC"),
            ),
            CDC_UNCHANGED_COLUMN => {
                Arc::new(StringArray::from_iter(changes.iter().map(|change| {
                    let flags: String = data
                        .iter()
                        .map(|(_, position)| {
                            match position.and_then(|position| change.values.get(position)) {
                                Some(TupleValue::Unchanged) => '1',
                                _ => '0',
                            }
                        })
                        .collect();
                    flags.contains('1').then_some(flags)
                })))
            }
            name if name.starts_with(CDC_PREFIX) => {
                arrow_array::new_null_array(&arrow_type(&column.output)?, changes.len())
            }
            _ => {
                let position = relation
                    .columns
                    .iter()
                    .position(|source| source.name == column.name);
                let values: Vec<Option<&str>> = changes
                    .iter()
                    .map(
                        |change| match position.and_then(|position| change.values.get(position)) {
                            Some(TupleValue::Text(text)) => Some(text.as_str()),
                            _ => None,
                        },
                    )
                    .collect();
                text_array(&values, &column.output)
                    .map_err(|problem| format!("{}: {problem}", column.name))?
            }
        };
        fields.push(Field::new(&column.name, array.data_type().clone(), true));
        arrays.push(array);
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| error.to_string())?;
    Ok((batch, truncated))
}
