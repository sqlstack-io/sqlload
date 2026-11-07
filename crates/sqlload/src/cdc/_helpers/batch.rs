//! One table's buffered changes as the parts its load stages.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{Field, Schema};

use crate::adapters::contract::models::{ChangeApply, Truncation};
use crate::cdc::_helpers::fold::{Folder, fold_changes, key_of, key_positions};
use crate::cdc::_helpers::spill::{SpillFile, SpillReader};
use crate::cdc::_helpers::text_values::{arrow_type, micros_text, text_array};
use crate::cdc::constants::{
    DELETE_OP, INSERT_OP, SPILL_CHUNK_CHANGES, SPILL_PARTITIONS, TRUNCATE_OP, UPDATE_OP,
};
use crate::cdc::models::{Change, ChangeOp, Relation, TableChanges, TupleValue};
use crate::postgres_source::models::ReadColumn;
use crate::schema_plan::constants::{
    CDC_COMMITTED_AT_COLUMN, CDC_LSN_COLUMN, CDC_OP_COLUMN, CDC_PREFIX, CDC_SEQ_COLUMN,
    CDC_UNCHANGED_COLUMN,
};

/// The rows `changes` of `relation` write, with the columns `read` (data columns, then the change columns) in that order: a truncate is a row without values.
pub(crate) fn rows_batch(
    changes: &[Change],
    relation: &Relation,
    read: &[ReadColumn],
) -> Result<RecordBatch, String> {
    let positions = column_positions(relation, read)?;
    let data_positions: Vec<(&str, usize)> = read
        .iter()
        .zip(&positions)
        .filter_map(|(column, position)| Some((column.name.as_str(), (*position)?)))
        .collect();
    let mut fields: Vec<Field> = Vec::with_capacity(read.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(read.len());
    for (column, position) in read.iter().zip(&positions) {
        let array = match position {
            Some(position) => data_array(changes, *position, column)?,
            None => metadata_array(changes, column, &data_positions)?,
        };
        fields.push(Field::new(&column.name, array.data_type().clone(), true));
        arrays.push(array);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| error.to_string())
}

/// The changes one table's batch stages, part by part: a changelog keeps every change, in chunks once spilled; `merge` keeps each key's last change after the newest truncate, folded per key partition once spilled, so no key is in two parts.
#[derive(Debug)]
pub(crate) enum ChangeParts {
    /// Changes in memory, already applied, staged as one part.
    Ready(Option<Vec<Change>>),
    /// A spilled changelog: the file, then the changes still in memory.
    Log {
        reader: SpillReader,
        memory: std::vec::IntoIter<Change>,
    },
    /// A spilled `merge` table: one file per key partition, each folded as it is read, so a part holds one change per key.
    Partitions {
        files: std::collections::btree_map::IntoIter<u64, SpillFile>,
        positions: Vec<usize>,
        /// How many partitions the keys are hashed into.
        partitions: u64,
    },
}

impl ChangeParts {
    /// The next part's changes, or `None` when every part is staged.
    pub(crate) fn next_changes(&mut self) -> Result<Option<Vec<Change>>, String> {
        match self {
            Self::Ready(changes) => Ok(changes.take()),
            Self::Log { reader, memory } => {
                let mut part: Vec<Change> = Vec::new();
                while part.len() < SPILL_CHUNK_CHANGES {
                    match reader.next_change()? {
                        Some(change) => part.push(change),
                        None => break,
                    }
                }
                part.extend(memory.take(SPILL_CHUNK_CHANGES - part.len()));
                Ok((!part.is_empty()).then_some(part))
            }
            Self::Partitions {
                files,
                positions,
                partitions,
            } => {
                let Some((partition, mut file)) = files.next() else {
                    return Ok(None);
                };
                let mut reader = file.read()?;
                let mut folder = Folder::default();
                while let Some(change) = reader.next_change()? {
                    folder.push(change, positions)?;
                }
                let mut folded = folder.finish();
                folded.retain(|change| {
                    key_partition(&key_of(&change.values, positions), *partitions) == partition
                });
                Ok(Some(folded))
            }
        }
    }
}

/// `table`'s batch as parts, with the changes `spilled` to disk before those in memory, and for `merge` the newest truncate, which applies before them; a spilled `merge` table is split into enough key partitions that each holds about a quarter of `spill_bytes`, at most [`SPILL_PARTITIONS`].
pub(crate) fn change_parts(
    table: &TableChanges,
    (spilled, spill_bytes): (Option<SpillFile>, usize),
    (apply, unique_key): (ChangeApply, &[String]),
) -> Result<(ChangeParts, Option<Truncation>), String> {
    let Some(mut spilled) = spilled else {
        let (changes, truncated) = applied_changes(table, apply, unique_key)?;
        return Ok((ChangeParts::Ready(Some(changes)), truncated));
    };
    let memory = table.changes.clone().into_iter();
    if apply == ChangeApply::Changelog {
        let reader = spilled.read()?;
        return Ok((ChangeParts::Log { reader, memory }, None));
    }
    let mut last: Option<Change> = None;
    let mut reader = spilled.read()?;
    while let Some(change) = reader.next_change()? {
        if change.op == ChangeOp::Truncate {
            last = Some(change);
        }
    }
    if let Some(change) = table
        .changes
        .iter()
        .rev()
        .find(|change| change.op == ChangeOp::Truncate)
    {
        last = Some(change.clone());
    }
    let after = last.as_ref().map(|change| (change.lsn, change.seq));
    let positions = key_positions(&table.relation, unique_key)?;
    let spilled_bytes = u64::try_from(spilled.bytes).unwrap_or(u64::MAX);
    let limit = u64::try_from(spill_bytes.max(1)).unwrap_or(u64::MAX);
    let partitions = spilled_bytes
        .saturating_mul(4)
        .div_ceil(limit)
        .clamp(1, SPILL_PARTITIONS);
    let mut files = PartitionFiles::default();
    let mut reader = spilled.read()?;
    let mut memory = memory;
    loop {
        let change = match reader.next_change()? {
            Some(change) => change,
            None => match memory.next() {
                Some(change) => change,
                None => break,
            },
        };
        if after.is_some_and(|after| (change.lsn, change.seq) <= after) {
            continue;
        }
        let key = key_of(&change.values, &positions);
        if let Some(old) = change
            .old_key
            .as_ref()
            .filter(|old| key_of(old, &positions) != key)
        {
            let deleted = Change {
                op: ChangeOp::Delete,
                values: old.clone(),
                old_key: None,
                ..change.clone()
            };
            files.push(
                key_partition(&key_of(old, &positions), partitions),
                &deleted,
            )?;
        }
        files.push(key_partition(&key, partitions), &change)?;
    }
    let truncated = last.map(|change| Truncation {
        lsn: change.lsn,
        committed_at: micros_text(change.committed_at),
    });
    let parts = ChangeParts::Partitions {
        files: files.0.into_iter(),
        positions,
        partitions,
    };
    Ok((parts, truncated))
}

/// The partition, of `partitions`, a key's changes are folded in.
fn key_partition(key: &[TupleValue], partitions: u64) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    std::hash::Hash::hash(key, &mut hasher);
    std::hash::Hasher::finish(&hasher) % partitions
}

/// A spilled `merge` table's changes, filed by key partition.
#[derive(Debug, Default)]
struct PartitionFiles(std::collections::BTreeMap<u64, SpillFile>);

impl PartitionFiles {
    /// Append `change` to `partition`'s file, made on first use.
    fn push(&mut self, partition: u64, change: &Change) -> Result<(), String> {
        let file = match self.0.entry(partition) {
            std::collections::btree_map::Entry::Occupied(file) => file.into_mut(),
            std::collections::btree_map::Entry::Vacant(slot) => slot.insert(SpillFile::new()?),
        };
        file.push_change(change)
    }
}

/// The changes a batch in memory writes and, for `merge`, its newest truncate.
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
    Ok((fold_changes(kept, &positions)?, truncated))
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
                "{}.{} has no column {} on the stream: the table changed shape during this load, so run it again, and the next load plans for the change",
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
    text_array(&values, column).map_err(|problem| format!("{}: {problem}", column.name))
}

/// One change column: the commit LSN, position, operation and time; `_sqlload_unchanged` flags; NULL for the rest (`_sqlload_deleted_at`, which the write sets).
fn metadata_array(
    changes: &[Change],
    column: &ReadColumn,
    data_positions: &[(&str, usize)],
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

/// The data columns whose value the change left out, as `,a,b,`: an unchanged TOAST value or, for an insert or update, a column its shape lacked (a delete or truncate writes no data columns); `None` when it left none out.
fn unchanged_flags(change: &Change, data_positions: &[(&str, usize)]) -> Option<String> {
    let writes = matches!(change.op, ChangeOp::Insert | ChangeOp::Update);
    let mut names = String::new();
    for (name, position) in data_positions {
        let value = change.values.get(*position).unwrap_or(&TupleValue::Absent);
        if *value == TupleValue::Unchanged || (writes && *value == TupleValue::Absent) {
            names.push(',');
            names.push_str(name);
        }
    }
    (!names.is_empty()).then(|| format!("{names},"))
}
