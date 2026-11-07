//! Keep a transaction's messages until it commits, then file its changes by table, on local disk once they outgrow memory.

use crate::cdc::_helpers::pgoutput::decode_message;
use crate::cdc::_helpers::spill::SpillFile;
use crate::cdc::constants::{
    CHANGE_OVERHEAD_BYTES, FULL_IDENTITY, POSTGRES_EPOCH_MICROS, VALUE_OVERHEAD_BYTES,
};
use crate::cdc::models::{
    Change, ChangeBuffer, ChangeOp, PgOutputMessage, Relation, RelationColumn, StreamStep,
    TableChanges, TupleValue,
};

/// Apply one stream step to `buffer`: messages wait in the open transaction, and its commit files its changes under their tables with the commit LSN, so a batch only ever holds whole transactions. A relation message takes effect at its place in the transaction, so changes before a DDL are read with the shape they were written in; a table that changes shape within a batch keeps one shape that covers them all, and each change's values are put in its columns.
pub(crate) fn buffer_change(
    mut buffer: ChangeBuffer,
    step: StreamStep<'_>,
) -> Result<ChangeBuffer, String> {
    match step {
        StreamStep::Begin => {
            buffer.bytes = buffer.bytes.saturating_sub(open_bytes(&buffer.open));
            buffer.open.clear();
            buffer.open_spill = None;
        }
        StreamStep::Message(bytes) => match decode_message(bytes)? {
            PgOutputMessage::Other(_) => {}
            _ => buffer = hold_message(buffer, bytes)?,
        },
        StreamStep::Commit { lsn, end_lsn, time } => {
            let commit = Commit {
                lsn: i64::try_from(lsn).map_err(|error| error.to_string())?,
                committed_at: time.saturating_add(POSTGRES_EPOCH_MICROS),
            };
            buffer.bytes = buffer.bytes.saturating_sub(open_bytes(&buffer.open));
            let held = std::mem::take(&mut buffer.open);
            let mut index = 0;
            if let Some(mut spilled) = buffer.open_spill.take() {
                let mut reader = spilled.read()?;
                while let Some(message) = reader.next_bytes()? {
                    let message = decode_message(&message)?;
                    index += usize::from(!matches!(message, PgOutputMessage::Relation(_)));
                    buffer = file_message(buffer, &message, (commit, index))?;
                }
            }
            for message in held {
                let message = decode_message(&message)?;
                index += usize::from(!matches!(message, PgOutputMessage::Relation(_)));
                buffer = file_message(buffer, &message, (commit, index))?;
            }
            buffer.end_lsn = Some(end_lsn);
        }
    }
    Ok(buffer)
}

/// A transaction's commit LSN and time (microseconds since 1970 UTC).
#[derive(Debug, Clone, Copy)]
struct Commit {
    lsn: i64,
    committed_at: i64,
}

/// Keep one message of the open transaction: in memory, or on local disk once the buffer passes its limit.
fn hold_message(mut buffer: ChangeBuffer, bytes: &[u8]) -> Result<ChangeBuffer, String> {
    if let Some(spilled) = &mut buffer.open_spill {
        spilled.push_bytes(bytes)?;
        return Ok(buffer);
    }
    buffer.open.push(bytes.to_vec());
    buffer.bytes += bytes.len();
    if buffer.bytes > buffer.spill_bytes {
        let mut spilled = SpillFile::new()?;
        for message in &buffer.open {
            spilled.push_bytes(message)?;
        }
        buffer.bytes = buffer.bytes.saturating_sub(open_bytes(&buffer.open));
        buffer.open.clear();
        buffer.open_spill = Some(spilled);
        tracing::info!(
            limit = buffer.spill_bytes,
            "a transaction outgrew the memory limit; spilling it to local disk"
        );
    }
    Ok(buffer)
}

/// The bytes of the open transaction's messages in memory.
fn open_bytes(open: &[Vec<u8>]) -> usize {
    open.iter().map(Vec::len).sum()
}

/// File the changes of `message`, the transaction's `index`th, under their tables; past the limit every table's changes move to local disk.
fn file_message(
    mut buffer: ChangeBuffer,
    message: &PgOutputMessage,
    (commit, index): (Commit, usize),
) -> Result<ChangeBuffer, String> {
    let seq = i64::try_from(index).unwrap_or(i64::MAX);
    let change = |op, values: &[TupleValue], old_key: &Option<Vec<TupleValue>>| Change {
        op,
        values: values.to_vec(),
        old_key: old_key.clone(),
        lsn: commit.lsn,
        seq,
        committed_at: commit.committed_at,
    };
    let filed: Vec<(u32, Change)> = match message {
        PgOutputMessage::Insert { relation, new } => {
            vec![(*relation, change(ChangeOp::Insert, new, &None))]
        }
        PgOutputMessage::Update { relation, old, new } => {
            vec![(*relation, change(ChangeOp::Update, new, old))]
        }
        PgOutputMessage::Delete { relation, old } => {
            vec![(*relation, change(ChangeOp::Delete, old, &None))]
        }
        PgOutputMessage::Truncate { relations, .. } => relations
            .iter()
            .map(|relation| (*relation, change(ChangeOp::Truncate, &[], &None)))
            .collect(),
        PgOutputMessage::Relation(relation) => {
            if let Some(table) = buffer.tables.get_mut(&relation.oid) {
                table.relation = union_shape(&table.relation, relation);
            }
            buffer.relations.insert(relation.oid, relation.clone());
            Vec::new()
        }
        PgOutputMessage::Other(_) => Vec::new(),
    };
    for (oid, mut change) in filed {
        let Some(relation) = buffer.relations.get(&oid) else {
            return Err(format!(
                "a change refers to relation {oid}, which the stream never described"
            ));
        };
        let copied = buffer
            .floors
            .get(&(relation.schema.clone(), relation.table.clone()))
            .is_some_and(|floor| change.lsn < *floor);
        if copied {
            continue;
        }
        let table = buffer.tables.entry(oid).or_insert_with(|| TableChanges {
            relation: relation.clone(),
            changes: Vec::new(),
        });
        if table.relation.columns != relation.columns {
            change.values = fit_values(&change.values, &table.relation, relation);
            change.old_key = change
                .old_key
                .map(|old| fit_values(&old, &table.relation, relation));
        }
        buffer.bytes += change_bytes(&change);
        table.changes.push(change);
        buffer.count += 1;
    }
    match buffer.bytes > buffer.spill_bytes {
        true => spill_tables(buffer),
        false => Ok(buffer),
    }
}

/// `current` grown by `newest`: its columns in order, with type from `newest`, then the columns `newest` adds. A column is a key only while every shape that had it keys it, and the replica identity is full only while every shape was, so changes written under a narrower identity still need their old key.
fn union_shape(current: &Relation, newest: &Relation) -> Relation {
    let mut columns: Vec<RelationColumn> = current
        .columns
        .iter()
        .map(|column| {
            named(newest, &column.name)
                .and_then(|position| newest.columns.get(position))
                .map_or_else(
                    || RelationColumn {
                        key: false,
                        ..column.clone()
                    },
                    |now| RelationColumn {
                        key: column.key && now.key,
                        ..now.clone()
                    },
                )
        })
        .collect();
    for column in &newest.columns {
        if !current
            .columns
            .iter()
            .any(|known| known.name == column.name)
        {
            columns.push(column.clone());
        }
    }
    let replica_identity = match current.replica_identity {
        FULL_IDENTITY => newest.replica_identity,
        narrower => narrower,
    };
    Relation {
        columns,
        replica_identity,
        ..newest.clone()
    }
}

/// `values` of a change in `shape`'s columns, put in `union`'s: by name, absent where `shape` lacks a column.
fn fit_values(values: &[TupleValue], union: &Relation, shape: &Relation) -> Vec<TupleValue> {
    union
        .columns
        .iter()
        .map(|column| {
            named(shape, &column.name)
                .and_then(|position| values.get(position).cloned())
                .unwrap_or(TupleValue::Absent)
        })
        .collect()
}

/// The position of column `name` in `relation`.
fn named(relation: &Relation, name: &str) -> Option<usize> {
    relation
        .columns
        .iter()
        .position(|column| column.name == name)
}

/// About how many bytes `change` takes in memory.
fn change_bytes(change: &Change) -> usize {
    let text = |values: &[TupleValue]| -> usize {
        values
            .iter()
            .map(|value| match value {
                TupleValue::Text(text) => text.len() + VALUE_OVERHEAD_BYTES,
                _ => VALUE_OVERHEAD_BYTES,
            })
            .sum()
    };
    CHANGE_OVERHEAD_BYTES + text(&change.values) + change.old_key.as_deref().map_or(0, text)
}

/// Move every table's changes in memory to the end of its file on local disk, keeping its shape in memory.
fn spill_tables(mut buffer: ChangeBuffer) -> Result<ChangeBuffer, String> {
    if buffer.spilled.is_empty() {
        tracing::info!(
            limit = buffer.spill_bytes,
            "a batch outgrew the memory limit; spilling its changes to local disk"
        );
    }
    for (oid, table) in &mut buffer.tables {
        let spilled = match buffer.spilled.entry(*oid) {
            std::collections::btree_map::Entry::Occupied(file) => file.into_mut(),
            std::collections::btree_map::Entry::Vacant(slot) => slot.insert(SpillFile::new()?),
        };
        for change in table.changes.drain(..) {
            spilled.push_change(&change)?;
        }
    }
    buffer.bytes = open_bytes(&buffer.open);
    Ok(buffer)
}
