//! Keep a transaction's messages until it commits, then file its changes by table, on local disk once they outgrow memory.

use crate::cdc::_helpers::pgoutput::decode_message;
use crate::cdc::_helpers::spill::SpillFile;
use crate::cdc::constants::{CHANGE_OVERHEAD_BYTES, POSTGRES_EPOCH_MICROS};
use crate::cdc::models::{
    Change, ChangeBuffer, ChangeOp, PgOutputMessage, StreamStep, TableChanges, TupleValue,
};

/// Apply one stream step to `buffer`: messages wait in the open transaction, and its commit files its changes under their tables with the commit LSN, so a batch only ever holds whole transactions. A relation message updates the table's shape at once.
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
            PgOutputMessage::Relation(relation) => {
                let reshaped = buffer
                    .tables
                    .get(&relation.oid)
                    .is_some_and(|table| table.relation.columns != relation.columns);
                if reshaped {
                    return Err(format!(
                        "{}.{} changed shape while streaming; schema changes while streaming come in a later release, so re-snapshot with --resnapshot",
                        relation.schema, relation.table
                    ));
                }
                buffer.relations.insert(relation.oid, relation);
            }
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
                    index += 1;
                    buffer = file_message(buffer, &decode_message(&message)?, (commit, index))?;
                }
            }
            for message in held {
                index += 1;
                buffer = file_message(buffer, &decode_message(&message)?, (commit, index))?;
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
        PgOutputMessage::Relation(_) | PgOutputMessage::Other(_) => Vec::new(),
    };
    for (oid, change) in filed {
        let Some(relation) = buffer.relations.get(&oid) else {
            return Err(format!(
                "a change refers to relation {oid}, which the stream never described"
            ));
        };
        buffer.bytes += change_bytes(&change);
        buffer
            .tables
            .entry(oid)
            .or_insert_with(|| TableChanges {
                relation: relation.clone(),
                changes: Vec::new(),
            })
            .changes
            .push(change);
        buffer.count += 1;
    }
    match buffer.bytes > buffer.spill_bytes {
        true => spill_tables(buffer),
        false => Ok(buffer),
    }
}

/// About how many bytes `change` takes in memory.
fn change_bytes(change: &Change) -> usize {
    let text = |values: &[TupleValue]| -> usize {
        values
            .iter()
            .map(|value| match value {
                TupleValue::Text(text) => text.len() + CHANGE_OVERHEAD_BYTES,
                _ => CHANGE_OVERHEAD_BYTES,
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
