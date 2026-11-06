//! Keep a transaction's messages until it commits, then file its changes by table.

use crate::cdc::_helpers::pgoutput::decode_message;
use crate::cdc::models::{Change, ChangeBuffer, ChangeOp, PgOutputMessage, TableChanges};

/// Microseconds from 1970-01-01 to 2000-01-01, where Postgres counts commit times from.
const POSTGRES_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// What the stream delivered next.
#[derive(Debug, Clone, Copy)]
pub enum StreamStep<'a> {
    /// A transaction's start: anything left open before it is dropped.
    Begin,
    /// One pgoutput message of the open transaction.
    Message(&'a [u8]),
    /// The open transaction's commit: its LSN, end LSN and time (microseconds since 2000 UTC).
    Commit { lsn: u64, end_lsn: u64, time: i64 },
}

/// Apply one stream step to `buffer`: messages wait in the open transaction, and its commit files its changes under their tables with the commit LSN, so a batch only ever holds whole transactions. A relation message updates the table's shape at once.
pub fn buffer_change(buffer: &mut ChangeBuffer, step: StreamStep<'_>) -> Result<(), String> {
    match step {
        StreamStep::Begin => buffer.open.clear(),
        StreamStep::Message(bytes) => match decode_message(bytes)? {
            PgOutputMessage::Relation(relation) => {
                buffer.relations.insert(relation.oid, relation);
            }
            PgOutputMessage::Other(_) => {}
            message => buffer.open.push(message),
        },
        StreamStep::Commit { lsn, end_lsn, time } => {
            let lsn = i64::try_from(lsn).map_err(|error| error.to_string())?;
            let committed_at = time.saturating_add(POSTGRES_EPOCH_MICROS);
            let messages = std::mem::take(&mut buffer.open);
            for (index, message) in messages.into_iter().enumerate() {
                let seq = i64::try_from(index + 1).unwrap_or(i64::MAX);
                let change = |op, values, old_key| Change {
                    op,
                    values,
                    old_key,
                    lsn,
                    seq,
                    committed_at,
                };
                let filed: Vec<(u32, Change)> = match message {
                    PgOutputMessage::Insert { relation, new } => {
                        vec![(relation, change(ChangeOp::Insert, new, None))]
                    }
                    PgOutputMessage::Update { relation, old, new } => {
                        vec![(relation, change(ChangeOp::Update, new, old))]
                    }
                    PgOutputMessage::Delete { relation, old } => {
                        vec![(relation, change(ChangeOp::Delete, old, None))]
                    }
                    PgOutputMessage::Truncate { relations, .. } => relations
                        .into_iter()
                        .map(|relation| (relation, change(ChangeOp::Truncate, Vec::new(), None)))
                        .collect(),
                    PgOutputMessage::Relation(_) | PgOutputMessage::Other(_) => Vec::new(),
                };
                for (oid, change) in filed {
                    let Some(relation) = buffer.relations.get(&oid) else {
                        return Err(format!(
                            "a change refers to relation {oid}, which the stream never described"
                        ));
                    };
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
            }
            buffer.end_lsn = Some(end_lsn);
        }
    }
    Ok(())
}
