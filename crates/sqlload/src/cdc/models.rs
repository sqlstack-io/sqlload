/// One column of a relation as pgoutput describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationColumn {
    pub name: String,
    /// Part of the replica identity (the key pgoutput sends old values for).
    pub key: bool,
    pub type_oid: u32,
    pub type_modifier: i32,
}

/// A table's shape on the stream: sent before its first change, and again after it changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub oid: u32,
    pub schema: String,
    pub table: String,
    /// `d` default (primary key), `n` nothing, `f` full, `i` index.
    pub replica_identity: char,
    pub columns: Vec<RelationColumn>,
}

/// One column value of a changed row.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TupleValue {
    Null,
    /// A TOASTed value the change did not touch: pgoutput leaves it out.
    Unchanged,
    /// A column the change's table did not have when it was written: added after it, or dropped before it. Written like an unchanged value, so `merge` keeps the live one.
    Absent,
    Text(String),
}

/// One pgoutput message inside a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgOutputMessage {
    Relation(Relation),
    Insert {
        relation: u32,
        new: Vec<TupleValue>,
    },
    /// `old` holds the replica identity's old values (`K`) or the whole old row (`O`, replica identity full) when the server sent them.
    Update {
        relation: u32,
        old: Option<Vec<TupleValue>>,
        new: Vec<TupleValue>,
    },
    Delete {
        relation: u32,
        old: Vec<TupleValue>,
    },
    Truncate {
        relations: Vec<u32>,
        cascade: bool,
        restart_identity: bool,
    },
    /// What the decoder does not use: a type, an origin, or a message it does not know.
    Other(char),
}

/// What a captured change did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
    /// The whole table was emptied; the change has no values.
    Truncate,
}

/// One captured row change, in its transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub op: ChangeOp,
    /// The row's values in relation column order: the new row, or the old key for a delete.
    pub values: Vec<TupleValue>,
    /// An update's old key values, when the key changed (replica identity sends them then).
    pub old_key: Option<Vec<TupleValue>>,
    /// The transaction's commit LSN.
    pub lsn: i64,
    /// The change's position within its transaction, from 1.
    pub seq: i64,
    /// When the transaction committed: microseconds since 1970 UTC.
    pub committed_at: i64,
}

/// One table's changes in a batch: its shape on the stream, and its changes in commit order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableChanges {
    /// Every shape the batch saw, as one: the first shape's columns, then columns later shapes added, with types and key flags from the newest. A change's values follow these columns; one written before a column was added stops short of it.
    pub relation: Relation,
    pub changes: Vec<Change>,
}

/// Whole transactions read from the stream and not yet written: each table's changes, the shapes seen, and the transaction in progress. Past `spill_bytes` in memory, the open transaction's messages and the tables' changes move to local files, so a batch can outgrow memory.
#[derive(Debug, Default)]
pub struct ChangeBuffer {
    /// The relations sent so far, by oid; they stay known across batches.
    pub relations: std::collections::BTreeMap<u32, Relation>,
    /// Committed changes per relation oid still in memory (a table's spilled ones come before them).
    pub tables: std::collections::BTreeMap<u32, TableChanges>,
    /// Committed changes per relation oid moved to local disk.
    pub(crate) spilled: std::collections::BTreeMap<u32, crate::cdc::_helpers::spill::SpillFile>,
    /// The open transaction's raw messages in memory, kept until its commit.
    pub open: Vec<Vec<u8>>,
    /// The open transaction's raw messages on local disk, once they outgrew memory.
    pub(crate) open_spill: Option<crate::cdc::_helpers::spill::SpillFile>,
    /// About how many bytes `open` and `tables` hold.
    pub bytes: usize,
    /// How many bytes the buffer keeps in memory before spilling.
    pub spill_bytes: usize,
    /// Changes committed into the batch.
    pub count: usize,
    /// The newest committed transaction's end LSN, which the slot may confirm once the batch is written.
    pub end_lsn: Option<u64>,
    /// Each table's copy point by `(schema, table)`: its changes committed before it are already in its copy, so they are dropped. A table copied alone after the slot was made (a backfill) has a point later than the slot's.
    pub floors: std::collections::BTreeMap<(String, String), i64>,
}

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

/// A captured table's snapshot record: the data columns it was copied with (`,a,b,`) or last resumed with, the highest column number the table had then (dropped columns included), its key (`,id,`), and the columns it stopped loading since its snapshot (`,c,`), whose destination values no longer follow the source. Records written before column numbers were kept carry none of the last three (`left` empty).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRecord {
    pub table: String,
    pub columns: String,
    pub max_attnum: Option<i16>,
    pub key: Option<String>,
    pub left: String,
}

/// A source table's column in the catalog now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogColumn {
    pub name: String,
    pub attnum: i16,
    pub dropped: bool,
    /// It has a default (`atthasdef`), rows written before it was added read a stored value (`atthasmissing`), or it is an identity or generated column: values the stream never carried.
    pub has_default: bool,
}

/// A captured table as this load plans it: its data columns (`,a,b,`), key (`,id,`), how changes apply, and the source columns the plan knew (excluded ones included).
#[derive(Debug, Clone, Copy)]
pub struct PlannedShape<'a> {
    pub columns: &'a str,
    pub key: &'a str,
    pub merge: bool,
    pub known: &'a [String],
}

/// What a following stream waits on next.
#[derive(Debug)]
pub enum FollowStep {
    /// The stream's next event; `None` once it ended.
    Event(Option<pgwire_replication::client::ReplicationEvent>),
    /// The batch interval passed.
    Tick,
    /// Following was asked to stop.
    Stop,
}

/// What a stream read: the end LSN it confirmed last (`None` when it confirmed none), the rows each table wrote, and why it stopped to re-plan, when a table gained a column while following.
#[derive(Debug, Default)]
pub struct Streamed {
    pub end: Option<u64>,
    pub rows: std::collections::BTreeMap<String, u64>,
    pub replan: Option<String>,
}
