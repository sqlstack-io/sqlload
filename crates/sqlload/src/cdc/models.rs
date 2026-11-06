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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum TupleValue {
    Null,
    /// A TOASTed value the change did not touch: pgoutput leaves it out.
    Unchanged,
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
    pub relation: Relation,
    pub changes: Vec<Change>,
}

/// Whole transactions read from the stream and not yet written: each table's changes, the shapes seen, and the transaction in progress.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChangeBuffer {
    /// The relations sent so far, by oid; they stay known across batches.
    pub relations: std::collections::BTreeMap<u32, Relation>,
    /// Committed changes per relation oid.
    pub tables: std::collections::BTreeMap<u32, TableChanges>,
    /// The open transaction's messages, kept until its commit.
    pub open: Vec<PgOutputMessage>,
    /// Changes committed into `tables`.
    pub count: usize,
    /// The newest committed transaction's end LSN, which the slot may confirm once the batch is written.
    pub end_lsn: Option<u64>,
}
