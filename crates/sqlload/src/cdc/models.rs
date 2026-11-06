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
#[derive(Debug, Clone, PartialEq, Eq)]
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
