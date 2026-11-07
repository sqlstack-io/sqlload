use crate::cdc::models::{Change, PgOutputMessage};

pub(crate) struct DecodeTestCase {
    pub(crate) description: &'static str,
    pub(crate) bytes: Vec<u8>,
    pub(crate) expected_message: Result<PgOutputMessage, &'static str>,
}

/// One table's batch of changes, of which the first `spilled` sit on disk, and the rows `merge` stages for it (`op:id:value`, by id) with its truncate's LSN.
pub(crate) struct ChangePartsTestCase {
    pub(crate) description: &'static str,
    pub(crate) changes: Vec<Change>,
    pub(crate) spilled: usize,
    pub(crate) expected_rows: (Vec<&'static str>, Option<i64>),
}

/// A transaction's stream messages, and the `_sqlload_seq` of each change it buffers, or the error its commit ends with.
pub(crate) struct BufferTestCase {
    pub(crate) description: &'static str,
    pub(crate) messages: Vec<Vec<u8>>,
    pub(crate) expected_seqs: Result<Vec<i64>, &'static str>,
}

/// Transactions on table 7 and the rows `merge` and a changelog stage for them (see `staged_texts`).
pub(crate) struct StagedShapesTestCase {
    pub(crate) description: &'static str,
    pub(crate) transactions: Vec<Vec<Vec<u8>>>,
    pub(crate) columns: Vec<&'static str>,
    pub(crate) expected_merge: Vec<&'static str>,
    pub(crate) expected_changelog: Vec<&'static str>,
}
