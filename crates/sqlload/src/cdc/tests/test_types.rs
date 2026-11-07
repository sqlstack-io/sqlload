use crate::cdc::models::{CatalogColumn, Change, PgOutputMessage, SnapshotRecord};
use crate::cdc::types::CurrentSnapshot;

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
    pub(crate) gone: Vec<&'static str>,
    pub(crate) expected_merge: Result<Vec<&'static str>, &'static str>,
    pub(crate) expected_changelog: Result<Vec<&'static str>, &'static str>,
}

/// Transactions on table 7, and the replica identity and key flags of its batch shape.
pub(crate) struct UnionShapeTestCase {
    pub(crate) description: &'static str,
    pub(crate) transactions: Vec<Vec<Vec<u8>>>,
    pub(crate) expected_shape: (char, Vec<bool>),
}

/// A table's snapshot record (`,id,a,` with highest column number 3 and key `,id,` unless the case says otherwise), its plan and catalog now, and the record to write or why it must be copied again.
pub(crate) struct ReshapeTestCase {
    pub(crate) description: &'static str,
    pub(crate) recorded: SnapshotRecord,
    pub(crate) planned: (&'static str, &'static str, bool),
    pub(crate) known: Vec<&'static str>,
    pub(crate) catalog: Vec<CatalogColumn>,
    pub(crate) expected_rewrite: Result<Option<SnapshotRecord>, &'static str>,
}

/// Snapshot records as read back, and the newest snapshot they give, or the error.
pub(crate) struct SnapshotRecordsTestCase {
    pub(crate) description: &'static str,
    pub(crate) rows: Vec<&'static str>,
    pub(crate) expected_snapshot: Result<Option<CurrentSnapshot>, &'static str>,
}

/// Transactions on table 7, committed at LSN 100, 101, ..., the table's copy point, and the commit LSNs of the changes it keeps.
pub(crate) struct FloorTestCase {
    pub(crate) description: &'static str,
    pub(crate) transactions: Vec<Vec<Vec<u8>>>,
    pub(crate) floor: i64,
    pub(crate) expected_lsns: Vec<i64>,
}

/// A files target's path template and the folder that holds its source's snapshot records.
pub(crate) struct SnapshotFolderTestCase {
    pub(crate) description: &'static str,
    pub(crate) template: &'static str,
    pub(crate) expected_folder: &'static str,
}
