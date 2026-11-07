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
