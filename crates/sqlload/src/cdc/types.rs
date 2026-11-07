use crate::cdc::models::SnapshotRecord;

/// The slot's newest snapshot LSN, each table recorded under it with the LSN it was copied at, and the tables recorded only under older slot records (captured before the slot was recreated).
pub type CurrentSnapshot = (i64, Vec<(i64, SnapshotRecord)>, Vec<String>);
