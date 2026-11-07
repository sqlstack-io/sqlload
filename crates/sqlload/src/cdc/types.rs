use crate::cdc::models::SnapshotRecord;

/// The slot's newest snapshot LSN, and each table recorded under it with the LSN it was copied at.
pub type CurrentSnapshot = (i64, Vec<(i64, SnapshotRecord)>);
