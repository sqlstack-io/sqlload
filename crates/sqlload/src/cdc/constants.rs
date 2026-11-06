/// The `_sqlload_op` of rows read from the initial snapshot.
pub const SNAPSHOT_OP: &str = "snapshot";
/// The `_sqlload_op` of an insert.
pub const INSERT_OP: &str = "insert";
/// The `_sqlload_op` of an update.
pub const UPDATE_OP: &str = "update";
/// The `_sqlload_op` of a delete.
pub const DELETE_OP: &str = "delete";
/// The `_sqlload_op` of a truncate.
pub const TRUNCATE_OP: &str = "truncate";
/// The prefix of the logical message marking where a catch-up load stops.
pub const STOP_MESSAGE_PREFIX: &str = "sqlload.stop";
/// The most changes a batch holds before it commits at the next transaction end.
pub const BATCH_CHANGES: usize = 100_000;
/// Session settings for the replication connection, so values arrive as the snapshot reads them.
pub const REPLICATION_OPTIONS: &str = "-c TimeZone=UTC -c DateStyle=ISO -c extra_float_digits=3";
/// Microseconds from 1970-01-01 to 2000-01-01, where Postgres counts commit times from.
pub const POSTGRES_EPOCH_MICROS: i64 = 946_684_800_000_000;
/// `pg_replication_slots.wal_status` of a slot that lost WAL it had not confirmed.
pub const LOST_WAL_STATUS: &str = "lost";
