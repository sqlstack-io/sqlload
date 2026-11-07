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
pub const REPLICATION_OPTIONS: &str = "-c TimeZone=UTC -c DateStyle=ISO -c IntervalStyle=postgres -c extra_float_digits=3 -c bytea_output=hex";
/// The same settings for the snapshot's session, so a value it reads as text matches the streamed one.
pub const SNAPSHOT_SETTINGS: &str = "SET TimeZone = 'UTC'; SET DateStyle = 'ISO'; SET IntervalStyle = 'postgres'; SET extra_float_digits = 3; SET bytea_output = 'hex'";
/// Microseconds from 1970-01-01 to 2000-01-01, where Postgres counts commit times from.
pub const POSTGRES_EPOCH_MICROS: i64 = 946_684_800_000_000;
/// `pg_replication_slots.wal_status` of a slot that lost WAL it had not confirmed.
pub const LOST_WAL_STATUS: &str = "lost";
/// `relreplident` of a table whose changes carry the whole old row.
pub const FULL_IDENTITY: char = 'f';
/// The Postgres type name of `json`, which keeps the text as written (unlike `jsonb`).
pub const PLAIN_JSON_TYPE: &str = "json";
/// How many bytes of changes a load keeps in memory before it moves them to local disk.
pub const SPILL_BYTES: usize = 256 * 1024 * 1024;
/// About how many bytes a change takes in memory beyond its values.
pub const CHANGE_OVERHEAD_BYTES: usize = 96;
/// About how many bytes a value takes in memory beyond its text.
pub const VALUE_OVERHEAD_BYTES: usize = 32;
/// The most parts a spilled `merge` table's changes are split into by key, each folded on its own (each part is a file open at once).
pub const SPILL_PARTITIONS: u64 = 256;
/// The most spilled changelog changes staged at once.
pub const SPILL_CHUNK_CHANGES: usize = 50_000;

/// The position of a read column a batch never showed: past every change's values, so each leaves it out.
pub const NOT_STREAMED: usize = usize::MAX;

/// The first Postgres version (`server_version_num`) whose publications can list columns.
pub const COLUMN_LISTS_VERSION: i32 = 150_000;

/// The first Postgres version (`server_version_num`) that can publish generated columns.
pub const GENERATED_COLUMNS_VERSION: i32 = 180_000;

/// The suffix of the temporary slot a load makes to copy tables that joined a source alone.
pub const BACKFILL_SLOT_SUFFIX: &str = "_backfill";

/// The longest replication slot name Postgres takes (`NAMEDATALEN - 1`).
pub const SLOT_NAME_BYTES: usize = 63;

/// What Postgres says when a slot cannot be made because every one under `max_replication_slots` is taken.
pub const SLOTS_FULL_ERROR: &str = "all replication slots are in use";
