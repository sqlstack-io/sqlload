use crate::postgres_source::models::PgType;

/// The first 11 bytes of every binary `COPY` stream.
pub const COPY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";

/// Bytes before the header extension: signature, flags and extension length.
pub const HEADER_FIXED_LENGTH: usize = 19;

/// Where the header extension length starts, after the signature and flags.
pub const HEADER_EXTENSION_OFFSET: usize = 15;

/// The tuple field count that marks the end of a binary `COPY` stream.
pub const COPY_TRAILER: i16 = -1;

/// The field length that marks SQL `NULL` in a binary `COPY` row.
pub const NULL_FIELD_LENGTH: i32 = -1;

/// Days from 1970-01-01 (Arrow's epoch) to 2000-01-01 (Postgres's).
pub const POSTGRES_EPOCH_DAYS: i32 = 10_957;

/// Microseconds from 1970-01-01 to 2000-01-01.
pub const POSTGRES_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// Rows per Arrow batch read from Postgres.
pub const DEFAULT_BATCH_ROWS: usize = 65_536;

/// Bytes per Arrow batch read from Postgres (64 MiB), however few rows that is.
pub const DEFAULT_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Decimal digits per base-10000 `numeric` digit.
pub const NUMERIC_DIGIT_WIDTH: i32 = 4;

/// `numeric` sign words.
pub const NUMERIC_POSITIVE: u16 = 0x0000;
pub const NUMERIC_NEGATIVE: u16 = 0x4000;

/// Every user column of every table, view and materialised view, in column order.
pub const CATALOG_COLUMNS_SQL: &str = "
SELECT n.nspname, c.relname, a.attname, format_type(a.atttypid, a.atttypmod), t.typname,
       information_schema._pg_numeric_precision(a.atttypid, a.atttypmod),
       information_schema._pg_numeric_scale(a.atttypid, a.atttypmod),
       information_schema._pg_char_max_length(a.atttypid, a.atttypmod),
       t.typcategory::text, NOT a.attnotnull,
       c.reltuples::bigint, pg_total_relation_size(c.oid), obj_description(c.oid, 'pg_class'),
       CASE WHEN current_setting('server_version_num')::int >= 140000 AND c.relkind = 'r'
            THEN pg_relation_size(c.oid) / current_setting('block_size')::bigint END
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
WHERE c.relkind IN ('r', 'p', 'v', 'm') AND a.attnum > 0 AND NOT a.attisdropped
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  AND n.nspname NOT LIKE 'pg\\_temp\\_%' AND n.nspname NOT LIKE 'pg\\_toast\\_temp\\_%'
  AND NOT c.relispartition
ORDER BY n.nspname, c.relname, a.attnum";

/// Primary key columns, in key order.
pub const CATALOG_PRIMARY_KEYS_SQL: &str = "
SELECT n.nspname, c.relname, a.attname
FROM pg_catalog.pg_constraint k
JOIN pg_catalog.pg_class c ON c.oid = k.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL unnest(k.conkey) WITH ORDINALITY AS key(attnum, position)
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = key.attnum
WHERE k.contype = 'p'
ORDER BY n.nspname, c.relname, key.position";

/// Columns leading a valid, whole-table btree index, by table: reading in their order is cheap.
pub const CATALOG_INDEXED_COLUMNS_SQL: &str = "
SELECT DISTINCT n.nspname, c.relname, a.attname
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
JOIN pg_catalog.pg_am am ON am.oid = ic.relam
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = i.indkey[0]
WHERE am.amname = 'btree' AND i.indisvalid AND i.indpred IS NULL
ORDER BY 1, 2, 3";

/// `pg_type.typcategory` for arrays.
pub const ARRAY_CATEGORY: &str = "A";

/// `pg_type.typname` values read natively.
pub const NATIVE_TYPE_NAMES: &[(&str, PgType)] = &[
    ("bool", PgType::Bool),
    ("int2", PgType::Int2),
    ("int4", PgType::Int4),
    ("int8", PgType::Int8),
    ("oid", PgType::Oid),
    ("float4", PgType::Float4),
    ("float8", PgType::Float8),
    ("numeric", PgType::Numeric),
    ("text", PgType::Text),
    ("varchar", PgType::Varchar),
    ("date", PgType::Date),
    ("time", PgType::Time),
    ("timestamp", PgType::Timestamp),
    ("timestamptz", PgType::TimestampTz),
    ("json", PgType::Json),
    ("jsonb", PgType::Json),
    ("uuid", PgType::Uuid),
    ("bytea", PgType::Bytea),
];

/// Session settings: UTC so timestamp text and filter literals ignore the server zone; standard strings so `\\` is literal.
pub const SESSION_OPTIONS: &str = "-c TimeZone=UTC -c standard_conforming_strings=on";
/// Batches read ahead of the consumer, so Postgres keeps sending while one is written.
pub const COPY_QUEUE_BATCHES: usize = 2;
/// Bytes of `COPY` data the socket reader groups before passing them to the decoder.
pub const COPY_READ_AHEAD_BYTES: usize = 1024 * 1024;
/// Groups read off the socket ahead of the decoder, so Postgres keeps sending while chunks are decoded.
pub const COPY_READ_AHEAD_GROUPS: usize = 8;
/// Opens the snapshot a split read shares across its connections.
pub const SPLIT_BEGIN: &str = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY";
/// Whether a table can be read in `ctid` ranges (a plain table, Postgres 14+ for TID range scans), and its pages now.
pub const SPLIT_PAGES_SQL: &str = "
SELECT current_setting('server_version_num')::int >= 140000 AND c.relkind = 'r',
       pg_relation_size(c.oid) / current_setting('block_size')::bigint
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2";
/// Pages (8 KiB each by default, so 128 MiB) below which a table is read in one pass: more connections would not pay off.
pub const SPLIT_READ_MIN_PAGES: i64 = 16_384;

/// Postgres's `SSLRequest`: length 8, then code 80877103.
pub const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];

/// Signature algorithm OIDs (DER contents) whose certificates bind with SHA-256: sha256WithRSAEncryption, sha1WithRSAEncryption, md5WithRSAEncryption, ecdsa-with-SHA256, ecdsa-with-SHA1.
pub const SHA256_SIGNATURES: &[&[u8]] = &[
    &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0b],
    &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x05],
    &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x04],
    &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02],
    &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x01],
];

/// sha384WithRSAEncryption and ecdsa-with-SHA384.
pub const SHA384_SIGNATURES: &[&[u8]] = &[
    &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0c],
    &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x03],
];

/// sha512WithRSAEncryption and ecdsa-with-SHA512.
pub const SHA512_SIGNATURES: &[&[u8]] = &[
    &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0d],
    &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x04],
];

/// A DER length byte with this bit set counts the length bytes that follow.
pub const DER_LONG_FORM: u8 = 0x80;

/// The most length bytes a DER element in a certificate takes here (lengths below 4 GiB).
pub const DER_LENGTH_BYTES: usize = 4;
