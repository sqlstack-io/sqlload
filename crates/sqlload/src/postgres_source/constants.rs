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
       c.reltuples::bigint, pg_total_relation_size(c.oid)
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
