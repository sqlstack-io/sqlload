/// The schema of the session's temporary tables, which holds a load's staging table.
pub const TEMPORARY_SCHEMA: &str = "pg_temp";

/// A table's columns in order, with the type as Postgres prints it and its precision, scale and length: tables and partitioned tables only, never views.
pub const DESCRIBE_TABLE_SQL: &str = "
SELECT a.attname::text,
       format_type(a.atttypid, a.atttypmod),
       information_schema._pg_numeric_precision(a.atttypid, a.atttypmod)::bigint,
       information_schema._pg_numeric_scale(a.atttypid, a.atttypmod)::bigint,
       information_schema._pg_char_max_length(a.atttypid, a.atttypmod)::bigint
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')
  AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum";

/// What `debug` shows of the session: the user, the database and the server version.
pub const SESSION_QUERY: &str = "SELECT current_user || ' on ' || current_database() || ', Postgres ' || current_setting('server_version')";

/// Bytes of binary `COPY` data sent at a time.
pub const COPY_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// Shown for `copy_to_internal_stage` on a Postgres target.
pub const NO_INTERNAL_STAGE_COPY: &str =
    "copy_to_internal_stage is for Snowflake targets; Postgres takes the rows by binary COPY";

/// Whether a table has a unique index `ON CONFLICT` can use for a key: valid, checked immediately, not partial, on plain columns, whose key columns are exactly `$3` (sorted by byte order).
pub const KEY_INDEX_SQL: &str = "
SELECT count(*)
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2 AND i.indisunique AND i.indisvalid AND i.indimmediate
  AND i.indpred IS NULL AND i.indexprs IS NULL
  AND ARRAY(
    SELECT a.attname::text FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = i.indrelid AND a.attnum = ANY ((i.indkey::int2[])[0:i.indnkeyatts - 1])
    ORDER BY a.attname COLLATE \"C\"
  ) = $3::text[]";

/// The foreign keys (of other tables, or the table itself) that reference a table: the referencing table, the constraint, what a delete does (`confdeltype`: `a` no action, `r` restrict, `c` cascade, `n` set null, `d` set default), and whether its check waits for the commit.
pub const REFERENCES_SQL: &str = "
SELECT format('%I.%I', rn.nspname, r.relname), con.conname::text, con.confdeltype::text, con.condeferred, con.conrelid = con.confrelid
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class r ON r.oid = con.conrelid
JOIN pg_catalog.pg_namespace rn ON rn.oid = r.relnamespace
JOIN pg_catalog.pg_class c ON c.oid = con.confrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE con.contype = 'f' AND con.conparentid = 0
  AND n.nspname = $1 AND c.relname = $2
ORDER BY 1, 2";

/// The dollar quote around the block that creates a missing schema.
pub const BLOCK_QUOTE: &str = "$sqlload$";

/// A `numeric` column without a precision, which holds any decimal exactly.
pub const UNBOUNDED_NUMERIC: &str = "numeric";
