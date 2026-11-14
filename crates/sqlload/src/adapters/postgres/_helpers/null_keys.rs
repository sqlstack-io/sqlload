//! What stops a Postgres `merge` from matching a NULL in its key with a NULL.

use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::postgres::constants::NULLS_NOT_DISTINCT_VERSION;
use crate::adapters::postgres::models::{KeyIndexes, PostgresSql};

/// Why a merge into `table` whose key columns `nullable` may hold NULL (in the source, and not declared NOT NULL by an existing table) cannot run on a server of `version` (`server_version_num`, and as printed): `ON CONFLICT` matches a NULL key only through a unique index `NULLS NOT DISTINCT`, which Postgres has from 15; `None` from 15 on, or when no key column may be NULL.
pub(crate) fn version_problem(
    table: &TableName,
    nullable: &[&str],
    (number, version): (i32, &str),
) -> Option<String> {
    (!nullable.is_empty() && number < NULLS_NOT_DISTINCT_VERSION).then(|| {
        format!(
            "{}.{}: merge matches a NULL in a key column with a NULL (null_keys = \"match\"), and {} can be NULL; INSERT ... ON CONFLICT does that only through a unique index created NULLS NOT DISTINCT, which Postgres has from 15, and this server is Postgres {version}. Fix: set null_keys = \"fail\" to refuse a row with a NULL key, make {} NOT NULL in the source or the target table, or upgrade the target to Postgres 15 or later",
            table.schema,
            table.table,
            nullable.join(", "),
            nullable.join(", ")
        )
    })
}

/// Why `INSERT ... ON CONFLICT` on `unique_key` cannot run against `table`, from its unique `indexes` on exactly those columns. None at all is refused; with key columns `nullable` that may hold NULL (in the source, and not declared NOT NULL by the table), so is one treating NULLs as distinct, as it would let a row with a NULL key in again on every load. The fix creates the index without blocking writes (`CONCURRENTLY`), and drops a constraint an index backs with `ALTER TABLE`, which `DROP INDEX` cannot.
pub(crate) fn index_problem(
    table: &TableName,
    (unique_key, nullable): (&[String], &[&str]),
    indexes: &KeyIndexes,
) -> Option<String> {
    let qualified = PostgresSql.render_qualified_name(table);
    let columns: Vec<String> = unique_key
        .iter()
        .map(|column| PostgresSql.render_identifier(column))
        .collect();
    let nulls = match nullable.is_empty() {
        true => "",
        false => " NULLS NOT DISTINCT",
    };
    let example = format!(
        "CREATE UNIQUE INDEX CONCURRENTLY ON {qualified} ({}){nulls}",
        columns.join(", ")
    );
    match (indexes.count, indexes.nulls_equal) {
        (0, _) => Some(format!(
            "merge upserts with INSERT ... ON CONFLICT ({}), which needs a valid, non-deferrable unique index or constraint on exactly those columns, and {}.{} has none; add one, for example {example}",
            unique_key.join(", "),
            table.schema,
            table.table
        )),
        (_, 0) if !nullable.is_empty() => Some(format!(
            "{}.{}: merge matches a NULL in a key column with a NULL (null_keys = \"match\"), and {} can be NULL, but its unique index on ({}), {}, treats NULLs as distinct, so INSERT ... ON CONFLICT would insert a row with a NULL key again on every load. Fix: create one that treats them as equal, {example}, then {} (creating it fails while the table holds a key more than once, so remove those copies first); make {} NOT NULL in the table; or set null_keys = \"fail\" to refuse a row with a NULL key",
            table.schema,
            table.table,
            nullable.join(", "),
            unique_key.join(", "),
            indexes.others,
            indexes.drops,
            nullable.join(", ")
        )),
        _ => None,
    }
}
