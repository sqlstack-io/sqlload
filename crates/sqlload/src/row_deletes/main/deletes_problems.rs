//! What is wrong with a table's `deletes`.

use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::Warehouse;
use crate::project::models::{Deletes, SourceKind, WriteStrategy};

/// Why a table of a `kind` source written with `strategy` into `warehouse` cannot take `deletes` (a `postgres_cdc` table's apply captured deletes, so none): it marks rows of a merge, by every key a database or warehouse source lists, on a target where its mark statements have run (DuckDB, MotherDuck, Postgres and Snowflake), and a deleted row would leave its child tables' rows (`unnests`).
pub fn deletes_problems(
    deletes: Option<Deletes>,
    (strategy, kind): (WriteStrategy, SourceKind),
    warehouse: Warehouse,
    unnests: bool,
) -> Vec<String> {
    if deletes.is_none() || kind == SourceKind::PostgresCdc {
        return Vec::new();
    }
    let mut problems: Vec<String> = Vec::new();
    if strategy != WriteStrategy::Merge {
        problems.push(format!(
            "deletes marks the rows whose unique_key the source no longer holds, which needs write_strategy = \"merge\"; {strategy} keeps no key to match. Fix: remove deletes, or merge on a unique_key"
        ));
    }
    if !matches!(
        kind,
        SourceKind::Postgres | SourceKind::Odbc | SourceKind::Snowflake | SourceKind::BigQuery
    ) {
        problems.push(format!(
            "deletes reads every key of the source table to find the ones it no longer holds, which only database and warehouse sources list; a {} source can't. Fix: remove deletes",
            kind.name()
        ));
    }
    if !sql_adapter(warehouse).marks_deleted_rows() {
        problems.push(format!(
            "deletes marks rows of the table in place with statements SQLLoad has run only on DuckDB, MotherDuck, Postgres and Snowflake so far, so it refuses them on {warehouse} rather than run them untested. Fix: remove deletes, or load into DuckDB, MotherDuck, Postgres or Snowflake"
        ));
    }
    if unnests {
        problems.push("deletes would leave the child tables' rows of each row it removes or marks. Fix: drop unnest, or remove deletes".to_owned());
    }
    problems
}
