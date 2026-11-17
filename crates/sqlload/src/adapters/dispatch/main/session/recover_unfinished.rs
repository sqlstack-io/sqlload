//! Before a load runs: remove what loads that stopped between their pieces committed, for the tables it loads, so it reads each one's position from before them.

#[cfg(feature = "duckdb")]
use std::collections::{BTreeMap, BTreeSet};

use crate::adapters::contract::models::TableName;
#[cfg(feature = "duckdb")]
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
#[cfg(feature = "duckdb")]
use crate::adapters::dispatch::main::session::execute::execute;
#[cfg(feature = "duckdb")]
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::adapters::dispatch::models::Destination;
#[cfg(feature = "duckdb")]
use crate::column_types::models::Warehouse;
use crate::loader::errors::LoaderError;
#[cfg(feature = "duckdb")]
use crate::run_log::constants::{RUNS_TABLE, UNFINISHED_SEPARATOR, UNFINISHED_TABLE};
#[cfg(feature = "duckdb")]
use crate::run_log::main::unfinished_recovery::unfinished_recovery;

/// On a DuckDB or MotherDuck destination, every load a recovery marker names for one of `tables`, the tables this run loads there (one that committed some of a table's rows in pieces and stopped before its last commit): its rows, its run records and its mark go, in one transaction per load. Markers of other tables stay, since on MotherDuck another process may be loading them now. Other destinations commit whole and have none. Returns a line per load removed, for the plan's warnings.
pub async fn recover_unfinished(
    destination: &Destination,
    tables: &[&TableName],
) -> Result<Vec<String>, LoaderError> {
    #[cfg(feature = "duckdb")]
    if let Destination::DuckDb(_) = destination {
        return recover_duckdb(destination, tables).await;
    }
    let _ = (destination, tables);
    Ok(Vec::new())
}

/// The recovery on DuckDB: per schema of `tables`, the marked loads of its tables.
#[cfg(feature = "duckdb")]
async fn recover_duckdb(
    destination: &Destination,
    tables: &[&TableName],
) -> Result<Vec<String>, LoaderError> {
    let mut schemas: BTreeMap<(Option<String>, String), BTreeSet<String>> = BTreeMap::new();
    for table in tables {
        schemas
            .entry((table.database.clone(), table.schema.clone()))
            .or_default()
            .insert(table.table.clone());
    }
    let mut removed: Vec<String> = Vec::new();
    for (place, names) in schemas {
        removed.extend(recover_schema(destination, place, &names).await?);
    }
    Ok(removed)
}

/// The recovery in one schema: its marker table if it has one, and the marked loads of `names` in it.
#[cfg(feature = "duckdb")]
async fn recover_schema(
    destination: &Destination,
    (database, schema): (Option<String>, String),
    names: &BTreeSet<String>,
) -> Result<Vec<String>, LoaderError> {
    let adapter = sql_adapter(Warehouse::DuckDb);
    let text = |value: &str| adapter.render_loader_value_literal(value);
    let catalog = database
        .as_deref()
        .map_or_else(|| "current_database()".to_owned(), text);
    let listed: Vec<String> = [UNFINISHED_TABLE, RUNS_TABLE]
        .into_iter()
        .chain(names.iter().map(String::as_str))
        .map(text)
        .collect();
    let query = format!(
        "SELECT table_name FROM information_schema.tables WHERE table_catalog = {catalog} AND table_schema = {} AND table_name IN ({})",
        text(&schema),
        listed.join(", ")
    );
    let present: BTreeSet<String> = text_values(destination, query).await?.into_iter().collect();
    if !present.contains(UNFINISHED_TABLE) {
        return Ok(Vec::new());
    }
    let markers = TableName {
        database,
        schema: schema.clone(),
        table: UNFINISHED_TABLE.to_owned(),
    };
    let marked: Vec<String> = names.iter().map(|name| text(name)).collect();
    let query = format!(
        "SELECT DISTINCT table_name || chr(31) || load_id || chr(31) || load_id_column FROM {} WHERE table_name IN ({})",
        adapter.render_qualified_name(&markers),
        marked.join(", ")
    );
    let mut removed: Vec<String> = Vec::new();
    for line in text_values(destination, query).await? {
        let fields: Vec<&str> = line.split(UNFINISHED_SEPARATOR).collect();
        let [table, load_id, column] = fields.as_slice() else {
            continue;
        };
        let name = TableName {
            table: (*table).to_owned(),
            ..markers.clone()
        };
        let exists = (
            present.contains(*table),
            present.contains(RUNS_TABLE),
            Warehouse::DuckDb,
        );
        execute(
            destination,
            unfinished_recovery(&name, (column, load_id), exists),
        )
        .await?;
        removed.push(format!(
            "{schema}.{table}: removed the rows of load {load_id}, which stopped before its last commit, so this load reads from before it"
        ));
    }
    Ok(removed)
}
