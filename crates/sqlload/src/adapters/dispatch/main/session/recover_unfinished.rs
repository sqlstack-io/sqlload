//! Before a load plans: remove what loads that stopped between their pieces committed, so the plan reads every table's position from before them.

use std::collections::BTreeSet;

use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::execute::execute;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::adapters::dispatch::models::Destination;
use crate::column_types::models::Warehouse;
use crate::loader::errors::LoaderError;
use crate::run_log::constants::{RUNS_TABLE, UNFINISHED_SEPARATOR, UNFINISHED_TABLE};
use crate::run_log::main::unfinished_recovery::unfinished_recovery;

/// On a DuckDB or MotherDuck destination, every load a recovery marker names (one that committed some of a table's rows in pieces and stopped before its last commit): its rows, its run records and its mark go, in one transaction per load. Other destinations commit whole and have none. Returns a line per load removed, for the plan's warnings.
pub async fn recover_unfinished(destination: &Destination) -> Result<Vec<String>, LoaderError> {
    #[cfg(feature = "duckdb")]
    if let Destination::DuckDb(_) = destination {
        return recover_duckdb(destination).await;
    }
    let _ = destination;
    Ok(Vec::new())
}

/// The recovery on DuckDB: the schemas holding markers, each marked load in them, and the tables there.
#[cfg(feature = "duckdb")]
async fn recover_duckdb(destination: &Destination) -> Result<Vec<String>, LoaderError> {
    let adapter = sql_adapter(Warehouse::DuckDb);
    let listed = format!(
        "SELECT tables.table_catalog || chr(31) || tables.table_schema || chr(31) || tables.table_name FROM information_schema.tables AS tables JOIN information_schema.tables AS markers ON markers.table_catalog = tables.table_catalog AND markers.table_schema = tables.table_schema WHERE markers.table_name = {}",
        adapter.render_loader_value_literal(UNFINISHED_TABLE)
    );
    let tables: BTreeSet<Vec<String>> = text_values(destination, listed)
        .await?
        .iter()
        .map(|line| {
            line.split(UNFINISHED_SEPARATOR)
                .map(str::to_owned)
                .collect()
        })
        .collect();
    let mut removed: Vec<String> = Vec::new();
    for found in tables
        .iter()
        .filter(|found| found.last().map(String::as_str) == Some(UNFINISHED_TABLE))
    {
        let [database, schema, _] = found.as_slice() else {
            continue;
        };
        let markers = TableName {
            database: Some(database.clone()),
            schema: schema.clone(),
            table: UNFINISHED_TABLE.to_owned(),
        };
        let query = format!(
            "SELECT DISTINCT table_name || chr(31) || load_id || chr(31) || load_id_column FROM {}",
            adapter.render_qualified_name(&markers)
        );
        for line in text_values(destination, query).await? {
            let fields: Vec<&str> = line.split(UNFINISHED_SEPARATOR).collect();
            let [table, load_id, column] = fields.as_slice() else {
                continue;
            };
            let has = |name: &str| {
                tables.contains(&vec![database.clone(), schema.clone(), name.to_owned()])
            };
            let name = TableName {
                table: (*table).to_owned(),
                ..markers.clone()
            };
            let exists = (has(table), has(RUNS_TABLE), Warehouse::DuckDb);
            execute(
                destination,
                unfinished_recovery(&name, (column, load_id), exists),
            )
            .await?;
            removed.push(format!(
                "{schema}.{table}: removed the rows of load {load_id}, which stopped before its last commit, so this load reads from before it"
            ));
        }
    }
    Ok(removed)
}
