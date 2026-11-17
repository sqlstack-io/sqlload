//! Before a load plans: remove what loads that stopped between their pieces committed, so the plan reads every table's position from before them.

use std::collections::{BTreeMap, BTreeSet};

use crate::adapters::contract::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT};
use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::execute::execute;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::adapters::dispatch::models::Destination;
use crate::column_types::models::Warehouse;
use crate::loader::errors::LoaderError;
use crate::run_log::constants::{RUNS_TABLE, UNFINISHED_TABLE};
use crate::run_log::main::runs_table::runs_table;
use crate::run_log::main::unfinished_marker::unfinished_marker;

/// The separator of the fields the queries below return as one text.
const FIELD_SEPARATOR: char = '\u{1f}';

/// On each DuckDB and MotherDuck target, every load a recovery marker names (one that committed some of a table's rows in pieces and stopped before its last commit): its rows, its run records and its mark go, in one transaction per load. Returns a line per load removed, for the plan's warnings.
pub async fn recover_unfinished(
    destinations: &BTreeMap<String, Destination>,
) -> Result<Vec<String>, LoaderError> {
    let mut removed = Vec::new();
    for destination in destinations.values() {
        #[cfg(feature = "duckdb")]
        if let Destination::DuckDb(_) = destination {
            removed.extend(recover_target(destination).await?);
        }
        #[cfg(not(feature = "duckdb"))]
        let _ = destination;
    }
    Ok(removed)
}

/// Recover the loads the markers of one DuckDB target name.
async fn recover_target(destination: &Destination) -> Result<Vec<String>, LoaderError> {
    let adapter = sql_adapter(Warehouse::DuckDb);
    let text = |value: &str| adapter.render_loader_value_literal(value);
    let listed = format!(
        "SELECT tables.table_catalog || chr(31) || tables.table_schema || chr(31) || tables.table_name FROM information_schema.tables AS tables JOIN information_schema.tables AS markers ON markers.table_catalog = tables.table_catalog AND markers.table_schema = tables.table_schema WHERE markers.table_name = {}",
        text(UNFINISHED_TABLE)
    );
    let tables: BTreeSet<(String, String, String)> = text_values(destination, listed)
        .await?
        .iter()
        .filter_map(|line| {
            let mut fields = line.split(FIELD_SEPARATOR).map(str::to_owned);
            Some((fields.next()?, fields.next()?, fields.next()?))
        })
        .collect();
    let mut removed = Vec::new();
    for (database, schema, _) in tables
        .iter()
        .filter(|(_, _, table)| table == UNFINISHED_TABLE)
    {
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
            let fields: Vec<&str> = line.split(FIELD_SEPARATOR).collect();
            let [table, load_id, column] = fields.as_slice() else {
                continue;
            };
            let name = TableName {
                table: (*table).to_owned(),
                ..markers.clone()
            };
            let has = |table: &str| tables.contains(&(database.clone(), schema.clone(), table.to_owned()));
            let statements = recovery_statements(
                &name,
                (column, load_id),
                (has(table), has(RUNS_TABLE)),
            );
            execute(destination, statements).await?;
            removed.push(format!(
                "{schema}.{table}: removed the rows of load {load_id}, which stopped before its last commit, so this load reads from before it"
            ));
        }
    }
    Ok(removed)
}

/// One transaction removing the rows of load `load_id` from `table` (whose rows hold it in `column`), when it exists, its records from the run log beside it, when that exists, and its recovery mark.
pub(crate) fn recovery_statements(
    table: &TableName,
    (column, load_id): (&str, &str),
    (table_exists, runs_exist): (bool, bool),
) -> Vec<String> {
    let adapter = sql_adapter(Warehouse::DuckDb);
    let load = adapter.render_loader_value_literal(load_id);
    let rows = table_exists.then(|| {
        format!(
            "DELETE FROM {} WHERE {} = {load}",
            adapter.render_qualified_name(table),
            adapter.render_identifier(column)
        )
    });
    let records = runs_exist.then(|| {
        format!(
            "DELETE FROM {} WHERE {} = {load}",
            adapter.render_qualified_name(&runs_table(table)),
            adapter.render_identifier("run_id")
        )
    });
    let unmark = unfinished_marker(table, (column, load_id, ""), Warehouse::DuckDb).unmark;
    std::iter::once(BEGIN_STATEMENT.to_owned())
        .chain(rows)
        .chain(records)
        .chain([unmark, COMMIT_STATEMENT.to_owned()])
        .collect()
}
