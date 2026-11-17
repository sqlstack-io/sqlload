//! A load's recovery marker on the table it commits in pieces.

use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::Warehouse;
use crate::run_log::constants::{UNFINISHED_COLUMNS, UNFINISHED_TABLE};
use crate::run_log::models::UnfinishedMarker;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

/// The markers' table beside `table` (the run log's schema), and the statements marking the load `load_id` of `table`, whose rows hold it in `column`, as unfinished, and removing that mark.
pub fn unfinished_marker(
    table: &TableName,
    (column, load_id, started_at): (&str, &str, &str),
    warehouse: Warehouse,
) -> UnfinishedMarker {
    let adapter = sql_adapter(warehouse);
    let markers = TableName {
        database: table.database.clone(),
        schema: table.schema.clone(),
        table: UNFINISHED_TABLE.to_owned(),
    };
    let columns: Vec<DesiredColumn> = UNFINISHED_COLUMNS
        .iter()
        .map(|(name, column_type)| DesiredColumn {
            name: (*name).to_owned(),
            column_type: column_type.clone(),
            origin: ColumnOrigin::Metadata,
        })
        .collect();
    let text = |value: &str| adapter.render_loader_value_literal(value);
    let qualified = adapter.render_qualified_name(&markers);
    let names = format!(
        "{} = {} AND {} = {}",
        adapter.render_identifier("table_name"),
        text(&table.table),
        adapter.render_identifier("load_id"),
        text(load_id)
    );
    UnfinishedMarker {
        create: adapter.render_create_table(&markers, &columns),
        mark: format!(
            "INSERT INTO {qualified} VALUES ({}, {}, {}, CAST({} AS {}))",
            text(&table.table),
            text(load_id),
            text(column),
            text(started_at),
            adapter
                .render_loader_logical_type(&crate::column_types::models::ColumnType::TimestampTz)
                .sql
        ),
        unmark: format!("DELETE FROM {qualified} WHERE {names}"),
    }
}
