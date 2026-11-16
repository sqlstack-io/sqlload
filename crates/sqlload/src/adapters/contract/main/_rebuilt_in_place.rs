//! Column changes for a warehouse that changes any column's type in place.

use crate::adapters::contract::_helpers::schema::alter_statements;
use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::schema_plan::models::{ColumnChange, LiveColumn};

/// The statements applying `changes` to `table`, whose current columns are `live`, with every rebuild one more `ALTER COLUMN ... TYPE`: for a warehouse, such as DuckDB, that rewrites a column itself and keeps the rest of the table.
pub(in crate::adapters) fn rebuilt_in_place<A: SqlAdapter + ?Sized>(
    adapter: &A,
    table: &TableName,
    changes: &[ColumnChange],
    live: &[LiveColumn],
) -> Vec<String> {
    let altered: Vec<ColumnChange> = changes
        .iter()
        .cloned()
        .map(|change| match change {
            ColumnChange::Rebuild {
                column,
                from,
                to,
                sql_type,
            } => ColumnChange::Widen {
                column,
                from,
                to,
                sql_type,
            },
            other => other,
        })
        .collect();
    alter_statements(adapter, table, &altered, live)
}
