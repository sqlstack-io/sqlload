//! The table a `replace` or `--reload` builds and swaps in.

use crate::column_types::main::warehouse_type::warehouse_type;
use crate::column_types::models::Warehouse;
use crate::schema_plan::models::{DesiredColumn, LiveColumn};
use crate::write_sql::_helpers::identifiers::{qualified, quote, quote_live, with_suffix};
use crate::write_sql::constants::STAGING_SUFFIX;
use crate::write_sql::main::stored_name::stored_name;
use crate::write_sql::models::TableName;

/// The build table for `table` under `staging_id`, and the statements creating it in the table's own shape, so batches land in their final form, what the destination added stays, and the swap copies nothing. `LIKE` on Snowflake, keeping grants; elsewhere from the table's `definition`. A `--reload` passes the `differing` live columns, as it rebuilds with the current columns: dropped and re-added while empty on Snowflake, created afresh elsewhere.
pub fn build_table(
    table: &TableName,
    (columns, differing, definition): (&[DesiredColumn], Option<&[LiveColumn]>, Option<&str>),
    staging_id: &str,
    warehouse: Warehouse,
) -> (TableName, Vec<String>) {
    let safe_id: String = staging_id
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '_')
        .collect();
    let build = with_suffix(table, &format!("{STAGING_SUFFIX}_{safe_id}"));
    let name = qualified(&build, warehouse);
    let sql_type = |column: &DesiredColumn| warehouse_type(&column.column_type, warehouse).sql;
    let statements = match warehouse {
        Warehouse::Snowflake => {
            let mut statements = vec![format!(
                "CREATE OR REPLACE TABLE {name} LIKE {} COPY GRANTS",
                qualified(table, warehouse)
            )];
            let stored = |column: &DesiredColumn| stored_name(&column.name, warehouse);
            let same = |known: &LiveColumn, column: &DesiredColumn| {
                warehouse_type(&known.column_type, warehouse).sql == sql_type(column)
            };
            let live = differing.unwrap_or_default();
            for known in live {
                let kept = columns.iter().find(|column| stored(column) == known.name);
                if kept.is_none_or(|column| !same(known, column)) {
                    statements.push(format!(
                        "ALTER TABLE {name} DROP COLUMN {}",
                        quote_live(&known.name)
                    ));
                }
            }
            for column in columns.iter().filter(|_| differing.is_some()) {
                let known = live.iter().find(|known| known.name == stored(column));
                if known.is_none_or(|known| !same(known, column)) {
                    statements.push(format!(
                        "ALTER TABLE {name} ADD COLUMN {} {}",
                        quote(&column.name, warehouse),
                        sql_type(column)
                    ));
                }
            }
            statements
        }
        _ if definition.is_some() => vec![format!(
            "CREATE OR REPLACE TABLE {name}{}",
            definition.unwrap_or_default()
        )],
        _ => {
            let definitions: Vec<String> = columns
                .iter()
                .map(|column| format!("{} {}", quote(&column.name, warehouse), sql_type(column)))
                .collect();
            vec![format!(
                "CREATE OR REPLACE TABLE {name} ({})",
                definitions.join(", ")
            )]
        }
    };
    (build, statements)
}
