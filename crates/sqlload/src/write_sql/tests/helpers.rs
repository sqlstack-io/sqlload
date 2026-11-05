use crate::column_types::models::ColumnType;
use crate::schema_plan::main::metadata_columns::metadata_columns;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::write_sql::models::TableName;

/// `raw.app__accounts`.
pub(crate) fn accounts() -> TableName {
    TableName {
        database: None,
        schema: "raw".to_owned(),
        table: "app__accounts".to_owned(),
    }
}

/// `id int`, `profile json`, `updated_at timestamp_tz`, then the metadata columns.
pub(crate) fn columns() -> Vec<DesiredColumn> {
    let source = |name: &str, column_type: ColumnType| DesiredColumn {
        name: name.to_owned(),
        column_type,
        origin: ColumnOrigin::Source,
    };
    let mut columns = vec![
        source("id", ColumnType::Int),
        source("profile", ColumnType::Json),
        source("updated_at", ColumnType::TimestampTz),
    ];
    columns.extend(metadata_columns());
    columns
}

/// The statements changing the staging table made for `id int, score int` to hold `new_types` (`id`, `score`, then `note`).
pub(crate) fn changed_staging(
    new_types: &[ColumnType],
    warehouse: crate::column_types::models::Warehouse,
) -> Vec<String> {
    let staging = TableName {
        table: "app__accounts__sqlload_staging_load1".to_owned(),
        ..accounts()
    };
    let column = |name: &str, column_type: ColumnType| DesiredColumn {
        name: name.to_owned(),
        column_type,
        origin: ColumnOrigin::Source,
    };
    let old = vec![
        column("id", ColumnType::Int),
        column("score", ColumnType::Int),
    ];
    let new: Vec<DesiredColumn> = ["id", "score", "note"]
        .iter()
        .zip(new_types)
        .map(|(name, column_type)| column(name, column_type.clone()))
        .collect();
    crate::write_sql::main::staging_changes::staging_changes(&staging, (&old, &new), warehouse)
}
