use crate::adapters::contract::models::TableName;
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::{ColumnChange, ColumnOrigin, DesiredColumn, LiveColumn};

/// `main.raw.orders`.
pub(crate) fn table() -> TableName {
    TableName {
        database: Some("main".to_owned()),
        schema: "raw".to_owned(),
        table: "orders".to_owned(),
    }
}

/// A live column.
pub(crate) fn live(name: &str, column_type: ColumnType) -> LiveColumn {
    LiveColumn {
        name: name.to_owned(),
        column_type,
        fraction_digits: None,
    }
}

/// A column added with its DDL type.
pub(crate) fn added(name: &str, column_type: ColumnType, sql_type: &str) -> ColumnChange {
    ColumnChange::Add {
        column: DesiredColumn {
            name: name.to_owned(),
            column_type,
            origin: ColumnOrigin::Source,
        },
        sql_type: sql_type.to_owned(),
    }
}

/// A listed load folder `/v/sqlload/<name>` with one batch `00001` holding one file per time, `part-<n>`.
pub(crate) fn listed_load(
    name: &str,
    times: &[Option<i64>],
) -> crate::adapters::databricks::models::ListedLoad {
    let folder = format!("/v/sqlload/{name}/");
    crate::adapters::databricks::models::ListedLoad {
        batches: vec![format!("{folder}00001/")],
        files: times
            .iter()
            .enumerate()
            .map(
                |(index, time)| crate::databricks_api::models::DirectoryEntry {
                    path: format!("{folder}00001/part-{index}"),
                    is_directory: false,
                    last_modified_ms: *time,
                },
            )
            .collect(),
        folder,
    }
}
