use chrono::{DateTime, Utc};

use crate::column_types::models::ColumnType;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

use crate::snowflake_destination::_helpers::stale_folders::stale_folders;
use crate::snowflake_destination::main::copy_staged::copy_staged;
use crate::snowflake_destination::models::CopyStaged;
use crate::write_sql::models::TableName;

/// The `COPY` statements for `files` staged parts of batch 1 of load `load-1`: how many, and the first.
pub(crate) fn copies(files: usize) -> (usize, String) {
    let table = TableName {
        database: Some("analytics".to_owned()),
        schema: "raw".to_owned(),
        table: "shop__orders".to_owned(),
    };
    let names: Vec<String> = (1..=files)
        .map(|part| format!("part-{part:05}.parquet"))
        .collect();
    let statements = copy_staged(&CopyStaged {
        table: &table,
        stage: "@~",
        folder: "sqlload/load-1/00001/",
        names: &names,
        selected: None,
    });
    (
        statements.len(),
        statements.first().cloned().unwrap_or_default(),
    )
}

/// The load folders older than a day before 2026-11-05 10:00 UTC.
pub(crate) fn stale(listed: &[(&str, &str)]) -> Vec<String> {
    let listed: Vec<(String, String)> = listed
        .iter()
        .map(|(name, modified)| ((*name).to_owned(), (*modified).to_owned()))
        .collect();
    let before = DateTime::parse_from_rfc3339("2026-11-04T10:00:00Z")
        .map(|time| time.with_timezone(&Utc))
        .unwrap_or_default();
    stale_folders(&listed, before)
}

/// The `COPY` of one staged file into a table holding JSON, naming the table's columns.
pub(crate) fn selecting_copy(columns: &[(&str, ColumnType)]) -> String {
    let table = TableName {
        database: None,
        schema: "raw".to_owned(),
        table: "orders".to_owned(),
    };
    let selected: Vec<DesiredColumn> = columns
        .iter()
        .map(|(name, column_type)| DesiredColumn {
            name: (*name).to_owned(),
            column_type: column_type.clone(),
            origin: ColumnOrigin::Source,
        })
        .collect();
    copy_staged(&CopyStaged {
        table: &table,
        stage: "@\"RAW\".%\"ORDERS\"",
        folder: "sqlload/l/00001/",
        names: &["part-00001.parquet".to_owned()],
        selected: Some(&selected),
    })
    .concat()
}
