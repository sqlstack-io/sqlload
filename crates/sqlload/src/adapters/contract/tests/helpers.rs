use crate::adapters::contract::models::TableName;
use crate::column_types::models::{ColumnType, LiveColumnType};
use crate::schema_plan::main::metadata_columns::metadata_columns;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

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
    crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter(warehouse)
        .render_staging_changes(&staging, (&old, &new), None)
}

/// 2026-11-05 10:00 UTC.
pub(crate) fn ten_oclock() -> Result<chrono::NaiveDateTime, String> {
    chrono::NaiveDateTime::parse_from_str("2026-11-05 10:00:00", "%Y-%m-%d %H:%M:%S")
        .map_err(|error| error.to_string())
}

/// The statements inserting `rows` rows of `id` and a text of `value_bytes` bytes (and a NULL).
pub(crate) fn inserted(rows: usize, value_bytes: usize) -> Vec<String> {
    let column = |name: &str, column_type: ColumnType| DesiredColumn {
        name: name.to_owned(),
        column_type,
        origin: ColumnOrigin::Source,
    };
    let columns = vec![
        column("id", ColumnType::Int),
        column("note", ColumnType::Json),
        column("gone", ColumnType::Text),
    ];
    let row = vec![Some("1".to_owned()), Some("x".repeat(value_bytes)), None];
    crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter(
        crate::column_types::models::Warehouse::Snowflake,
    )
    .render_insert_rows(&accounts(), &columns, &vec![row; rows])
}

/// A decimal type.
pub(crate) const fn decimal(precision: u8, scale: u8) -> ColumnType {
    ColumnType::Decimal { precision, scale }
}

/// A reported column type.
pub(crate) fn live(
    data_type: &str,
    precision: Option<i64>,
    scale: Option<i64>,
    char_length: Option<i64>,
) -> LiveColumnType {
    LiveColumnType {
        data_type: data_type.to_owned(),
        precision,
        scale,
        char_length,
    }
}

const WRITE_VERBS: [&str; 7] = [
    "INSERT OVERWRITE",
    "INSERT INTO",
    "DELETE FROM",
    "MERGE INTO",
    "ALTER TABLE",
    "DROP TABLE",
    "CREATE OR REPLACE TABLE",
];

/// Whether `statement` writes `target` (its qualified name, or its bare `name` as a rename may take it).
pub(crate) fn writes(statement: &str, (target, name): (&str, &str)) -> bool {
    let publishes = statement
        .strip_prefix(crate::adapters::clickhouse::constants::PUBLISH_PREFIX)
        .is_some_and(|step| step.contains(&format!("\"target\":\"{target}\"")));
    publishes
        || statement.ends_with(&format!("RENAME TO {name}"))
        || statement.ends_with(&format!("RENAME TO {target}"))
        || WRITE_VERBS.iter().any(|verb| {
            statement.starts_with(&format!("{verb} {target}"))
                || statement.starts_with(&format!("{verb} IF EXISTS {target}"))
        })
}

/// `text` (`YYYY-MM-DD HH:MM:SS[.f]`) as a bound timestamp.
pub(crate) fn timestamp_parameter(
    text: &str,
) -> Result<crate::adapters::contract::models::BoundParameter, String> {
    chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .map(crate::adapters::contract::models::BoundParameter::Timestamp)
        .map_err(|error| error.to_string())
}

/// `text` (`YYYY-MM-DD`) as a bound date.
pub(crate) fn date_parameter(
    text: &str,
) -> Result<crate::adapters::contract::models::BoundParameter, String> {
    chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .map(crate::adapters::contract::models::BoundParameter::Date)
        .map_err(|error| error.to_string())
}
