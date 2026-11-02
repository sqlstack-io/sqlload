use std::collections::BTreeMap;

use crate::config::models::ColumnConfig;

/// The comparable family of a Snowflake column type.
pub(crate) fn type_family(sql_type: &str) -> String {
    let upper = sql_type.trim().to_ascii_uppercase();
    let base = upper.split(['(', ' ']).next().unwrap_or_default();
    let family = match base {
        "VARCHAR" | "STRING" | "TEXT" | "CHAR" | "CHARACTER" | "NCHAR" | "NVARCHAR"
        | "NVARCHAR2" => "TEXT",
        "NUMBER" | "DECIMAL" | "NUMERIC" | "INT" | "INTEGER" | "BIGINT" | "SMALLINT"
        | "TINYINT" | "BYTEINT" => "NUMBER",
        "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "REAL" => "FLOAT",
        "TIMESTAMP" | "DATETIME" | "TIMESTAMP_NTZ" => "TIMESTAMP_NTZ",
        "VARBINARY" | "BINARY" => "BINARY",
        other => other,
    };
    family.to_owned()
}

/// Why an existing table cannot take the configured columns.
pub(crate) fn column_problems(
    columns: &[ColumnConfig],
    existing: &BTreeMap<String, String>,
) -> Vec<String> {
    columns
        .iter()
        .filter_map(
            |column| match existing.get(&column.name.to_ascii_uppercase()) {
                None => Some(format!("has no column {}", column.name)),
                Some(found) if type_family(found) != type_family(&column.sql_type) => {
                    Some(format!(
                        "column {} is {found}, configured as {}",
                        column.name, column.sql_type
                    ))
                }
                Some(_) => None,
            },
        )
        .collect()
}
