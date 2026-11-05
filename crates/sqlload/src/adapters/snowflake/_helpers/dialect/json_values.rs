//! JSON values in a direct `COPY`, and the types a stage read reports for them.

use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::constants::{
    MAX_JSON_INT, MIN_JSON_INT, MISFIT_MARKER, RFC3339_PATTERN,
};
use crate::adapters::snowflake::models::SnowflakeSql;
use crate::column_types::main::combined_type::combined_type;
use crate::column_types::models::ColumnType;
use crate::files_source::models::Compression;

/// The `FILE_FORMAT` a direct `COPY` reads JSON and NDJSON with: one record per object, whether the file holds objects one after another or one top-level array of them.
pub(crate) fn json_file_format(compression: Compression) -> String {
    format!(
        "TYPE = JSON STRIP_OUTER_ARRAY = TRUE COMPRESSION = {}",
        match compression {
            Compression::Auto => "AUTO",
            Compression::None => "NONE",
            Compression::Gzip => "GZIP",
            Compression::Zstd => "ZSTD",
        }
    )
}

/// The value of JSON key `name` (`get` reads it) as `column_type` (`sql_type`), as SQLLoad's own reader converts it: a string's own text and other values' JSON text for text, the value itself for json, a cast otherwise, timestamps with an offset in UTC. An `inferred` column fails the load on a value sampling would not have inferred it from, naming the file, row, key and value.
pub(crate) fn json_value(
    (name, get): (&str, &str),
    (column_type, sql_type): (&ColumnType, &str),
    inferred: bool,
) -> String {
    let value = match column_type {
        ColumnType::Json => get.to_owned(),
        ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Uuid => {
            format!("IFF(IS_NULL_VALUE({get}), NULL, TO_VARCHAR({get}))")
        }
        ColumnType::TimestampTz => {
            format!("CONVERT_TIMEZONE('UTC', CAST({get} AS {sql_type}))")
        }
        _ => format!("CAST({get} AS {sql_type})"),
    };
    let Some(fits) = inferred.then(|| fits(get, column_type)).flatten() else {
        return value;
    };
    let literal = |text: &str| SnowflakeSql.render_loader_value_literal(text);
    let failure = format!(
        "CAST(TO_VARCHAR(TO_NUMBER({} || METADATA$FILENAME || ' row ' || METADATA$FILE_ROW_NUMBER || {} || LEFT(TO_VARCHAR({get}), 80) || {})) AS {sql_type})",
        literal(MISFIT_MARKER),
        literal(&format!(": {name}: ")),
        literal(&format!(
            " does not fit {column_type}, the type inferred from sampled values; pin a type for {name} in columns to convert it"
        )),
    );
    format!("IFF({get} IS NULL OR IS_NULL_VALUE({get}) OR ({fits}), {value}, {failure})")
}

/// The condition a value of an inferred `column_type` column meets, as SQLLoad's inference decides it; `None` where any value fits (text).
fn fits(get: &str, column_type: &ColumnType) -> Option<String> {
    let int = format!(
        "(TYPEOF({get}) = 'INTEGER' AND CAST({get} AS NUMBER(38,0)) BETWEEN {MIN_JSON_INT} AND {MAX_JSON_INT})"
    );
    match column_type {
        ColumnType::Int => Some(int),
        ColumnType::Float => Some(format!("{int} OR TYPEOF({get}) IN ('DECIMAL', 'DOUBLE')")),
        ColumnType::Bool => Some(format!("TYPEOF({get}) = 'BOOLEAN'")),
        ColumnType::Json => Some(format!("TYPEOF({get}) IN ('OBJECT', 'ARRAY')")),
        ColumnType::TimestampTz => Some(format!(
            "TYPEOF({get}) = 'VARCHAR' AND REGEXP_LIKE(TO_VARCHAR({get}), '{RFC3339_PATTERN}')"
        )),
        _ => None,
    }
}

/// The type SQLLoad's inference gives a key whose values a stage read reports as `types` (`TYPEOF` names): `rfc3339` that every string is an RFC 3339 timestamp, `wide` that an integer exceeds 64 bits; `None` when every value was NULL.
pub(crate) fn key_type(types: &[String], (rfc3339, wide): (bool, bool)) -> Option<ColumnType> {
    types
        .iter()
        .filter_map(|name| match name.as_str() {
            "NULL_VALUE" => None,
            "INTEGER" if wide => Some(ColumnType::Text),
            "INTEGER" => Some(ColumnType::Int),
            "DECIMAL" | "DOUBLE" => Some(ColumnType::Float),
            "BOOLEAN" => Some(ColumnType::Bool),
            "OBJECT" | "ARRAY" => Some(ColumnType::Json),
            "VARCHAR" if rfc3339 => Some(ColumnType::TimestampTz),
            _ => Some(ColumnType::Text),
        })
        .reduce(combined_type)
}

/// The query listing every key of the JSON records in `files` (keys under the stage `from`, as `@"DB"."SCHEMA"."STAGE"/folder/`), read with the temporary file format `format`: each key, its values' `TYPEOF` names, whether every string is an RFC 3339 timestamp, and whether any integer exceeds 64 bits.
pub(crate) fn keys_query(from: &str, format: &str, pattern: &str) -> String {
    format!(
        "SELECT f.KEY, TO_JSON(ARRAY_AGG(DISTINCT TYPEOF(f.VALUE))), COALESCE(BOOLAND_AGG(CASE WHEN TYPEOF(f.VALUE) = 'VARCHAR' THEN REGEXP_LIKE(TO_VARCHAR(f.VALUE), '{RFC3339_PATTERN}') END), FALSE), COALESCE(BOOLOR_AGG(CASE WHEN TYPEOF(f.VALUE) = 'INTEGER' THEN CAST(f.VALUE AS NUMBER(38,0)) NOT BETWEEN {MIN_JSON_INT} AND {MAX_JSON_INT} END), FALSE) FROM {from} (FILE_FORMAT => {}, PATTERN => {}) s, LATERAL FLATTEN(INPUT => s.$1) f GROUP BY f.KEY",
        SnowflakeSql.render_loader_value_literal(format),
        SnowflakeSql.render_loader_value_literal(pattern),
    )
}
