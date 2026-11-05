//! JSON and CSV values in a direct `COPY`, and the types a stage read reports for JSON keys.

use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::constants::{
    MAX_JSON_INT, MIN_JSON_INT, MISFIT_MARKER, RFC3339_PATTERN,
};
use crate::adapters::snowflake::models::SnowflakeSql;
use crate::column_types::main::combined_type::combined_type;
use crate::column_types::models::ColumnType;
use crate::files_source::models::{Compression, CsvOptions};

/// The `FILE_FORMAT` a direct `COPY` reads JSON and NDJSON with: one record per object, whether the file holds objects one after another or one top-level array of them; a key given twice keeps its last value, as SQLLoad's reader does.
pub(crate) fn json_file_format(compression: Compression) -> String {
    format!(
        "TYPE = JSON STRIP_OUTER_ARRAY = TRUE ALLOW_DUPLICATE = TRUE COMPRESSION = {}",
        compression_name(compression)
    )
}

/// The value of JSON key `name` (`get` reads it) as `column_type` (`sql_type`), as SQLLoad's own reader converts it: a string's own text and other values' JSON text for text (Snowflake's JSON text: keys sorted, numbers in its own form), the value itself for json (a JSON `null` is NULL), binary, times and timestamps from its text (see `text_cast`), a cast otherwise. An `inferred` column fails the load on a value sampling would not have inferred it from, naming the file, row, key and value.
pub(crate) fn json_value(
    (name, get): (&str, &str),
    (column_type, sql_type): (&ColumnType, &str),
    inferred: bool,
) -> String {
    let value = match column_type {
        ColumnType::Json => format!("IFF(IS_NULL_VALUE({get}), NULL, {get})"),
        ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Uuid => {
            format!("IFF(IS_NULL_VALUE({get}), NULL, TO_VARCHAR({get}))")
        }
        ColumnType::Binary
        | ColumnType::TimestampTz
        | ColumnType::TimestampNtz
        | ColumnType::Time => text_cast(&format!("TO_VARCHAR({get})"), (column_type, sql_type)),
        _ => format!("CAST({get} AS {sql_type})"),
    };
    match inferred.then(|| fits(get, column_type)).flatten() {
        Some(fits) => format!(
            "IFF({get} IS NULL OR IS_NULL_VALUE({get}) OR ({fits}), {value}, {})",
            misfit((name, get), (column_type, sql_type), true)
        ),
        None => value,
    }
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
            "TYPEOF({get}) = 'VARCHAR' AND REGEXP_SUBSTR(TO_VARCHAR({get}), '^{RFC3339_PATTERN}$') IS NOT NULL"
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
        "SELECT f.KEY, TO_JSON(ARRAY_AGG(DISTINCT TYPEOF(f.VALUE))), COALESCE(BOOLAND_AGG(CASE WHEN TYPEOF(f.VALUE) = 'VARCHAR' THEN REGEXP_LIKE(TO_VARCHAR(f.VALUE), '{RFC3339_PATTERN}') END), FALSE), COALESCE(BOOLOR_AGG(CASE WHEN TYPEOF(f.VALUE) = 'INTEGER' THEN CAST(f.VALUE AS NUMBER(38,0)) NOT BETWEEN {MIN_JSON_INT} AND {MAX_JSON_INT} END), FALSE) FROM {from} (FILE_FORMAT => {}, PATTERN => {}) s, LATERAL FLATTEN(INPUT => s.$1) f WHERE IS_OBJECT(s.$1) GROUP BY f.KEY",
        SnowflakeSql.render_loader_value_literal(format),
        SnowflakeSql.render_loader_value_literal(pattern),
    )
}

/// The `FILE_FORMAT` a direct `COPY` reads CSV with, as SQLLoad's reader does: `options`' delimiter and quote (a quote inside a quoted field written twice, backslashes plain text), the header skipped, every field as text (`csv_field` applies the NULL values, since Snowflake would not turn a quoted empty field into NULL; live, 2026-11-05).
pub(crate) fn csv_file_format(options: &CsvOptions, compression: Compression) -> String {
    let literal = |text: &str| SnowflakeSql.render_loader_value_literal(text);
    format!(
        "TYPE = CSV FIELD_DELIMITER = {} FIELD_OPTIONALLY_ENCLOSED_BY = {} ESCAPE = NONE ESCAPE_UNENCLOSED_FIELD = NONE SKIP_HEADER = {} NULL_IF = () EMPTY_FIELD_AS_NULL = FALSE COMPRESSION = {}",
        literal(&options.delimiter.to_string()),
        literal(&options.quote.to_string()),
        u8::from(options.header),
        compression_name(compression)
    )
}

/// The value of CSV field `field` (`$n`), named `name`, as `column_type` (`sql_type`), as SQLLoad's own reader converts its text; an `inferred` column fails the load on a value sampling would not have inferred it from.
pub(crate) fn csv_value(
    (name, field): (&str, &str),
    (column_type, sql_type): (&ColumnType, &str),
    inferred: bool,
) -> String {
    let value = match column_type {
        ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Uuid => field.to_owned(),
        ColumnType::Json => format!("PARSE_JSON({field})"),
        _ => text_cast(field, (column_type, sql_type)),
    };
    let int = format!(
        "(REGEXP_SUBSTR({field}, '^-?(0|[1-9][0-9]*)$') IS NOT NULL AND TRY_TO_NUMBER({field}) BETWEEN {MIN_JSON_INT} AND {MAX_JSON_INT})"
    );
    let fits = match column_type {
        ColumnType::Int => int,
        ColumnType::Float => format!(
            "{int} OR (REGEXP_SUBSTR({field}, '^-?(0|[1-9][0-9]*)([.][0-9]+([eE][+-]?[0-9]+)?|[eE][+-]?[0-9]+)$') IS NOT NULL AND TRY_TO_DOUBLE({field}) BETWEEN -1.7976931348623157e308 AND 1.7976931348623157e308)"
        ),
        ColumnType::Bool => {
            format!("REGEXP_SUBSTR({field}, '^(true|false)$', 1, 1, 'i') IS NOT NULL")
        }
        ColumnType::TimestampTz => {
            format!("REGEXP_SUBSTR({field}, '^{RFC3339_PATTERN}$') IS NOT NULL")
        }
        _ => return value,
    };
    match inferred {
        true => format!(
            "IFF({field} IS NULL OR ({fits}), {value}, {})",
            misfit((name, field), (column_type, sql_type), false)
        ),
        false => value,
    }
}

/// Text `text` as `column_type` (`sql_type`), as SQLLoad's reader parses it: binary as the text's UTF-8 bytes, times and timestamps cut to microseconds (dropping further digits, as arrow's parser does), timestamps with an offset in UTC.
fn text_cast(text: &str, (column_type, sql_type): (&ColumnType, &str)) -> String {
    match column_type {
        ColumnType::Binary => format!("TO_BINARY({text}, 'UTF-8')"),
        ColumnType::TimestampTz => {
            format!("CONVERT_TIMEZONE('UTC', CAST({text} AS TIMESTAMP_TZ(6)))")
        }
        ColumnType::TimestampNtz => format!("CAST({text} AS TIMESTAMP_NTZ(6))"),
        ColumnType::Time => format!("CAST({text} AS TIME(6))"),
        _ => format!("CAST({text} AS {sql_type})"),
    }
}

/// CSV field `position` (from 1) of a direct `COPY`, NULL where its text, unquoted, is one of `null_values`, as SQLLoad's reader decides (the file format itself turns nothing into NULL).
pub(crate) fn csv_field(position: usize, null_values: &[String]) -> String {
    null_values
        .iter()
        .fold(format!("${position}"), |field, null| {
            format!(
                "NULLIF({field}, {})",
                SnowflakeSql.render_loader_value_literal(null)
            )
        })
}

/// `value`, a direct `COPY`'s value of a JSON key typed `sql_type`, failing the load on a record that is not a JSON object, as SQLLoad's reader does.
pub(crate) fn json_record_guard(value: &str, sql_type: &str) -> String {
    format!(
        "IFF(IS_OBJECT($1), {value}, CAST(TO_VARCHAR(TO_NUMBER({} || METADATA$FILENAME || ' row ' || METADATA$FILE_ROW_NUMBER || {})) AS {sql_type}))",
        SnowflakeSql.render_loader_value_literal(MISFIT_MARKER),
        SnowflakeSql.render_loader_value_literal(": the record is not a JSON object"),
    )
}

/// An expression failing the load for a value of `name` (`get`) that does not fit `column_type`, naming the column and value, typed as `sql_type`; with `located`, the file and row too (JSON). A CSV error names its file and line itself, and reports a metadata column's placeholder instead of a message that reads one (live, 2026-11-05).
fn misfit(
    (name, get): (&str, &str),
    (column_type, sql_type): (&ColumnType, &str),
    located: bool,
) -> String {
    let literal = |text: &str| SnowflakeSql.render_loader_value_literal(text);
    let location = match located {
        true => "METADATA$FILENAME || ' row ' || METADATA$FILE_ROW_NUMBER || ",
        false => "",
    };
    format!(
        "CAST(TO_VARCHAR(TO_NUMBER({} || {location}{} || LEFT(TO_VARCHAR({get}), 80) || {})) AS {sql_type})",
        literal(MISFIT_MARKER),
        literal(&match located {
            true => format!(": {name}: "),
            false => format!("{name}: "),
        }),
        literal(&format!(
            " does not fit {column_type}, the type inferred from sampled values; pin a type for {name} in columns to convert it"
        )),
    )
}

fn compression_name(compression: Compression) -> &'static str {
    match compression {
        Compression::Auto => "AUTO",
        Compression::None => "NONE",
        Compression::Gzip => "GZIP",
        Compression::Zstd => "ZSTD",
    }
}
