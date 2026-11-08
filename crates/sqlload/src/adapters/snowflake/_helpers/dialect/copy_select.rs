//! The `SELECT` of a direct `COPY`: each column's value from the stage's files.

use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::_helpers::dialect::text_values::{
    csv_field, csv_file_format, csv_value, json_file_format, json_record_guard, json_value,
};
use crate::adapters::snowflake::_helpers::dialect::values::partition_text;
use crate::adapters::snowflake::constants::{
    COPY_OPTIONS, PARQUET_COPY_FORMAT, UNEXPECTED_FILE_MARKER,
};
use crate::adapters::snowflake::models::SnowflakeSql;
use crate::adapters::snowflake::models::{CopyColumn, CopyValue, DirectCopy};
use crate::column_types::models::ColumnType;
use crate::files_source::models::{DataFile, SourceFormat, StoredRead};

/// One `COPY` of `files`, which lie in `request.folder` below the stage; CSV files share one `header`, which places their columns.
pub(crate) fn copy_sql(
    request: &DirectCopy<'_>,
    header: Option<&[String]>,
    files: &[DataFile],
) -> Result<String, String> {
    let literal = |text: &str| SnowflakeSql.render_loader_value_literal(text);
    let matches: Vec<String> = files
        .iter()
        .map(|data| {
            format!(
                "METADATA$FILENAME = {}",
                literal(&format!(
                    "{}{}",
                    request.filename_prefix, data.file.relative_key
                ))
            )
        })
        .collect();
    let mut names: Vec<String> = Vec::new();
    let mut values: Vec<String> = Vec::new();
    let mut guarded = false;
    for column in request.columns {
        let value = column_value(request, (header, files, &matches), column)?;
        let value = match (&column.value, &request.encoding.format) {
            (CopyValue::File { .. }, SourceFormat::Json | SourceFormat::Ndjson) if !guarded => {
                guarded = true;
                json_record_guard(&value, &sql_type(column))
            }
            _ => value,
        };
        names.push(SnowflakeSql.render_identifier(&column.name));
        values.push(value);
    }
    let keys: Vec<String> = files
        .iter()
        .map(|data| literal(&data.file.relative_key))
        .collect();
    let file_format = match request.encoding.format {
        SourceFormat::Parquet => PARQUET_COPY_FORMAT.to_owned(),
        SourceFormat::Csv => format!(
            "FILE_FORMAT = ({})",
            csv_file_format(&request.encoding.csv, request.encoding.compression)
        ),
        _ => format!(
            "FILE_FORMAT = ({})",
            json_file_format(request.encoding.compression)
        ),
    };
    Ok(format!(
        "COPY INTO {} ({}) FROM (SELECT {} FROM {}/{}) FILES = ({}) {file_format} {COPY_OPTIONS}",
        SnowflakeSql.render_qualified_name(request.target),
        names.join(", "),
        values.join(", "),
        request.stage,
        request.folder,
        keys.join(", ")
    ))
}

fn sql_type(column: &CopyColumn) -> String {
    SnowflakeSql
        .render_loader_logical_type(&column.column_type)
        .sql
}

/// The value of `column` in a `COPY` of `files` (`matches` their `METADATA$FILENAME` conditions), CSV columns placed by `header`.
fn column_value(
    request: &DirectCopy<'_>,
    (header, files, matches): (Option<&[String]>, &[DataFile], &[String]),
    column: &CopyColumn,
) -> Result<String, String> {
    let literal = |text: &str| SnowflakeSql.render_loader_value_literal(text);
    let sql_type = sql_type(column);
    let per_file = |value: &dyn Fn(&DataFile) -> Result<String, String>,
                    otherwise: String|
     -> Result<String, String> {
        let arms: Vec<String> = files
            .iter()
            .zip(matches)
            .map(|(data, matched)| Ok(format!("WHEN {matched} THEN {}", value(data)?)))
            .collect::<Result<_, String>>()?;
        Ok(format!("CASE {} ELSE {otherwise} END", arms.join(" ")))
    };
    let inferred = request.inferred.contains(&column.name);
    Ok(match &column.value {
        CopyValue::File { .. } if request.encoding.record.is_some() => {
            record_value(request, header)
        }
        CopyValue::File { .. } if request.encoding.format == SourceFormat::Csv => {
            match header
                .unwrap_or_default()
                .iter()
                .position(|name| *name == column.name)
            {
                Some(position) => csv_value(
                    (
                        &column.name,
                        &csv_field(position + 1, &request.encoding.csv.null_values),
                    ),
                    (&column.column_type, &sql_type),
                    inferred,
                ),
                None => "NULL".to_owned(),
            }
        }
        CopyValue::File { .. } if request.encoding.format != SourceFormat::Parquet => json_value(
            (&column.name, &format!("GET($1, {})", literal(&column.name))),
            (&column.column_type, &sql_type),
            inferred,
        ),
        CopyValue::File { .. } if column.column_type == ColumnType::Json => format!(
            "PARSE_JSON(CAST(GET($1, {}) AS VARCHAR))",
            literal(&column.name)
        ),
        CopyValue::File { stored, short, .. } => file_value(
            &format!("GET($1, {})", literal(&column.name)),
            (&column.column_type, &sql_type),
            (*stored, *short),
        ),
        CopyValue::SourceRow => "METADATA$FILE_ROW_NUMBER".to_owned(),
        CopyValue::Literal(expression) => expression.clone(),
        CopyValue::SourceFile => per_file(
            &|data| {
                Ok(literal(&format!(
                    "{}{}",
                    request.source, data.file.relative_key
                )))
            },
            format!(
                "TO_VARCHAR(TO_NUMBER({} || METADATA$FILENAME))",
                literal(UNEXPECTED_FILE_MARKER)
            ),
        )?,
        CopyValue::SourceVersion => {
            per_file(&|data| Ok(literal(&data.file.version())), "NULL".to_owned())?
        }
        CopyValue::Partition => per_file(
            &|data| partition_value(data, column, &sql_type),
            "NULL".to_owned(),
        )?,
    })
}

/// The whole record of a `shape = "record"` read: a JSON record itself, or a CSV line as an array of its `header`'s fields as strings, a NULL field (see `csv_field`) as JSON `null`.
fn record_value(request: &DirectCopy<'_>, header: Option<&[String]>) -> String {
    match request.encoding.format {
        SourceFormat::Csv => {
            let fields: Vec<String> = (1..=header.unwrap_or_default().len())
                .map(|position| {
                    format!(
                        "COALESCE(TO_VARIANT({}), PARSE_JSON('null'))",
                        csv_field(position, &request.encoding.csv.null_values)
                    )
                })
                .collect();
            format!("ARRAY_CONSTRUCT({})", fields.join(", "))
        }
        _ => "$1".to_owned(),
    }
}

/// The file's value of partition column `column`, cast to its type; `NULL` when its path has none.
fn partition_value(data: &DataFile, column: &CopyColumn, sql_type: &str) -> Result<String, String> {
    let Some((_, raw)) = data
        .partitions
        .iter()
        .find(|(name, _)| *name == column.name)
    else {
        return Ok("NULL".to_owned());
    };
    let text = partition_text(raw, &column.column_type).map_err(|reason| {
        format!(
            "partition column {} of {}: {reason}",
            column.name, data.file.relative_key
        )
    })?;
    Ok(format!(
        "CAST({} AS {sql_type})",
        SnowflakeSql.render_loader_value_literal(&text)
    ))
}

/// The value of file column `get`, as `column_type` (`sql_type`): a bare integer converted from its unit, and a time or timestamp cut to microseconds toward zero where the live column is `short`, as SQLLoad's own reader does (a cast to fewer digits rounds down instead, which differs before 1970).
fn file_value(
    get: &str,
    (column_type, sql_type): (&ColumnType, &str),
    (stored, short): (StoredRead, bool),
) -> String {
    let read = match (stored, column_type) {
        (StoredRead::Epoch(scale), ColumnType::Date) => {
            format!("TO_DATE(TO_TIMESTAMP_NTZ({get}::NUMBER, {scale}))")
        }
        (StoredRead::Epoch(scale), ColumnType::TimestampTz) => {
            format!("TO_TIMESTAMP_TZ({get}::NUMBER, {scale})")
        }
        (StoredRead::Epoch(scale), _) => format!("TO_TIMESTAMP_NTZ({get}::NUMBER, {scale})"),
        _ => get.to_owned(),
    };
    let value = format!("CAST({read} AS {sql_type})");
    match (short, column_type) {
        (true, ColumnType::TimestampTz) => {
            format!("TO_TIMESTAMP_TZ(TRUNC(DATE_PART(EPOCH_NANOSECOND, {value}) / 1000), 6)")
        }
        (true, ColumnType::TimestampNtz) => {
            format!("TO_TIMESTAMP_NTZ(TRUNC(DATE_PART(EPOCH_NANOSECOND, {value}) / 1000), 6)")
        }
        (true, ColumnType::Time) => {
            format!("TIMEADD(NANOSECOND, -MOD(DATE_PART(NANOSECOND, {value}), 1000), {value})")
        }
        _ => value,
    }
}
