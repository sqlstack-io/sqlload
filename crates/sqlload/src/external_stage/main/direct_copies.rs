//! `COPY` a files source's files straight from the external stage.

use crate::column_types::main::warehouse_type::warehouse_type;
use crate::column_types::models::Warehouse;
use crate::external_stage::_helpers::values::partition_text;
use crate::external_stage::constants::{
    COPY_OPTIONS, MAX_COPY_SQL_BYTES, MAX_FILES_PER_COPY, PARQUET_COPY_FORMAT,
    UNEXPECTED_FILE_MARKER,
};
use crate::external_stage::models::{CopyBatch, CopyColumn, CopyValue, DirectCopy};
use crate::files_source::models::DataFile;
use crate::write_sql::main::qualified_name::qualified_name;
use crate::write_sql::main::quoted_identifier::quoted_identifier;
use crate::write_sql::main::sql_literal::sql_literal;

/// `COPY` statements loading `request.files` into `request.target`, at most 1,000 files and about 900 KiB of SQL each: data columns read by name and cast, partition values, source file and version per file (matched on `METADATA$FILENAME`, failing on any other file), and literals; always `FORCE = TRUE` and `ON_ERROR = ABORT_STATEMENT`.
pub fn direct_copies(request: &DirectCopy<'_>) -> Result<Vec<CopyBatch>, String> {
    let folder = request
        .source
        .prefix
        .strip_prefix(&request.stage_url.prefix)
        .ok_or_else(|| {
            format!(
                "{} is not under the stage url {}",
                request.source, request.stage_url
            )
        })?;
    let mut batches: Vec<CopyBatch> = Vec::new();
    for chunk in request.files.chunks(MAX_FILES_PER_COPY) {
        let mut pending: Vec<&[DataFile]> = vec![chunk];
        while let Some(files) = pending.pop() {
            let sql = copy_sql(request, folder, files)?;
            match (sql.len() > MAX_COPY_SQL_BYTES, files.len()) {
                (true, count) if count > 1 => {
                    let (first, second) = files.split_at(count / 2);
                    pending.push(second);
                    pending.push(first);
                }
                _ => batches.push(CopyBatch {
                    sql,
                    keys: files
                        .iter()
                        .map(|data| data.file.relative_key.clone())
                        .collect(),
                }),
            }
        }
    }
    Ok(batches)
}

/// One `COPY` of `files`, which lie in `folder` below the stage URL.
fn copy_sql(request: &DirectCopy<'_>, folder: &str, files: &[DataFile]) -> Result<String, String> {
    let snowflake = Warehouse::Snowflake;
    let literal = |text: &str| sql_literal(text, snowflake);
    let matches: Vec<String> = files
        .iter()
        .map(|data| {
            let key = &data.file.relative_key;
            format!(
                "METADATA$FILENAME IN ({}, {})",
                literal(&format!("{folder}{key}")),
                literal(&format!("{}{key}", request.source))
            )
        })
        .collect();
    let mut names: Vec<String> = Vec::new();
    let mut values: Vec<String> = Vec::new();
    for column in request.columns {
        let sql_type = warehouse_type(&column.column_type, snowflake).sql;
        let per_file = |value: &dyn Fn(&DataFile) -> Result<String, String>,
                        otherwise: String|
         -> Result<String, String> {
            let arms: Vec<String> = files
                .iter()
                .zip(&matches)
                .map(|(data, matched)| Ok(format!("WHEN {matched} THEN {}", value(data)?)))
                .collect::<Result<_, String>>()?;
            Ok(format!("CASE {} ELSE {otherwise} END", arms.join(" ")))
        };
        let value = match &column.value {
            CopyValue::File { .. } => {
                format!("CAST(GET($1, {}) AS {sql_type})", literal(&column.name))
            }
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
        };
        names.push(quoted_identifier(&column.name, snowflake));
        values.push(value);
    }
    let keys: Vec<String> = files
        .iter()
        .map(|data| literal(&data.file.relative_key))
        .collect();
    Ok(format!(
        "COPY INTO {} ({}) FROM (SELECT {} FROM {}/{folder}) FILES = ({}) {PARQUET_COPY_FORMAT} {COPY_OPTIONS}",
        qualified_name(request.target, snowflake),
        names.join(", "),
        values.join(", "),
        request.stage,
        keys.join(", ")
    ))
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
        sql_literal(&text, Warehouse::Snowflake)
    ))
}
