//! ClickHouse reading a files table's Parquet files from S3 itself: whether it may, and the statement that does it.

use crate::adapters::clickhouse::_helpers::dialect::write::typed_literal;
use crate::adapters::clickhouse::constants::ORDERED_READ_SETTINGS;
use crate::adapters::clickhouse::models::ClickHouseSql;
use crate::adapters::contract::_helpers::direct_checks::{held_as, pattern, retyped};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::files_source::constants::SOURCE_ROW_COLUMN;
use crate::files_source::models::{FilesRead, SourceFormat};
use crate::loader::models::TableSpec;
use crate::schema_plan::constants::{LOAD_ID_COLUMN, LOADED_AT_COLUMN};
use crate::schema_plan::models::DesiredColumn;

/// Why ClickHouse cannot load the planned `read` of a files table exactly by reading its Parquet files itself, so SQLLoad reads them and sends Arrow instead; `None` when it can. Every column must load as the type the files hold it as, so ClickHouse's casts never differ from SQLLoad's; masks need SQLLoad's reader, and ClickHouse reads a Parquet time as a timestamp.
pub(crate) fn direct_problem(spec: &TableSpec, read: &FilesRead) -> Option<String> {
    let files = spec.files.as_ref()?;
    let problems = [
        (files.encoding.format != SourceFormat::Parquet)
            .then(|| "only Parquet files are read by ClickHouse itself".to_owned()),
        (!spec.masked.columns.is_empty()).then(|| "the table masks columns".to_owned()),
        files.table.columns.iter().find_map(held_as),
        read.columns
            .iter()
            .find_map(|column| retyped(files, column)),
        read.columns
            .iter()
            .find(|column| column.column_type == ColumnType::Time)
            .map(|column| {
                format!(
                    "{} is a time, which ClickHouse reads from Parquet as a timestamp",
                    column.name
                )
            }),
        read.files.iter().find_map(|data| pattern(files, data)),
    ];
    problems.into_iter().flatten().next()
}

/// One Parquet file ClickHouse reads: its URL, the columns its own footer holds, and the values SQLLoad gives every row of it (its name and version as the files source records them, and its partition values), by column (`None` for a partition folder the file lacks).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PulledFile {
    pub url: String,
    pub in_file: Vec<String>,
    pub constants: Vec<(String, Option<String>)>,
}

/// The `s3()` credentials a pull signs its requests with.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct PullCredentials {
    pub key_id: String,
    pub secret: String,
    pub token: Option<String>,
}

impl std::fmt::Debug for PullCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PullCredentials")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

/// `INSERT INTO into` every one of `columns` (each typed by `sql_type`) from `file`, read by ClickHouse's `s3()` with `credentials`. A column takes the file's constant when it has one of that name, the file's own column when its footer holds it, and NULL otherwise; `_sqlload_source_row` numbers the file's rows from 1 in stored order (read by one thread, in order); the load id and load time are the batch's. Binary columns go through `substring`, which keeps a fixed-size value's trailing zero bytes that a cast to `String` drops.
pub(crate) fn pull_insert(
    into: &str,
    (columns, sql_type): (&[DesiredColumn], &dyn Fn(&ColumnType) -> String),
    file: &PulledFile,
    credentials: &PullCredentials,
    (load_id, loaded_at): (&str, &str),
) -> String {
    let literal = |text: &str| ClickHouseSql.render_loader_value_literal(text);
    let identifier = |name: &str| ClickHouseSql.render_identifier(name);
    let cast = |value: String, column: &DesiredColumn| {
        format!("CAST({value} AS {})", sql_type(&column.column_type))
    };
    let values: Vec<String> = columns
        .iter()
        .map(|column| {
            let constant = file.constants.iter().find(|(name, _)| *name == column.name);
            match (column.name.as_str(), constant) {
                (_, Some((_, Some(value)))) => {
                    cast(typed_literal(value, &column.column_type), column)
                }
                (_, Some((_, None))) => cast("NULL".to_owned(), column),
                (LOAD_ID_COLUMN, None) => cast(typed_literal(load_id, &column.column_type), column),
                (LOADED_AT_COLUMN, None) => {
                    cast(typed_literal(loaded_at, &column.column_type), column)
                }
                (SOURCE_ROW_COLUMN, None) => cast("rowNumberInAllBlocks() + 1".to_owned(), column),
                (name, None) if file.in_file.iter().any(|held| held == name) => {
                    match column.column_type {
                        ColumnType::Binary => {
                            cast(format!("substring({}, 1)", identifier(name)), column)
                        }
                        _ => cast(identifier(name), column),
                    }
                }
                _ => cast("NULL".to_owned(), column),
            }
        })
        .collect();
    let targets: Vec<String> = columns
        .iter()
        .map(|column| identifier(&column.name))
        .collect();
    let mut signed = vec![
        literal(&file.url),
        literal(&credentials.key_id),
        literal(&credentials.secret),
    ];
    signed.extend(credentials.token.as_deref().map(literal));
    signed.push(literal("Parquet"));
    let numbered = columns
        .iter()
        .any(|column| column.name == SOURCE_ROW_COLUMN);
    let settings = match numbered {
        true => format!(" SETTINGS {ORDERED_READ_SETTINGS}"),
        false => String::new(),
    };
    format!(
        "INSERT INTO {into} ({}) SELECT {} FROM s3({}){settings}",
        targets.join(", "),
        values.join(", "),
        signed.join(", ")
    )
}

/// The HTTP URL ClickHouse reads `key` of `bucket` at: under `endpoint` by path when the S3 connection names one, otherwise on AWS by virtual host, in `region` when known. Each path segment is percent-encoded, which ClickHouse decodes before signing.
pub(crate) fn object_url(
    (bucket, key): (&str, &str),
    region: Option<&str>,
    endpoint: Option<&str>,
) -> Result<String, String> {
    let (base, segments): (String, Vec<&str>) = match (endpoint, region) {
        (Some(endpoint), _) => (
            endpoint.trim_end_matches('/').to_owned(),
            std::iter::once(bucket).chain(key.split('/')).collect(),
        ),
        (None, Some(region)) => (
            format!("https://{bucket}.s3.{region}.amazonaws.com"),
            key.split('/').collect(),
        ),
        (None, None) => (
            format!("https://{bucket}.s3.amazonaws.com"),
            key.split('/').collect(),
        ),
    };
    let mut url = reqwest::Url::parse(&base).map_err(|error| format!("{base}: {error}"))?;
    url.path_segments_mut()
        .map_err(|()| format!("{base} cannot hold a path"))?
        .pop_if_empty()
        .extend(segments);
    Ok(url.to_string())
}
