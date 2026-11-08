//! The statement that has ClickHouse read a files table's Parquet file from S3 itself.

use crate::adapters::clickhouse::_helpers::dialect::write::typed_literal;
use crate::adapters::clickhouse::constants::{
    KEY_ID_PARAMETER, ORDERED_READ_SETTINGS, SECRET_PARAMETER, TOKEN_PARAMETER,
};
use crate::adapters::clickhouse::models::ClickHouseSql;
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::files_source::constants::SOURCE_ROW_COLUMN;
use crate::schema_plan::constants::{LOAD_ID_COLUMN, LOADED_AT_COLUMN};
use crate::schema_plan::models::DesiredColumn;

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

/// How a pull types each column it inserts: the staging table's type, or the build table's.
pub(crate) type SqlTypeOf<'a> = &'a dyn Fn(&ColumnType) -> String;

/// The query parameters a pull's statement names its credentials by, with their values: sent beside the SQL, never in it, so no statement text, log or error echo holds them.
pub(crate) fn credential_parameters(
    credentials: Option<&PullCredentials>,
) -> Vec<(String, String)> {
    let Some(credentials) = credentials else {
        return Vec::new();
    };
    let mut parameters = vec![
        (
            format!("param_{KEY_ID_PARAMETER}"),
            credentials.key_id.clone(),
        ),
        (
            format!("param_{SECRET_PARAMETER}"),
            credentials.secret.clone(),
        ),
    ];
    parameters.extend(
        credentials
            .token
            .clone()
            .map(|token| (format!("param_{TOKEN_PARAMETER}"), token)),
    );
    parameters
}

/// `INSERT INTO into` every one of `columns` (each typed by `sql_type`) from `file`, read by ClickHouse's `s3()` signed with `credentials` (named as [query parameters](credential_parameters)), or unsigned (`NOSIGN`) when there are none. A column takes the file's constant when it has one of that name, the file's own column when its footer holds it, and NULL otherwise; `_sqlload_source_row` numbers the file's rows from 1 in stored order (read by one thread, in order); the load id and load time are the batch's. Binary columns go through `substring`, which keeps a fixed-size value's trailing zero bytes that a cast to `String` drops.
pub(crate) fn pull_insert(
    into: &str,
    (columns, sql_type): (&[DesiredColumn], SqlTypeOf<'_>),
    (file, credentials): (&PulledFile, Option<&PullCredentials>),
    (load_id, loaded_at): (&str, &str),
) -> String {
    let literal = |text: &str| ClickHouseSql.render_loader_value_literal(text);
    let identifier = |name: &str| ClickHouseSql.render_identifier(name);
    let cast = |value: String, column: &DesiredColumn| {
        format!("CAST({value} AS {})", sql_type(&column.column_type))
    };
    let mut values: Vec<String> = Vec::with_capacity(columns.len());
    for column in columns {
        let constant = file.constants.iter().find(|(name, _)| *name == column.name);
        let held = file.in_file.contains(&column.name);
        let value = match (column.name.as_str(), constant, held) {
            (_, Some((_, Some(value))), _) => typed_literal(value, &column.column_type),
            (_, Some((_, None)), _) => "NULL".to_owned(),
            (LOAD_ID_COLUMN, None, _) => typed_literal(load_id, &column.column_type),
            (LOADED_AT_COLUMN, None, _) => typed_literal(loaded_at, &column.column_type),
            (SOURCE_ROW_COLUMN, None, _) => "rowNumberInAllBlocks() + 1".to_owned(),
            (name, None, true) => match column.column_type {
                ColumnType::Binary => format!("substring({}, 1)", identifier(name)),
                _ => identifier(name),
            },
            (_, None, false) => "NULL".to_owned(),
        };
        values.push(cast(value, column));
    }
    let targets: Vec<String> = columns
        .iter()
        .map(|column| identifier(&column.name))
        .collect();
    let parameter = |name: &str| format!("{{{name}:String}}");
    let mut signed = vec![literal(&file.url)];
    match credentials {
        Some(credentials) => {
            signed.extend([parameter(KEY_ID_PARAMETER), parameter(SECRET_PARAMETER)]);
            signed.extend(
                credentials
                    .token
                    .as_ref()
                    .map(|_| parameter(TOKEN_PARAMETER)),
            );
        }
        None => signed.push("NOSIGN".to_owned()),
    }
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

/// The HTTP URL ClickHouse reads `key` of `bucket` at: under `endpoint` by path when there is one (or with the bucket already in it, `in_endpoint`, for virtual-hosted requests to a custom endpoint), otherwise on AWS, in `region` when known, by virtual host unless the bucket's name holds a dot, which AWS's certificate does not cover. Each path segment is percent-encoded, which ClickHouse decodes before signing.
pub(crate) fn object_url(
    (bucket, key): (&str, &str),
    region: Option<&str>,
    (endpoint, in_endpoint): (Option<&str>, bool),
) -> Result<String, String> {
    let aws = match region {
        Some(region) => format!("s3.{region}.amazonaws.com"),
        None => "s3.amazonaws.com".to_owned(),
    };
    let (base, by_path): (String, bool) = match endpoint {
        Some(endpoint) => (endpoint.trim_end_matches('/').to_owned(), !in_endpoint),
        None if bucket.contains('.') => (format!("https://{aws}"), true),
        None => (format!("https://{bucket}.{aws}"), false),
    };
    let bucket_segment = by_path.then_some(bucket);
    let mut url = reqwest::Url::parse(&base).map_err(|error| format!("{base}: {error}"))?;
    url.path_segments_mut()
        .map_err(|()| format!("{base} cannot hold a path"))?
        .pop_if_empty()
        .extend(bucket_segment.into_iter().chain(key.split('/')));
    Ok(url.to_string())
}
