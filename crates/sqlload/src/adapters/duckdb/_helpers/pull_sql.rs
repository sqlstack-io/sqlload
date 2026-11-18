//! The statement that has DuckDB load Parquet files itself.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Instant;

use crate::adapters::contract::constants::{UNLOADED_BATCH_ROWS, UNLOADED_FILE_ROWS};
use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::duckdb::_helpers::motherduck_connection::environment_variable;
use crate::adapters::duckdb::constants::{CHAIN_SECRET_REFRESH, UTC_OFFSET};
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::{DuckDbSql, SetSecret};
use crate::column_types::main::is_variant::is_variant;
use crate::column_types::models::ColumnType;
use crate::file_store::main::s3_keys::s3_keys;
use crate::files_source::constants::SOURCE_ROW_COLUMN;
use crate::load_naming::main::metadata_role::metadata_role;
use crate::load_naming::models::{Compat, MetadataRole};
use crate::postgres_source::models::ReadColumn;
use crate::schema_plan::constants::MERGE_ROW_COLUMN;
use crate::schema_plan::models::DesiredColumn;
use crate::stage_api::models::AwsCredentials;
use crate::store_paths::models::{HmacKey, StoreKind};

/// One file DuckDB reads: where it reads it, and the values SQLLoad gives every row of it (its name and version as the files source records them, and its partition values), by column (`None` for a partition folder the file lacks).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PulledFile {
    pub url: String,
    pub constants: Vec<(String, Option<String>)>,
}

/// `INSERT INTO into` every one of `columns` from the Parquet `files`, matched by name so a file that lacks a column gives NULL. A column comes from the file's constants when it has one of that name (a list of every file's value, indexed by the row's `file_index`: no join, so DuckDB streams the rows into the table instead of holding them), otherwise from the files when at least one of them holds it (`in_files`, each column with its name in the files, which `naming`, `rename` or numbering may have changed: `f."Order ID"` loads into `order_id`), and is cast to its type; `_sqlload_source_row` counts each file's rows from 1; the load id and load time are the batch's; any other column is NULL. Every file must carry the same constant columns. Into MotherDuck (`remote`) the files are read on its servers.
pub(crate) fn pull_insert(
    into: &TableName,
    (columns, in_files): (&[DesiredColumn], &[(String, String)]),
    (files, remote): (&[PulledFile], bool),
    (compat, load_id, loaded_at): (Compat, &str, &str),
) -> String {
    let literal = |text: &str| DuckDbSql.render_loader_value_literal(text);
    let identifier = |name: &str| DuckDbSql.render_identifier(name);
    let cast = |value: String, column: &DesiredColumn| {
        let value = match is_variant(&column.column_type) {
            true => format!("CAST({value} AS JSON)"),
            false => value,
        };
        format!(
            "CAST({value} AS {})",
            DuckDbSql
                .render_loader_logical_type(&column.column_type)
                .sql
        )
    };
    let values: Vec<String> = columns
        .iter()
        .map(
            |column| match (metadata_role(compat, &column.name), column.name.as_str()) {
                (Some(MetadataRole::LoadId), _) => cast(literal(load_id), column),
                (Some(MetadataRole::LoadedAt), _) => cast(literal(loaded_at), column),
                (None, SOURCE_ROW_COLUMN) => cast("f.file_row_number + 1".to_owned(), column),
                (_, name)
                    if files
                        .first()
                        .is_some_and(|file| constant(file, name).is_some()) =>
                {
                    cast(
                        format!(
                            "[{}][CAST(f.file_index AS BIGINT) + 1]",
                            constant_list(files, name)
                        ),
                        column,
                    )
                }
                (_, name) if let Some(file) = in_file(in_files, name) => {
                    cast(format!("f.{}", identifier(file)), column)
                }
                _ => "NULL".to_owned(),
            },
        )
        .collect();
    let urls: Vec<String> = files.iter().map(|file| literal(&file.url)).collect();
    let targets: Vec<String> = columns
        .iter()
        .map(|column| identifier(&column.name))
        .collect();
    format!(
        "INSERT INTO {} ({}) SELECT {} FROM read_parquet([{}], union_by_name = true, file_row_number = true, hive_partitioning = false{}) AS f",
        DuckDbSql.render_qualified_name(into),
        targets.join(", "),
        values.join(", "),
        urls.join(", "),
        on_servers(remote)
    )
}

/// The name in the files of the column the table holds as `name`, when the files hold it.
fn in_file<'a>(in_files: &'a [(String, String)], name: &str) -> Option<&'a str> {
    in_files
        .iter()
        .find(|(held, _)| held == name)
        .map(|(_, file)| file.as_str())
}

/// `file`'s constant `name`: `Some(None)` when its folder lacks it, `None` when it has no such constant.
fn constant<'a>(file: &'a PulledFile, name: &str) -> Option<Option<&'a str>> {
    file.constants
        .iter()
        .find(|(constant, _)| constant == name)
        .map(|(_, value)| value.as_deref())
}

/// Every file's value of constant `name`, in file order, as literals (NULL when absent).
fn constant_list(files: &[PulledFile], name: &str) -> String {
    files
        .iter()
        .map(|file| {
            constant(file, name).flatten().map_or_else(
                || "NULL".to_owned(),
                |value| DuckDbSql.render_loader_value_literal(value),
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// What an unloaded batch's insert fills: the columns it writes, those the files hold as they are read, and the table's `constant_columns` (name, type and value as text).
#[derive(Debug, Clone, Copy)]
pub(crate) struct UnloadedColumns<'a> {
    pub columns: &'a [DesiredColumn],
    pub read: &'a [ReadColumn],
    pub constants: &'a [(String, ColumnType, String)],
    /// How the table names the columns a load adds.
    pub compat: Compat,
    /// The batch, from 1: a merge's `_sqlload_row` numbers its rows after earlier batches', by file and row in file.
    pub batch: u64,
}

/// `INSERT INTO into` every one of `columns` from a warehouse's unloaded Parquet files at `urls`: a column of `read` from the files, turned back from whole microseconds where it is read as a time or timestamp, then cast to its type, exactly whatever the session's time zone; a column of `constants` from its value; the load id and load time are the batch's; any other column is NULL.
pub(crate) fn unloaded_insert(
    into: &TableName,
    UnloadedColumns {
        columns,
        read,
        constants,
        compat,
        batch,
    }: UnloadedColumns<'_>,
    urls: &[String],
    (load_id, loaded_at): (&str, &str),
) -> String {
    let literal = |text: &str| DuckDbSql.render_loader_value_literal(text);
    let identifier = |name: &str| DuckDbSql.render_identifier(name);
    let values: Vec<String> = columns
        .iter()
        .map(|column| {
            match column_value(
                column,
                (read, constants),
                (compat, load_id, loaded_at),
                batch,
            ) {
                Some(value) => format!(
                    "CAST({value} AS {})",
                    DuckDbSql
                        .render_loader_logical_type(&column.column_type)
                        .sql
                ),
                None => "NULL".to_owned(),
            }
        })
        .collect();
    let targets: Vec<String> = columns
        .iter()
        .map(|column| identifier(&column.name))
        .collect();
    let urls: Vec<String> = urls.iter().map(|url| literal(url)).collect();
    let numbered = match columns.iter().any(|column| column.name == MERGE_ROW_COLUMN) {
        true => ", file_row_number = true",
        false => "",
    };
    format!(
        "INSERT INTO {} ({}) SELECT {} FROM read_parquet([{}], hive_partitioning = false{numbered}) AS f",
        DuckDbSql.render_qualified_name(into),
        targets.join(", "),
        values.join(", "),
        urls.join(", ")
    )
}

/// Where an unloaded batch's `column` comes from: the batch's load id or load time, a merge's row number (`batch`, then the file, then the row in it), the value of the constant it is, or the files' column of `read` it is, before its cast; `None` when the files lack it.
fn column_value(
    column: &DesiredColumn,
    (read, constants): (&[ReadColumn], &[(String, ColumnType, String)]),
    (compat, load_id, loaded_at): (Compat, &str, &str),
    batch: u64,
) -> Option<String> {
    let constant = constants
        .iter()
        .find(|(name, _, _)| *name == column.name)
        .map(|(_, _, value)| DuckDbSql.render_loader_value_literal(value));
    match (metadata_role(compat, &column.name), column.name.as_str()) {
        (Some(MetadataRole::LoadId), _) => Some(DuckDbSql.render_loader_value_literal(load_id)),
        (Some(MetadataRole::LoadedAt), _) => Some(DuckDbSql.render_loader_value_literal(loaded_at)),
        (None, MERGE_ROW_COLUMN) => Some(format!(
            "{batch} * {UNLOADED_BATCH_ROWS} + CAST(f.file_index AS BIGINT) * {UNLOADED_FILE_ROWS} + f.file_row_number"
        )),
        _ if constant.is_some() => constant,
        (_, name) => read
            .iter()
            .find(|read| read.name == name)
            .map(|read| unloaded_value(&read.output, &DuckDbSql.render_identifier(name))),
    }
}

/// File column `quoted` as the value it was unloaded from: a time or timestamp from its whole microseconds, anything else as it is. A timestamp with time zone goes through its UTC text with an explicit offset, the one conversion that is exact both with ICU loaded (whatever the session's time zone) and without it.
fn unloaded_value(output: &ColumnType, quoted: &str) -> String {
    let timestamp = format!("make_timestamp(CAST(f.{quoted} AS BIGINT))");
    match output {
        ColumnType::TimestampNtz => timestamp,
        ColumnType::TimestampTz => {
            format!("CAST(CAST({timestamp} AS VARCHAR) || '{UTC_OFFSET}' AS TIMESTAMPTZ)")
        }
        ColumnType::Time => format!("CAST({timestamp} AS TIME)"),
        _ => format!("f.{quoted}"),
    }
}

/// The `read_parquet` option that has MotherDuck read the files on its servers (`remote`), so no row passes through this machine; none for SQLLoad's own DuckDB.
fn on_servers(remote: bool) -> &'static str {
    match remote {
        true => ", MD_RUN = REMOTE",
        false => "",
    }
}

/// The statement, with its parameters, that gives DuckDB a temporary secret for `bucket`: scoped to it and named after it (hex, so two buckets never share a name), in `region`, with `keys` (key id, secret and session token, empty when there is none) passed as parameters and never in SQL, or else the AWS credential chain.
pub(crate) fn s3_secret(
    bucket: &str,
    region: String,
    keys: Option<&[String; 3]>,
) -> (String, Vec<String>) {
    let name: String = bucket.bytes().map(|byte| format!("{byte:02x}")).collect();
    let scope = format!("s3://{bucket}/");
    match keys {
        Some([key, secret, token]) => (
            format!(
                "CREATE OR REPLACE TEMPORARY SECRET sqlload_s3_{name} (TYPE s3, KEY_ID ?, SECRET ?, SESSION_TOKEN ?, REGION ?, SCOPE ?)"
            ),
            vec![key.clone(), secret.clone(), token.clone(), region, scope],
        ),
        None => (
            format!(
                "CREATE OR REPLACE TEMPORARY SECRET sqlload_s3_{name} (TYPE s3, PROVIDER credential_chain, REGION ?, SCOPE ?)"
            ),
            vec![region, scope],
        ),
    }
}

/// The region a secret without its connection's own gets: `AWS_REGION`, else `AWS_DEFAULT_REGION` (each as `variable` reads it from the environment, unset when empty), else us-east-1, as SQLLoad's reader picks it.
pub(crate) fn default_region(variable: &dyn Fn(&str) -> Option<String>) -> String {
    let set = |name: &str| variable(name).filter(|value| !value.is_empty());
    set("AWS_REGION")
        .or_else(|| set("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|| "us-east-1".to_owned())
}

/// What DuckDB needs to read files in an S3 or GCS bucket itself: the extensions to load (installing them on first use) and the [temporary secret](s3_secret), with its parameters, whose keys come from the AWS credential chain when `chain`.
pub(crate) struct BucketAccess {
    pub bucket: String,
    pub extensions: &'static str,
    pub statement: String,
    pub parameters: Vec<String>,
    pub chain: bool,
}

impl BucketAccess {
    /// The secret as it is set `at`, with a hash of its statement and parameters in place of the keys.
    pub(crate) fn set_at(&self, at: Instant) -> SetSecret {
        let mut hasher = DefaultHasher::new();
        (&self.statement, &self.parameters).hash(&mut hasher);
        SetSecret {
            hash: hasher.finish(),
            chain: self.chain,
            at,
        }
    }
}

/// Whether the secret `set` on the database for a bucket still serves `wanted`, the one a table would set `now`: the same statement and parameters, and, for keys DuckDB resolved through the AWS credential chain when it was set, set less than [`CHAIN_SECRET_REFRESH`] ago, as they may expire.
pub(crate) fn secret_current(set: Option<&SetSecret>, wanted: &SetSecret, now: Instant) -> bool {
    set.is_some_and(|set| {
        set.hash == wanted.hash
            && (!set.chain || now.saturating_duration_since(set.at) < CHAIN_SECRET_REFRESH)
    })
}

/// For files in S3, [`BucketAccess`] for their bucket: the secret in the connection's region, else `AWS_REGION` / `AWS_DEFAULT_REGION` (unset when empty), else us-east-1 as SQLLoad's reader does; with the keys SQLLoad's own S3 reads use (the `AWS_*` variables, else the shared credentials file under the connection's `profile` or `AWS_PROFILE`), passed as parameters and never in SQL, otherwise the AWS credential chain, resolved where DuckDB runs. For files in GCS, [`gcs_access`] when the connection has an HMAC key. `None` for files elsewhere.
pub(crate) fn bucket_access(store: &StoreKind) -> Result<Option<BucketAccess>, DuckDbError> {
    let (bucket, region, profile) = match store {
        StoreKind::S3 {
            bucket,
            region,
            profile,
            ..
        } => (bucket, region, profile),
        StoreKind::Gcs {
            bucket,
            hmac: Some(key),
            ..
        } => return Ok(Some(gcs_access(bucket, key))),
        StoreKind::Gcs { .. } | StoreKind::Filesystem { .. } | StoreKind::Sftp { .. } => {
            return Ok(None);
        }
    };
    let keys = s3_keys(profile.as_deref())
        .map_err(DuckDbError::Task)?
        .map(secret_parameters);
    let extensions = match keys {
        Some(_) => "INSTALL httpfs; LOAD httpfs;",
        None => "INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;",
    };
    let region = region
        .clone()
        .unwrap_or_else(|| default_region(&environment_variable));
    let (statement, parameters) = s3_secret(bucket, region, keys.as_ref());
    Ok(Some(BucketAccess {
        bucket: bucket.clone(),
        extensions,
        statement,
        parameters,
        chain: keys.is_none(),
    }))
}

/// For files in a GCS bucket, [`BucketAccess`] with the connection's HMAC key `key`, the only credential DuckDB reads GCS with: a temporary `gcs` secret scoped to the bucket and named after it (hex, as for S3), its key passed as parameters and never in SQL, kept apart from an S3 bucket of the same name.
fn gcs_access(bucket: &str, key: &HmacKey) -> BucketAccess {
    let name: String = bucket.bytes().map(|byte| format!("{byte:02x}")).collect();
    BucketAccess {
        bucket: format!("gs://{bucket}"),
        extensions: "INSTALL httpfs; LOAD httpfs;",
        statement: format!(
            "CREATE OR REPLACE TEMPORARY SECRET sqlload_gcs_{name} (TYPE gcs, KEY_ID ?, SECRET ?, SCOPE ?)"
        ),
        parameters: vec![
            key.key_id.clone(),
            key.secret.expose().to_owned(),
            format!("gs://{bucket}/"),
        ],
        chain: false,
    }
}

/// `keys` as the `KEY_ID`, `SECRET` and `SESSION_TOKEN` parameters of a DuckDB secret.
fn secret_parameters(keys: AwsCredentials) -> [String; 3] {
    let token = keys.session_token.map(|token| token.0).unwrap_or_default();
    [keys.key_id, keys.secret.0, token]
}
