use arrow_array::RecordBatch;
use tokio_postgres::Client;

use crate::column_types::main::parse_type::parse_type;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::file_store::main::open_folder::open_folder;
use crate::files_source::constants::SOURCE_FILE_COLUMN;
use crate::files_source::main::discover_files::discover_files;
use crate::files_source::main::read_files::read_files;
use crate::files_source::main::select_files::select_files;
use crate::files_source::models::{FileSelection, FilesRead};
use crate::loader::_helpers::destination::text_values;
use crate::loader::constants::SOURCE_BATCH_ROWS;
use crate::loader::errors::LoaderError;
use crate::loader::models::{
    Destination, FilesSource, PlanOptions, SourceReader, TablePlan, TableSpec,
};
use crate::postgres_source::main::connect_postgres::connect_postgres;
use crate::postgres_source::main::copy_table::copy_table;
use crate::postgres_source::models::{
    CopyRequest, PgType, PostgresAddress, ReadColumn, SourceColumn, SourceTable, WireType,
};
use crate::project::models::{
    ConnectionConfig, ProjectConfig, SourceConfig, SourceKind, WriteStrategy,
};
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn, LiveColumn};
use crate::store_paths::main::resolve_location::resolve_location;
use crate::store_paths::models::{Location, StoreKind};
use crate::write_sql::main::distinct_query::distinct_query;

/// A client for a source's Postgres connection.
pub(crate) async fn connect_source(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<Client, LoaderError> {
    let Some(ConnectionConfig::Postgres {
        host,
        port,
        user,
        password,
        database,
    }) = project.connections.get(&source.connection)
    else {
        return Err(LoaderError::Invalid(format!(
            "connection {:?} is not a Postgres connection",
            source.connection
        )));
    };
    let address = PostgresAddress {
        host,
        port: *port,
        user,
        password: password.as_ref().map(|secret| secret.expose()),
        database,
    };
    Ok(connect_postgres(address).await?)
}

/// The folder a files source reads: its `path` within its connection.
pub(crate) fn files_location(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<Location, LoaderError> {
    let store = match project.connections.get(&source.connection) {
        Some(ConnectionConfig::S3 {
            bucket,
            region,
            endpoint,
        }) => StoreKind::S3 {
            bucket: bucket.clone(),
            region: region.clone(),
            endpoint: endpoint.clone(),
        },
        Some(ConnectionConfig::Filesystem { root }) => StoreKind::Filesystem { root: root.clone() },
        _ => {
            return Err(LoaderError::Invalid(format!(
                "connection {:?} is not an s3 or filesystem connection",
                source.connection
            )));
        }
    };
    let path = source.path.as_deref().unwrap_or_default();
    resolve_location(Some((source.connection.as_str(), &store)), path)
        .map_err(|error| LoaderError::Invalid(error.to_string()))
}

/// A connected reader for `source`.
pub(crate) async fn open_source(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<SourceReader, LoaderError> {
    match source.kind {
        SourceKind::Postgres => Ok(SourceReader::Postgres(
            connect_source(project, source).await?,
        )),
        SourceKind::Files => Ok(SourceReader::Files(open_folder(&files_location(
            project, source,
        )?)?)),
    }
}

/// A files source as one source table (file columns, partition columns, the source-file column), and its files.
pub(crate) async fn discover_files_table(
    project: &ProjectConfig,
    source_name: &str,
    source: &SourceConfig,
    warehouse: Warehouse,
) -> Result<(SourceTable, FilesSource), LoaderError> {
    let location = files_location(project, source)?;
    let folder = open_folder(&location)?;
    let partition_names: Vec<String> = source.partition_columns.keys().cloned().collect();
    let table = discover_files(&folder, &partition_names).await?;
    if let Some(problem) = table.problems.first() {
        return Err(LoaderError::Invalid(format!(
            "[sources.{source_name}]: {problem}"
        )));
    }
    let mut partition_types: Vec<(String, ColumnType)> = Vec::new();
    for (name, text) in &source.partition_columns {
        let parsed = parse_type(text, &[warehouse]).map_err(|error| {
            LoaderError::Invalid(format!(
                "[sources.{source_name}] partition column {name}: {error}"
            ))
        })?;
        partition_types.push((name.clone(), parsed.column_type));
    }
    let typed = table
        .columns
        .iter()
        .map(|column| (column.name.clone(), column.column_type.clone()))
        .chain(
            partition_types
                .iter()
                .map(|(name, column_type)| (name.clone(), Ok(column_type.clone()))),
        )
        .chain([(SOURCE_FILE_COLUMN.to_owned(), Ok(ColumnType::Text))]);
    let columns: Vec<SourceColumn> = typed
        .map(|(name, column_type)| SourceColumn {
            name,
            postgres_type: "parquet".to_owned(),
            pg_type: PgType::Other,
            column_type,
            nullable: true,
        })
        .collect();
    let source_table = SourceTable {
        schema: String::new(),
        name: source_name.to_owned(),
        columns,
        primary_key: Vec::new(),
        estimated_rows: -1,
        estimated_bytes: table
            .files
            .iter()
            .map(|data| i64::try_from(data.file.size).unwrap_or(i64::MAX))
            .sum(),
    };
    Ok((
        source_table,
        FilesSource {
            location,
            table,
            partition_types,
        },
    ))
}

/// The read columns of a files source: the file's types, or the pinned ones (converted while reading).
pub(crate) fn files_read_columns(spec: &TableSpec) -> Result<Vec<ReadColumn>, Vec<String>> {
    let mut problems: Vec<String> = Vec::new();
    let mut columns: Vec<ReadColumn> = Vec::new();
    for column in &spec.source_table.columns {
        let excluded = spec
            .excluded_columns
            .iter()
            .any(|(excluded, _)| *excluded == column.name);
        let output = match (spec.pinned.get(&column.name), &column.column_type) {
            _ if excluded => continue,
            (Some(pinned), _) => pinned.clone(),
            (None, Ok(column_type)) => column_type.clone(),
            (None, Err(reason)) => {
                problems.push(format!("column {}: {reason}", column.name));
                continue;
            }
        };
        columns.push(ReadColumn {
            name: column.name.clone(),
            expression: column.name.clone(),
            wire: WireType::Text,
            output,
        });
    }
    match problems.is_empty() {
        true => Ok(columns),
        false => Err(problems),
    }
}

/// What a files source's load reads: the files in the cursor window that are not loaded yet.
pub(crate) struct FilesPlanRequest<'a> {
    pub(crate) spec: &'a TableSpec,
    pub(crate) files: &'a FilesSource,
    pub(crate) destination: &'a Destination,
    pub(crate) live: Option<&'a [LiveColumn]>,
    pub(crate) read_columns: &'a [ReadColumn],
    pub(crate) written: &'a [String],
    pub(crate) bounds: (Option<&'a str>, Option<&'a str>),
    pub(crate) options: &'a PlanOptions,
}

/// The files to read, or why the table cannot be loaded.
pub(crate) async fn plan_files(
    request: &FilesPlanRequest<'_>,
) -> Result<Result<FilesRead, String>, LoaderError> {
    let spec = request.spec;
    let cursor = match &spec.cursor {
        None => None,
        Some(cursor) => match request
            .files
            .partition_types
            .iter()
            .find(|(name, _)| *name == cursor.column)
        {
            Some((name, column_type)) => Some((name.as_str(), column_type)),
            None => {
                return Ok(Err(format!(
                    "cursor_column {} must be one of the partition_columns for a files source",
                    cursor.column
                )));
            }
        },
    };
    let skips_loaded = matches!(spec.strategy, WriteStrategy::Append | WriteStrategy::Merge);
    if skips_loaded
        && !request
            .written
            .iter()
            .any(|name| name == SOURCE_FILE_COLUMN)
    {
        return Ok(Err(format!(
            "{SOURCE_FILE_COLUMN} must be loaded: append and merge use it to skip files already loaded"
        )));
    }
    let has_record = request.live.is_some_and(|live| {
        live.iter()
            .any(|column| column.name.eq_ignore_ascii_case(SOURCE_FILE_COLUMN))
    });
    let loaded = match skips_loaded && has_record && !request.options.reload {
        true => {
            let query = distinct_query(&spec.destination, SOURCE_FILE_COLUMN, spec.warehouse);
            text_values(request.destination, query).await?
        }
        false => Vec::new(),
    };
    let location = request.files.location.to_string();
    let selection = FileSelection {
        files: &request.files.table.files,
        location: &location,
        loaded: &loaded,
        cursor,
        from: request.bounds.0,
        to: request.bounds.1,
    };
    let columns: Vec<DesiredColumn> = request
        .read_columns
        .iter()
        .map(|column| DesiredColumn {
            name: column.name.clone(),
            column_type: column.output.clone(),
            origin: ColumnOrigin::Source,
        })
        .collect();
    Ok(select_files(&selection).map(|files| FilesRead {
        files,
        columns,
        batch_rows: SOURCE_BATCH_ROWS,
    }))
}

/// Read `table`'s data from `reader`, passing each batch to `on_batch`; returns the row count.
pub(crate) async fn read_source(
    reader: &SourceReader,
    table: &TablePlan,
    on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, LoaderError> {
    match (reader, &table.files) {
        (SourceReader::Postgres(client), _) => {
            let request = CopyRequest {
                table: &table.spec.source_table,
                columns: table.read_columns.clone(),
                filter: table.source_filter.clone(),
                batch_rows: SOURCE_BATCH_ROWS,
            };
            Ok(copy_table(client, request, on_batch).await?)
        }
        (SourceReader::Files(folder), Some(files)) => {
            Ok(read_files(folder, files, on_batch).await?)
        }
        (SourceReader::Files(_), None) => Err(LoaderError::Invalid(format!(
            "{}: a files source has no files planned",
            table.spec.source_table.qualified_name()
        ))),
    }
}

/// Whether `selectors` pick a table, by source name, destination name or `schema.table`.
pub(crate) fn selected(
    selectors: &[String],
    source: &str,
    destination: &str,
    source_table: &str,
) -> bool {
    selectors.is_empty()
        || selectors.iter().any(|selector| {
            selector == source || selector == destination || selector == source_table
        })
}
