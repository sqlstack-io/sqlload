use arrow_array::RecordBatch;
use tokio_postgres::Client;

use crate::column_types::main::parse_type::parse_type;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::file_store::main::open_folder::open_folder;
use crate::files_source::constants::{
    SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN, VERSION_SEPARATOR,
};
use crate::files_source::main::discover_files::discover_files;
use crate::files_source::main::read_files::read_files;
use crate::files_source::main::select_files::select_files;
use crate::files_source::models::{
    DataFile, FileSelection, FilesRead, FilesTable, LoadedFile, SourceFormat, TextChecks,
};
use crate::loader::_helpers::destination::text_values;
use crate::loader::_helpers::python::python_tables;
use crate::loader::_helpers::resolve::source_target;
use crate::loader::constants::SOURCE_BATCH_ROWS;
use crate::loader::errors::LoaderError;
use crate::loader::models::{
    Destination, FilesSource, PlanOptions, SourceReader, TablePlan, TableSpec,
};
use crate::postgres_source::constants::DEFAULT_BATCH_BYTES;
use crate::postgres_source::main::connect_postgres::connect_postgres;
use crate::postgres_source::main::copy_table::copy_table;
use crate::postgres_source::main::read_catalog::read_catalog;
use crate::postgres_source::models::{
    CopyRequest, PgType, PostgresAddress, ReadColumn, SourceColumn, SourceTable, WireType,
};
use crate::project::models::{
    ConnectionConfig, ProjectConfig, SourceConfig, SourceKind, TableOverride, WriteStrategy,
};
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn, LiveColumn};
use crate::similar_names::main::close_name::close_name;
use crate::store_paths::main::resolve_location::resolve_location;
use crate::store_paths::models::{Location, StoreKind};
use crate::write_sql::main::loaded_files_query::loaded_files_query;

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
        SourceKind::Python => Err(LoaderError::Invalid(
            "a Python source is read by calling its table functions".to_owned(),
        )),
    }
}

/// A source's tables, and its files for a files source; `Ok(Err(problem))` when they cannot be found.
pub(crate) async fn source_tables(
    project: &ProjectConfig,
    source_name: &str,
    source: &SourceConfig,
) -> Result<Result<(Vec<SourceTable>, Option<FilesSource>), String>, LoaderError> {
    match source.kind {
        SourceKind::Postgres => {
            let client = connect_source(project, source).await?;
            let discovered = read_catalog(&client, &source.include, &source.exclude).await?;
            Ok(Ok((discovered, None)))
        }
        SourceKind::Files => {
            let warehouse = match source_target(project, source) {
                Ok(target) => target.warehouse,
                Err(problem) => return Ok(Err(problem)),
            };
            Ok(
                match discover_files_table(project, source_name, source, warehouse).await {
                    Ok((table, files)) => Ok((vec![table], Some(files))),
                    Err(error) => Err(error.to_string()),
                },
            )
        }
        SourceKind::Python => Ok(Ok((python_tables(source), None))),
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
    let encoding = source.files_encoding();
    let table = discover_files(&folder, &partition_names, &encoding).await?;
    if let Some(problem) = table.problems.first() {
        return Err(LoaderError::Invalid(problem.clone()));
    }
    files_source_parts(source_name, source, warehouse, (location, table))
}

/// A files source's table and files from the files found: the file columns, partition columns and source-file columns as one source table.
pub(crate) fn files_source_parts(
    source_name: &str,
    source: &SourceConfig,
    warehouse: Warehouse,
    (location, table): (Location, FilesTable),
) -> Result<(SourceTable, FilesSource), LoaderError> {
    let encoding = source.files_encoding();
    let mut partition_types: Vec<(String, ColumnType)> = Vec::new();
    for (name, text) in &source.partition_columns {
        let parsed = parse_type(text, &[warehouse])
            .map_err(|error| LoaderError::Invalid(format!("partition column {name}: {error}")))?;
        partition_types.push((name.clone(), parsed.column_type));
    }
    let added: Vec<&str> = partition_types
        .iter()
        .map(|(name, _)| name.as_str())
        .chain([SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN])
        .collect();
    let overrides = source.tables.get(source_name);
    let pinned_only = pinned_unsampled(overrides, &table, &added, encoding.format)?;
    let dropped = (!table.dropped.is_empty()).then(|| {
        format!(
            "dropped {} from the files (replaced by this load's)",
            table.dropped.join(", ")
        )
    });
    let notes: Vec<String> = dropped
        .into_iter()
        .chain(pinned_only.iter().map(|name| {
            format!("pinned column {name} was not seen in the sampled files (check the name)")
        }))
        .collect();
    let typed = table
        .columns
        .iter()
        .filter(|column| !added.contains(&column.name.as_str()))
        .map(|column| (column.name.clone(), column.column_type.clone()))
        .chain(
            pinned_only
                .into_iter()
                .map(|name| (name, Ok(ColumnType::Text))),
        )
        .chain(
            partition_types
                .iter()
                .map(|(name, column_type)| (name.clone(), Ok(column_type.clone()))),
        )
        .chain([
            (SOURCE_FILE_COLUMN.to_owned(), Ok(ColumnType::Text)),
            (SOURCE_VERSION_COLUMN.to_owned(), Ok(ColumnType::Text)),
        ]);
    let columns: Vec<SourceColumn> = typed
        .map(|(name, column_type)| SourceColumn {
            name,
            postgres_type: encoding.format.to_string(),
            pg_type: PgType::Other,
            column_type,
            nullable: true,
        })
        .collect();
    let checks = TextChecks {
        known: columns.iter().map(|column| column.name.clone()).collect(),
        exclude: [
            source.exclude_columns.as_slice(),
            overrides.map_or(&[], |table| table.exclude_columns.as_slice()),
        ]
        .concat(),
        allow: [
            source.allow_columns.as_slice(),
            overrides.map_or(&[], |table| table.allow_columns.as_slice()),
        ]
        .concat(),
        inferred: Vec::new(),
    };
    let source_table = SourceTable {
        schema: String::new(),
        name: source_name.to_owned(),
        columns,
        primary_key: Vec::new(),
        indexed_columns: Vec::new(),
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
            encoding,
            checks,
            notes,
        },
    ))
}

/// Columns a JSON or CSV source's table pins in `columns` that sampling did not see, so a key found only in older files can be pinned; a name close to a sampled one (a typo, or another case) is an error, and a `mask` key alone never adds a column.
fn pinned_unsampled(
    overrides: Option<&TableOverride>,
    table: &FilesTable,
    added: &[&str],
    format: SourceFormat,
) -> Result<Vec<String>, LoaderError> {
    let Some(overrides) = overrides.filter(|_| format != SourceFormat::Parquet) else {
        return Ok(Vec::new());
    };
    let sampled: Vec<&str> = table
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    let mut unsampled: Vec<String> = Vec::new();
    for name in overrides.columns.keys() {
        if added.contains(&name.as_str()) || sampled.contains(&name.as_str()) {
            continue;
        }
        let known = sampled.iter().chain(added).copied();
        if let Some(close) = close_name(name, known) {
            return Err(LoaderError::Invalid(format!(
                "pinned column {name} was not seen in the sampled files; did you mean {close:?}? (pin a name exactly as it appears in the files)"
            )));
        }
        unsampled.push(name.clone());
    }
    Ok(unsampled)
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

/// A row of the loaded-files query: `file|version`, or just the file when `versioned` is false.
fn loaded_file(text: String, versioned: bool) -> LoadedFile {
    match (versioned, text.rsplit_once(VERSION_SEPARATOR)) {
        (true, Some((name, version))) => LoadedFile {
            name: name.to_owned(),
            version: version.to_owned(),
        },
        _ => LoadedFile {
            name: text,
            version: String::new(),
        },
    }
}

/// Whether the live table has `column`.
fn live_has(live: Option<&[LiveColumn]>, column: &str) -> bool {
    match live {
        Some(live) => live
            .iter()
            .any(|existing| existing.name.eq_ignore_ascii_case(column)),
        None => false,
    }
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
    if spec.strategy == WriteStrategy::Merge {
        return Ok(Err(
            "merge is not supported for files sources yet (loaded files would be re-read once their rows are updated); use delete_insert on a partition cursor, or append".to_owned(),
        ));
    }
    let skips_loaded = spec.strategy == WriteStrategy::Append;
    if skips_loaded
        && request.live.is_some()
        && !live_has(request.live, SOURCE_FILE_COLUMN)
        && !request.options.reload
    {
        return Ok(Err(format!(
            "the table has no {SOURCE_FILE_COLUMN} column, so append cannot tell which files are loaded and would load them all again; run it once with --reload"
        )));
    }
    if skips_loaded
        && !request
            .written
            .iter()
            .any(|name| name == SOURCE_FILE_COLUMN)
    {
        return Ok(Err(format!(
            "{SOURCE_FILE_COLUMN} must be loaded: append uses it to skip files already loaded"
        )));
    }
    let live_has = |column: &str| live_has(request.live, column);
    let loaded: Vec<LoadedFile> =
        match skips_loaded && live_has(SOURCE_FILE_COLUMN) && !request.options.reload {
            true => {
                let version = live_has(SOURCE_VERSION_COLUMN).then_some(SOURCE_VERSION_COLUMN);
                let query = loaded_files_query(
                    &spec.destination,
                    SOURCE_FILE_COLUMN,
                    version,
                    spec.warehouse,
                );
                text_values(request.destination, query)
                    .await?
                    .into_iter()
                    .map(|text| loaded_file(text, version.is_some()))
                    .collect()
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
    let constants: Vec<&str> = request
        .files
        .partition_types
        .iter()
        .map(|(name, _)| name.as_str())
        .chain([SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN])
        .collect();
    let checks = TextChecks {
        inferred: request
            .read_columns
            .iter()
            .map(|column| column.name.clone())
            .filter(|name| !spec.pinned.contains_key(name) && !constants.contains(&name.as_str()))
            .collect(),
        ..request.files.checks.clone()
    };
    Ok(select_files(&selection).map(|files| FilesRead {
        files,
        encoding: request.files.encoding.clone(),
        checks,
        columns,
        batch_rows: SOURCE_BATCH_ROWS,
    }))
}

/// Which part of a table's planned read one pass reads: a cursor window's filter, an order, or some of a files source's files.
#[derive(Debug, Clone, Default)]
pub(crate) struct ReadPart {
    /// The Postgres filter; `None` reads the plan's.
    pub(crate) filter: Option<String>,
    /// `ORDER BY` this quoted column.
    pub(crate) order_by: Option<String>,
    /// The files to read; `None` reads every planned file.
    pub(crate) files: Option<Vec<DataFile>>,
    /// A smaller byte cap for the source's batches than the usual 64 MiB (the hidden `--batch-bytes`, for tests).
    pub(crate) batch_bytes: Option<usize>,
}

/// Read `part` of `table`'s data from `reader`, passing each batch to `on_batch`; returns the row count.
pub(crate) async fn read_source(
    reader: &SourceReader,
    table: &TablePlan,
    part: ReadPart,
    on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, LoaderError> {
    match (reader, &table.files) {
        (SourceReader::Postgres(client), _) => {
            let request = CopyRequest {
                table: &table.spec.source_table,
                columns: table.read_columns.clone(),
                filter: part.filter.or_else(|| table.source_filter.clone()),
                order_by: part.order_by,
                batch_rows: SOURCE_BATCH_ROWS,
                batch_bytes: part
                    .batch_bytes
                    .map_or(DEFAULT_BATCH_BYTES, |bytes| bytes.min(DEFAULT_BATCH_BYTES)),
            };
            Ok(copy_table(client, request, on_batch).await?)
        }
        (SourceReader::Files(folder), Some(files)) => {
            let read = match part.files {
                Some(subset) => FilesRead {
                    files: subset,
                    ..files.clone()
                },
                None => files.clone(),
            };
            Ok(read_files(folder, &read, on_batch).await?)
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
