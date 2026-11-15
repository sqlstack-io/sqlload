use crate::adapters::contract::models::BoundParameter;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio_postgres::Client;

use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::main::session::live_columns::live_columns;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::adapters::dispatch::models::Destination;
use crate::auth::constants::{GOOGLE_AUTHORIZE_URL, GOOGLE_TOKEN_URL};
use crate::auth::main::google_token_cache_file::google_token_cache_file;
use crate::auth::models::GoogleUser;
use crate::bigquery_source::main::bigquery_reader::bigquery_reader;
use crate::bigquery_source::main::read_catalog::read_catalog as read_bigquery_catalog;
use crate::bigquery_source::main::read_rows::read_rows as read_bigquery_rows;
use crate::bigquery_source::models::{BigQueryReadRequest, BigQueryReader};
use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::config::models::{DestinationConfig, DestinationKind};
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::constants::{
    SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN, SOURCE_VERSION_COLUMN,
};
use crate::files_source::errors::FilesError;
use crate::files_source::main::discover_files::discover_files;
use crate::files_source::main::discover_reload_files::discover_reload_files;
use crate::files_source::main::merge_order::merge_order;
use crate::files_source::main::move_to_prefix::move_to_prefix;
use crate::files_source::main::read_files::read_files;
use crate::files_source::main::select_files::select_files;
use crate::files_source::main::shape_rows::shape_rows;
use crate::files_source::models::{
    AfterLoad, BatchFooters, DataFile, FileColumn, FileSelection, FilesEncoding, FilesRead,
    FilesTable, LoadedFile, StoredRead, TextChecks,
};
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;
use crate::load_naming::main::loaded_name::loaded_name;
use crate::load_naming::models::Naming;
use crate::loader::_helpers::destination::{folder_claims, nanosecond_columns};
use crate::loader::_helpers::python::{fitting_type, python_tables};
use crate::loader::_helpers::python::{read_schema, read_types};
use crate::loader::_helpers::resolve::{
    display_name, resolve_tables, source_target, spec_names, target_display_name, unread_source,
};
use crate::loader::_helpers::run_log::{
    file_column_problem, loaded_file, logs_files, merge_ledger, reload_order,
};
use crate::loader::_helpers::specs::integer_cursor;
use crate::loader::_helpers::specs::{files_read_as, path_values, pinned_unsampled};
use crate::loader::_helpers::specs::{
    flattened_object_problems, referenced_columns, table_name, table_naming,
};
use crate::loader::_helpers::specs::{returned_columns, runnable_transform, transform_input};
use crate::loader::constants::NO_TRANSFORM_HOST;
use crate::loader::constants::{
    DRAINED_REASON, KEPT_TABLES_COLUMN_FIX, LOCALHOST, PARALLEL_READERS, PRIMARY_KEY_PLACEHOLDER,
    RELOAD_FIX, SOURCE_BATCH_ROWS,
};
use crate::loader::errors::LoaderError;
use crate::loader::models::FilesTransform;
use crate::loader::models::{
    CommitRule, DrainedFiles, FilesSource, PlanOptions, ResolvedTables, SkippedTable, SourceEnd,
    SourceProblem, SourceReader, TablePlan, TableSpec,
};
use crate::loader::models::{FoundSource, FoundSources, TransformRun};
use crate::postgres_source::constants::DEFAULT_BATCH_BYTES;
use crate::postgres_source::constants::SPLIT_READ_MIN_PAGES;
use crate::postgres_source::main::connect_postgres::connect_postgres;
use crate::postgres_source::main::copy_table::copy_table;
use crate::postgres_source::main::cursor_end::cursor_end;
use crate::postgres_source::main::read_catalog::read_catalog;
use crate::postgres_source::models::{
    CopyRequest, Encryption, PgType, PostgresAddress, ReadColumn, SourceColumn, SourceTable,
    SslMode,
};
use crate::project::main::postgres_address::postgres_address;
use crate::project::models::{
    BatchStep, ConnectionConfig, OnDuplicateKeys, ProjectConfig, SourceConfig, SourceKind,
    TableOverride, TableSettings, WriteStrategy,
};
use crate::python_sources::main::transform_files::transform_files;
use crate::python_sources::main::transform_range::transform_range;
use crate::python_sources::models::{TransformCall, TransformContext, TransformFiles};
use crate::python_sources::types::PythonHost;
use crate::row_batches::main::loaded_batch::loaded_batch;
use crate::schema_plan::models::LiveColumn;
use crate::sheets_source::constants::{EMPTY_SHEET_COLUMN_KIND, SCOPE, SHEET_COLUMN_KIND};
use crate::sheets_source::main::a1_width::a1_width;
use crate::sheets_source::main::duplicate_key::duplicate_key;
use crate::sheets_source::main::read_grids::read_grids;
use crate::sheets_source::main::sheet_batch::sheet_batch;
use crate::sheets_source::main::sheet_input::sheet_input;
use crate::sheets_source::main::sheet_table::sheet_table;
use crate::sheets_source::main::sheets_client::sheets_client;
use crate::sheets_source::main::tab_name::tab_name;
use crate::sheets_source::main::tab_ranges::tab_ranges;
use crate::sheets_source::main::user_sheets_client::user_sheets_client;
use crate::sheets_source::models::{Grid, HeaderRule, KeySource, SheetsClient};
use crate::snowflake_source::constants::SESSION_SCHEMA;
use crate::snowflake_source::main::connect_snowflake_source::connect_snowflake_source;
use crate::snowflake_source::main::read_catalog::read_catalog as read_snowflake_catalog;
use crate::snowflake_source::main::unload_files::unload_files;
use crate::snowflake_source::main::unload_rows::unload_rows;
use crate::snowflake_source::models::{UnloadRequest, UnloadedFiles};
use crate::ssh_tunnel::models::SshTunnel;
use crate::store_paths::main::resolve_location::resolve_location;
use crate::store_paths::models::{Location, StoreKind};

/// The sources the plan running now has finished checking, which only the line `plan` shows on a terminal reads.
pub(crate) static PLANNED_SOURCES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// A client for a source's Postgres connection, and what protects it.
pub(crate) async fn connect_source(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<(Client, Encryption), LoaderError> {
    let address = postgres_address(project, &source.connection).map_err(LoaderError::Invalid)?;
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
            profile,
        }) => StoreKind::S3 {
            bucket: bucket.clone(),
            region: region.clone(),
            endpoint: endpoint.clone(),
            profile: profile.clone(),
        },
        Some(ConnectionConfig::Filesystem { root }) => StoreKind::Filesystem { root: root.clone() },
        Some(ConnectionConfig::Sftp {
            host,
            port,
            user,
            private_key_file,
            private_key_passphrase,
            password,
            known_hosts,
        }) => StoreKind::Sftp {
            server: Box::new(SshTunnel {
                host: host.clone(),
                port: *port,
                user: user.clone(),
                private_key_file: private_key_file.clone(),
                private_key_passphrase: private_key_passphrase.clone(),
                password: password.clone(),
                known_hosts: Some(known_hosts.clone()),
            }),
            root: String::new(),
        },
        _ => {
            return Err(LoaderError::Invalid(format!(
                "connection {:?} is not an s3, filesystem or sftp connection",
                source.connection
            )));
        }
    };
    let path = source.path.as_deref().unwrap_or_default();
    resolve_location(Some((source.connection.as_str(), &store)), path)
        .map_err(|error| LoaderError::Invalid(error.to_string()))
}

/// The Postgres connections a table's read uses: several for a plain table of `SPLIT_READ_MIN_PAGES` or more on Postgres 14+, read whole and in no order (windows and cursor-ordered reads keep one connection).
pub(crate) fn source_readers(table: &TablePlan) -> usize {
    let whole = matches!(table.commits, CommitRule::Size | CommitRule::Whole(_));
    let postgres = table.spec.python.is_none()
        && table.spec.files.is_none()
        && table.spec.sheet.is_none()
        && table.spec.warehouse_source.is_none();
    let large = table
        .spec
        .source_table
        .split_pages
        .is_some_and(|pages| pages >= SPLIT_READ_MIN_PAGES);
    match whole && postgres && large {
        true => PARALLEL_READERS,
        false => 1,
    }
}

/// A connected reader for `source`, with up to `readers` connections in all for a Postgres table read in parallel (a helper that cannot connect is left out).
pub(crate) async fn open_source(
    project: &ProjectConfig,
    source: &SourceConfig,
    readers: usize,
) -> Result<SourceReader, LoaderError> {
    match source.kind {
        SourceKind::Postgres | SourceKind::PostgresCdc => {
            let (client, _) = connect_source(project, source).await?;
            let mut helpers = Vec::with_capacity(readers.saturating_sub(1));
            for _ in 1..readers {
                match connect_source(project, source).await {
                    Ok((helper, _)) => helpers.push(helper),
                    Err(_) => break,
                }
            }
            Ok(SourceReader::Postgres(client, helpers))
        }
        SourceKind::Files => Ok(SourceReader::Files(open_folder(&files_location(
            project, source,
        )?)?)),
        SourceKind::Snowflake => Ok(SourceReader::Snowflake(Box::new(connect_snowflake_source(
            snowflake_session(project, source)?,
            source.stage.clone(),
        )?))),
        SourceKind::Odbc => Ok(open_odbc(project, source)?),
        SourceKind::BigQuery => Ok(SourceReader::BigQuery(Box::new(bigquery_source_reader(
            project, source,
        )?))),
        SourceKind::Python => Err(LoaderError::Invalid(
            "a Python source is read by calling its table functions".to_owned(),
        )),
        SourceKind::GoogleSheets => {
            let (client, spreadsheet_id) = sheets_reader(project, source)?;
            Ok(SourceReader::Sheets(client, spreadsheet_id))
        }
    }
}

/// A source's tables, its files for a files source, and warnings about reaching it; `Ok(Err(problem))` when they cannot be found, including when a Postgres source cannot be reached or its `include`/`exclude` patterns are invalid.
pub(crate) async fn source_tables(
    (project, options): (&ProjectConfig, &PlanOptions),
    source_name: &str,
    source: &SourceConfig,
) -> Result<Result<(Vec<SourceTable>, Option<FilesSource>, Vec<String>), SourceProblem>, LoaderError>
{
    match source.kind {
        SourceKind::Postgres | SourceKind::PostgresCdc => {
            let address = match postgres_address(project, &source.connection) {
                Ok(address) => address,
                Err(problem) => return Ok(Err(problem.into())),
            };
            let (client, encryption) = match connect_postgres(address.clone()).await {
                Ok(connected) => connected,
                Err(error) => return Ok(Err(LoaderError::from(error).into())),
            };
            let warnings: Vec<String> =
                plain_text_warning((source_name, &source.connection), &address, encryption)
                    .into_iter()
                    .collect();
            Ok(
                match read_catalog(&client, &source.include, &source.exclude).await {
                    Ok(discovered) => Ok((discovered, None, warnings)),
                    Err(error) => Err(LoaderError::from(error).into()),
                },
            )
        }
        SourceKind::Files => {
            let target = match source_target(project, source) {
                Ok(target) => target,
                Err(problem) => return Ok(Err(problem.into())),
            };
            let unset = TableSettings::default();
            let settings = source
                .tables
                .get(source_name)
                .map_or(&unset, |overrides| &overrides.settings);
            let naming = table_naming((settings, &source.defaults), target.naming);
            Ok(
                match discover_files_table(
                    (project, options),
                    (source_name, source),
                    (target.warehouse, naming),
                )
                .await
                {
                    Ok((table, files)) => Ok((vec![table], Some(files), Vec::new())),
                    Err(LoaderError::Files(FilesError::NoFiles { .. }))
                        if source.after_load != AfterLoad::Keep =>
                    {
                        Ok((Vec::new(), None, Vec::new()))
                    }
                    Err(error) => Err(error.into()),
                },
            )
        }
        SourceKind::Snowflake => {
            let reader = match snowflake_session(project, source)
                .and_then(|session| Ok(connect_snowflake_source(session, None)?))
            {
                Ok(reader) => reader,
                Err(error) => return Ok(Err(error.into())),
            };
            let queries: Vec<(String, String)> = source
                .tables
                .iter()
                .filter_map(|(name, table)| Some((name.clone(), table.sql.clone()?)))
                .collect();
            Ok(
                match read_snowflake_catalog(&reader, (&source.include, &source.exclude), &queries)
                    .await
                {
                    Ok(discovered) => Ok((discovered, None, Vec::new())),
                    Err(error) => Err(LoaderError::from(error).into()),
                },
            )
        }
        SourceKind::Odbc => {
            let queries: Vec<(String, String)> = source
                .tables
                .iter()
                .filter_map(|(name, table)| Some((name.clone(), table.sql.clone()?)))
                .collect();
            Ok(discover_odbc(project, source, &queries)
                .await
                .map(|discovered| (discovered, None, Vec::new()))
                .map_err(SourceProblem::from))
        }
        SourceKind::BigQuery => {
            let reader = match bigquery_source_reader(project, source) {
                Ok(reader) => reader,
                Err(error) => return Ok(Err(error.into())),
            };
            let queries: Vec<(String, String)> = source
                .tables
                .iter()
                .filter_map(|(name, table)| Some((name.clone(), table.sql.clone()?)))
                .collect();
            Ok(
                match read_bigquery_catalog(&reader, (&source.include, &source.exclude), &queries)
                    .await
                {
                    Ok(discovered) => Ok((discovered, None, Vec::new())),
                    Err(error) => Err(LoaderError::from(error).into()),
                },
            )
        }
        SourceKind::Python => Ok(Ok((python_tables(source), None, Vec::new()))),
        SourceKind::GoogleSheets => Ok(match sheets_tables(project, source).await {
            Ok((tables, warnings)) => Ok((tables, None, warnings)),
            Err(error) => Err(error.into()),
        }),
    }
}

/// An `odbc` source's reader, for its connection's settings and the source's value caps.
#[cfg(feature = "odbc")]
pub(crate) fn odbc_source_reader(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<crate::odbc_source::models::OdbcReader, LoaderError> {
    let Some(ConnectionConfig::Odbc {
        connection_string,
        dsn,
        user,
        password,
    }) = project.connections.get(&source.connection)
    else {
        return Err(LoaderError::Invalid(format!(
            "connection {:?} is not an odbc connection",
            source.connection
        )));
    };
    let limits = crate::odbc_source::models::ValueLimits {
        max_text_size: source.max_text_size,
        max_binary_size: source.max_binary_size,
        long_column_threshold: source
            .long_column_threshold
            .map(|size| usize::try_from(size.0).unwrap_or(usize::MAX)),
    };
    crate::odbc_source::main::odbc_reader::odbc_reader(
        (connection_string.as_ref(), dsn.as_deref()),
        (user.as_deref(), password.as_ref()),
        limits,
    )
    .map_err(LoaderError::Invalid)
}

/// `planned`, each unblocked table of ODBC source `source` given how its rows are fetched on this machine: its budget, and in bulk or value by value (see [`plan_fetches`](plan_fetches)).
#[cfg(feature = "odbc")]
pub(crate) async fn with_fetch_memory(
    project: &ProjectConfig,
    source: &SourceConfig,
    mut planned: Vec<TablePlan>,
    options: &PlanOptions,
) -> Result<Vec<TablePlan>, LoaderError> {
    let reader = odbc_source_reader(project, source)?;
    let open: Vec<usize> = (0..planned.len())
        .filter(|&index| planned[index].schema.blocked.is_none())
        .collect();
    let requests: Vec<crate::odbc_source::models::OdbcReadRequest<'_>> = open
        .iter()
        .map(|&index| crate::odbc_source::models::OdbcReadRequest {
            table: &planned[index].spec.source_table,
            sql: planned[index]
                .spec
                .warehouse_source
                .as_ref()
                .and_then(|source| source.sql.as_deref()),
            columns: &planned[index].read_columns,
            filter: None,
            parameters: &[],
            batch_rows: SOURCE_BATCH_ROWS,
            memory_budget: 0,
        })
        .collect();
    let set = source
        .max_batch_memory
        .map(|size| usize::try_from(size.0).unwrap_or(usize::MAX));
    let fetches = crate::odbc_source::main::plan_fetches::plan_fetches(
        &reader,
        &requests,
        (
            set,
            options
                .concurrency
                .or(project.concurrency)
                .unwrap_or(crate::loader::constants::DEFAULT_THREADS),
        ),
    )
    .await;
    for (index, fetch) in open.into_iter().zip(fetches) {
        planned[index].fetch_memory = Some(crate::loader::models::FetchMemory {
            budget: fetch.budget,
            estimate: fetch.estimate,
            memory: fetch.memory,
            budget_note: fetch.budget_note,
            limit: fetch.limit,
        });
    }
    Ok(planned)
}

#[cfg(feature = "odbc")]
fn open_odbc(project: &ProjectConfig, source: &SourceConfig) -> Result<SourceReader, LoaderError> {
    Ok(SourceReader::Odbc(Box::new(odbc_source_reader(
        project, source,
    )?)))
}

#[cfg(not(feature = "odbc"))]
fn open_odbc(_: &ProjectConfig, _: &SourceConfig) -> Result<SourceReader, LoaderError> {
    Err(LoaderError::Invalid(
        crate::loader::constants::NO_ODBC.to_owned(),
    ))
}

/// An `odbc` source's tables, or why they cannot be listed.
#[cfg(feature = "odbc")]
async fn discover_odbc(
    project: &ProjectConfig,
    source: &SourceConfig,
    queries: &[(String, String)],
) -> Result<Vec<SourceTable>, String> {
    let reader = odbc_source_reader(project, source).map_err(|error| error.to_string())?;
    crate::odbc_source::main::read_catalog::read_catalog(
        &reader,
        (&source.include, &source.exclude),
        queries,
    )
    .await
    .map_err(|error| error.to_string())
}

#[cfg(not(feature = "odbc"))]
async fn discover_odbc(
    _: &ProjectConfig,
    _: &SourceConfig,
    _: &[(String, String)],
) -> Result<Vec<SourceTable>, String> {
    Err(crate::loader::constants::NO_ODBC.to_owned())
}

/// A `bigquery` source's reader, for its connection and the project its tables live in.
pub(crate) fn bigquery_source_reader(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<BigQueryReader, LoaderError> {
    let connection = project.connections.get(&source.connection).ok_or_else(|| {
        LoaderError::Invalid(format!(
            "connection {:?} is not a [connections] entry",
            source.connection
        ))
    })?;
    Ok(bigquery_reader(connection, source.project.as_deref())?)
}

/// A Sheets client for a `google_sheets` source's connection, with the key it names (or `GOOGLE_APPLICATION_CREDENTIALS`'s) or as the Google user who consents to its OAuth client, and the spreadsheet it reads.
pub(crate) fn sheets_reader(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<(SheetsClient, String), LoaderError> {
    let Some(ConnectionConfig::GoogleSheets {
        service_account_file,
        service_account_json,
        service_account_json_base64,
        client_id,
        client_secret,
        refresh_token,
        base_url,
        token_url,
    }) = project.connections.get(&source.connection)
    else {
        return Err(LoaderError::Invalid(format!(
            "connection {:?} is not a google_sheets connection",
            source.connection
        )));
    };
    let given = [
        service_account_json.is_some(),
        service_account_json_base64.is_some(),
        service_account_file.is_some(),
        client_id.is_some(),
    ];
    if given.into_iter().filter(|given| *given).count() > 1 {
        return Err(LoaderError::Invalid(format!(
            "connection {:?} sets more than one of service_account_json, service_account_json_base64, service_account_file and client_id; keep one",
            source.connection
        )));
    }
    let http = build_http_client(&ConnectionCounter::default())?;
    let client = match (client_id, client_secret) {
        (Some(client_id), Some(client_secret)) => {
            let cache_file = google_token_cache_file().ok_or_else(|| {
                LoaderError::Invalid(
                    "no cache folder for the Google sign-in: set HOME or XDG_CACHE_HOME (LOCALAPPDATA on Windows)".to_owned(),
                )
            })?;
            let user = GoogleUser {
                client_id: client_id.clone(),
                client_secret: client_secret.clone(),
                refresh_token: refresh_token.clone(),
                scope: SCOPE.to_owned(),
                connection: source.connection.clone(),
                authorize_url: GOOGLE_AUTHORIZE_URL.to_owned(),
                token_url: token_url.as_deref().unwrap_or(GOOGLE_TOKEN_URL).to_owned(),
                cache_file,
            };
            user_sheets_client(http, user, base_url.as_deref())
        }
        (Some(_), None) => {
            return Err(LoaderError::Invalid(format!(
                "connection {:?} sets client_id without client_secret; set both, from the OAuth client's JSON file",
                source.connection
            )));
        }
        (None, _) if client_secret.is_some() || refresh_token.is_some() => {
            return Err(LoaderError::Invalid(format!(
                "connection {:?} sets client_secret or refresh_token without client_id; set client_id to read as a Google user",
                source.connection
            )));
        }
        (None, _) => {
            let key = match (
                service_account_json,
                service_account_json_base64,
                service_account_file,
            ) {
                (Some(json), _, _) => KeySource::Json(json.clone()),
                (None, Some(encoded), _) => KeySource::Base64(encoded.clone()),
                (None, None, Some(path)) => KeySource::File(path.clone()),
                (None, None, None) => KeySource::Environment,
            };
            sheets_client(http, &key, base_url.as_deref())?
        }
    };
    let spreadsheet_id = source.spreadsheet_id.clone().ok_or_else(|| {
        LoaderError::Invalid("a google_sheets source needs a spreadsheet_id".to_owned())
    })?;
    Ok((client, spreadsheet_id))
}

/// Where table `name` of a `google_sheets` source has its header: the table's `header` and `header_row`, else the source's, else a header in the first row.
pub(crate) fn sheet_rule(source: &SourceConfig, name: &str) -> HeaderRule {
    let table = source.tables.get(name).map(|table| &table.settings);
    let pick = |value: fn(&TableSettings) -> Option<u32>| {
        table.and_then(value).or(value(&source.defaults))
    };
    HeaderRule {
        header: pick(|settings| settings.header.map(u32::from)).is_none_or(|header| header == 1),
        header_row: pick(|settings| settings.header_row).unwrap_or(1),
        column_names: table
            .and_then(|settings| settings.column_names.clone())
            .or_else(|| source.defaults.column_names.clone()),
        width: source.ranges.get(name).and_then(|range| a1_width(range)),
    }
}

/// A `google_sheets` source's tables: each of its `ranges`, or each tab named after it when it names none (an empty tab is left out with a warning); with the warnings about how cells were typed, or every header cell that cannot name a column.
async fn sheets_tables(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<(Vec<SourceTable>, Vec<String>), LoaderError> {
    let (client, spreadsheet_id) = sheets_reader(project, source)?;
    let mut warnings: Vec<String> = Vec::new();
    let mut named: Vec<(String, Grid)> = Vec::new();
    match source.ranges.is_empty() {
        true => {
            let (_, grids) = read_grids(&client, &spreadsheet_id, None).await?;
            for grid in grids {
                match grid.rows.is_empty() {
                    true => warnings.push(format!(
                        "tab {:?} is empty, so it loads no table",
                        grid.title
                    )),
                    false => named.push((tab_name(&grid.title), grid)),
                }
            }
        }
        false => {
            for (name, range) in &source.ranges {
                let (_, grids) = read_grids(&client, &spreadsheet_id, Some(range)).await?;
                let grid = grids
                    .into_iter()
                    .next()
                    .ok_or_else(|| LoaderError::Invalid(format!("range {range:?} read no tab")))?;
                named.push((name.clone(), grid));
            }
        }
    }
    let mut problems: Vec<String> = Vec::new();
    let mut tables: Vec<SourceTable> = Vec::with_capacity(named.len());
    for (name, grid) in named {
        if tables.iter().any(|table| table.name == name) {
            problems.push(format!(
                "tab {:?} loads as table {name}, as an earlier tab does; rename one, or name the tabs in ranges",
                grid.title
            ));
            continue;
        }
        if let Some(transform) = runnable_transform(project, source, &name) {
            match transformed_sheet(transform, (&spreadsheet_id, source), (&name, &grid)).await {
                Ok(table) => tables.push(table),
                Err(problem) => problems.push(problem),
            }
            continue;
        }
        let mut read = match sheet_table(&grid, &sheet_rule(source, &name)) {
            Ok(read) => read,
            Err(found) => {
                problems.extend(found);
                continue;
            }
        };
        let settings = source.tables.get(&name).map(|table| &table.settings);
        let key = settings
            .and_then(|settings| settings.unique_key.as_ref())
            .or(source.defaults.unique_key.as_ref())
            .filter(|key| !key.contains(&PRIMARY_KEY_PLACEHOLDER.to_owned()));
        let merge = settings
            .and_then(|settings| settings.write_strategy)
            .or(source.defaults.write_strategy)
            == Some(WriteStrategy::Merge);
        let fails = settings
            .and_then(|settings| settings.on_duplicate_keys)
            .or(source.defaults.on_duplicate_keys)
            == Some(OnDuplicateKeys::Fail);
        let duplicate = key.filter(|_| merge && fails).and_then(|key| {
            let problem = duplicate_key(&grid, &sheet_rule(source, &name), &read, key)?;
            Some((problem, key.first()?.clone()))
        });
        if let Some((problem, first)) = duplicate
            && let Some(column) = read
                .columns
                .iter_mut()
                .find(|column| column.name.eq_ignore_ascii_case(&first))
        {
            column.column_type = Err(problem);
        }
        warnings.extend(read.warnings);
        tables.push(SourceTable {
            split_pages: None,
            schema: String::new(),
            name,
            columns: read
                .columns
                .into_iter()
                .map(|column| SourceColumn {
                    name: column.name,
                    postgres_type: match column.has_values {
                        true => SHEET_COLUMN_KIND,
                        false => EMPTY_SHEET_COLUMN_KIND,
                    }
                    .to_owned(),
                    pg_type: PgType::Other,
                    column_type: column.column_type,
                    nullable: true,
                    source_spelling: None,
                })
                .collect(),
            primary_key: Vec::new(),
            indexed_columns: Vec::new(),
            estimated_rows: read.rows as i64,
            estimated_bytes: 0,
            comment: None,
            listed_keys: Vec::new(),
        });
    }
    match problems.is_empty() {
        true => Ok((tables, warnings)),
        false => Err(LoaderError::Invalid(problems.join("; "))),
    }
}

/// Table `name` of a `google_sheets` source with a `transform`: the columns the function returned for the range `grid` read now, typed as a Python table's yields are, or why it failed.
async fn transformed_sheet(
    (host, function): (Arc<dyn PythonHost>, String),
    (spreadsheet_id, source): (&str, &SourceConfig),
    (name, grid): (&str, &Grid),
) -> Result<SourceTable, String> {
    let input = sheet_input(grid, &sheet_rule(source, name))?;
    let call = TransformCall {
        function,
        context: range_context(name, spreadsheet_id, (grid, source.ranges.get(name))),
        masked: Vec::new(),
        arrays: Vec::new(),
        naming: (Naming::default(), None),
    };
    let (batches, _) = transform_range(host, call, input).await?;
    let columns: Vec<SourceColumn> = returned_columns(&batches)
        .into_iter()
        .map(|(name, column_type)| SourceColumn {
            name,
            postgres_type: SHEET_COLUMN_KIND.to_owned(),
            pg_type: PgType::Other,
            column_type: Ok(column_type),
            nullable: true,
            source_spelling: None,
        })
        .collect();
    Ok(SourceTable {
        estimated_rows: batches.iter().map(RecordBatch::num_rows).sum::<usize>() as i64,
        estimated_bytes: 0,
        ..SourceTable::of_columns(name.to_owned(), columns)
    })
}

/// The `ctx` of table `table`'s `transform` for the range `grid` read (`range` as configured, else its whole tab).
fn range_context(
    table: &str,
    spreadsheet_id: &str,
    (grid, range): (&Grid, Option<&String>),
) -> TransformContext {
    TransformContext {
        unit: "range",
        table: table.to_owned(),
        sheet: Some(grid.title.clone()),
        range: range.cloned(),
        spreadsheet_id: Some(spreadsheet_id.to_owned()),
        ..TransformContext::default()
    }
}

/// A sheet table's spec with each column typed as the live table has it when its cells fit that type (see [`fitting_type`]; dates fit a timestamp column, at midnight), and each column no cell gave a value typed as the live table has it, so a column keeps its type as its values change or empty; a column holding an error cell keeps its error. `None` when no column's type changes.
pub(crate) fn with_live_types(spec: &TableSpec, live: Option<&[LiveColumn]>) -> Option<TableSpec> {
    spec.sheet.as_ref()?;
    let live = live?;
    let mut typed = spec.clone();
    let mut changed = false;
    for column in &mut typed.source_table.columns {
        let Some(live_type) = live
            .iter()
            .find(|live| live.name.eq_ignore_ascii_case(&column.name))
            .map(|live| live.column_type.clone())
        else {
            continue;
        };
        let Ok(inferred) = &column.column_type else {
            continue;
        };
        let column_type = match (column.postgres_type.as_str(), &live_type, inferred) {
            (EMPTY_SHEET_COLUMN_KIND, _, _) | (_, ColumnType::TimestampNtz, ColumnType::Date) => {
                live_type
            }
            _ => fitting_type(inferred.clone(), Some(live_type)),
        };
        changed |= *inferred != column_type;
        column.column_type = Ok(column_type);
    }
    changed.then_some(typed)
}

/// A `google_sheets` table's rows as they are now, in its read columns' types, named as they load; with a `transform`, what it returned for them (its time noted in `run`).
async fn sheet_rows(
    (client, spreadsheet_id): (&SheetsClient, &str),
    table: &TablePlan,
    run: Option<&TransformRun>,
) -> Result<Vec<RecordBatch>, LoaderError> {
    let name = &table.spec.source_table.name;
    let read = table
        .spec
        .sheet
        .as_ref()
        .ok_or_else(|| LoaderError::Invalid(format!("{name} is not a sheet's table")))?;
    let range = match &read.range {
        Some(range) => range.clone(),
        None => {
            let (_, tabs) = tab_ranges(client, spreadsheet_id).await?;
            tabs.into_iter()
                .find(|(title, _)| tab_name(title) == *name)
                .map(|(_, range)| range)
                .ok_or_else(|| {
                    LoaderError::Invalid(format!(
                        "the spreadsheet has no tab for table {name} any more; run plan again"
                    ))
                })?
        }
    };
    let (_, grids) = read_grids(client, spreadsheet_id, Some(&range)).await?;
    let grid = grids
        .into_iter()
        .next()
        .ok_or_else(|| LoaderError::Invalid(format!("range {range:?} read no tab")))?;
    if table.spec.transform.is_some() {
        let (mut call, run) = transform_call(&table.spec, run)?;
        call.context = range_context(name, spreadsheet_id, (&grid, read.range.as_ref()));
        let input = sheet_input(&grid, &read.rule).map_err(LoaderError::Invalid)?;
        let (batches, time) = transform_range(run.host.clone(), call, input)
            .await
            .map_err(LoaderError::Invalid)?;
        let mut typed: Vec<RecordBatch> = Vec::with_capacity(batches.len());
        for batch in &batches {
            unplanned_column(batch, &table.spec, &time.file).map_err(LoaderError::Invalid)?;
            let loaded =
                loaded_batch(batch, &table.spec.source_table).map_err(LoaderError::Invalid)?;
            typed.push(read_types(&loaded, &read_schema(table))?);
        }
        run.times
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(time);
        return Ok(typed);
    }
    let columns: Vec<(String, ColumnType)> = table
        .read_columns
        .iter()
        .map(|column| {
            (
                table.spec.source_table.spelling(&column.name).to_owned(),
                column.output.clone(),
            )
        })
        .collect();
    let batch = sheet_batch(&grid, &read.rule, &columns).map_err(LoaderError::Invalid)?;
    Ok(vec![
        loaded_batch(&batch, &table.spec.source_table).map_err(LoaderError::Invalid)?,
    ])
}

/// A warning when source `source_name` reads `address` over the network in plain text because the server refused TLS under `sslmode = "prefer"`; none for a loopback or Unix socket host, whose traffic stays on the machine, or for an explicit `disable`.
fn plain_text_warning(
    (source_name, connection): (&str, &str),
    address: &PostgresAddress<'_>,
    encryption: Encryption,
) -> Option<String> {
    let local = address.host == LOCALHOST
        || address.host.starts_with('/')
        || address
            .host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    (encryption == Encryption::None && address.tls.mode == SslMode::Prefer && !local).then(|| {
        format!(
            "[sources.{source_name}] reads {}:{} in plain text: the server does not offer TLS, so sslmode = \"prefer\" fell back; set sslmode = \"require\" (or \"verify-full\") on connection {connection} to refuse plain text, or \"disable\" to accept it",
            address.host, address.port
        )
    })
}

/// The session a `snowflake` source reads in: its connection's account, credentials, role and database, on the source's `warehouse` or else the connection's.
fn snowflake_session(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<DestinationConfig, LoaderError> {
    let Some(ConnectionConfig::Snowflake {
        account,
        user,
        auth,
        role,
        warehouse,
        database,
        base_url,
        ..
    }) = project.connections.get(&source.connection)
    else {
        return Err(LoaderError::Invalid(format!(
            "connection {:?} is not a snowflake connection",
            source.connection
        )));
    };
    Ok(DestinationConfig {
        kind: DestinationKind::Snowflake,
        account: account.clone(),
        user: user.clone(),
        auth: auth.clone(),
        role: role.clone(),
        warehouse: source.warehouse.clone().or_else(|| warehouse.clone()),
        database: database.clone(),
        schema: SESSION_SCHEMA.to_owned(),
        base_url: base_url.clone(),
    })
}

/// A files source as one source table (file columns, partition columns, the source-file column), and its files: under `--reload` with `after_load.move_to`, those it moved there too.
pub(crate) async fn discover_files_table(
    (project, options): (&ProjectConfig, &PlanOptions),
    (source_name, source): (&str, &SourceConfig),
    (warehouse, naming): (Warehouse, Naming),
) -> Result<(SourceTable, FilesSource), LoaderError> {
    let location = files_location(project, source)?;
    let folder = open_folder(&location)?;
    let (values, _) = path_values((source_name, source), warehouse)?;
    let encoding = FilesEncoding {
        flatten_objects: naming == Naming::Dlt,
        ..source.files_encoding()
    };
    let archive = match &source.after_load {
        AfterLoad::MoveTo(move_to) if options.reload => Some(OpenFolder {
            store: folder.store.clone(),
            location: Location {
                store: location.store.clone(),
                prefix: move_to_prefix(&location, move_to).map_err(LoaderError::Invalid)?,
            },
        }),
        _ => None,
    };
    let table = match &archive {
        Some(archive) => discover_reload_files((&folder, archive), &values, &encoding).await?,
        None => discover_files(&folder, &values, &encoding).await?,
    };
    if let Some(problem) = table.problems.first() {
        return Err(LoaderError::Invalid(problem.clone()));
    }
    let overrides = source.tables.get(source_name);
    let (table, transform) = match runnable_transform(project, source, source_name) {
        Some((host, function)) => {
            let own = |pick: fn(&TableOverride) -> &[String]| overrides.map_or(&[][..], pick);
            let patterns = (
                [
                    &source.exclude_columns[..],
                    own(|table| &table.exclude_columns),
                ]
                .concat(),
                [&source.allow_columns[..], own(|table| &table.allow_columns)].concat(),
            );
            let input = transform_input(&table, encoding.format, patterns)?;
            let call = TransformCall {
                function,
                context: TransformContext {
                    unit: "file",
                    table: source_name.to_owned(),
                    ..TransformContext::default()
                },
                masked: Vec::new(),
                arrays: Vec::new(),
                naming: (naming, sql_adapter(warehouse).max_identifier_length()),
            };
            let (table, input) =
                transform_sample((&folder, host, call), (input, &encoding), table).await?;
            (table, Some(input))
        }
        None => (table, None),
    };
    let (table, files) =
        files_source_parts(source_name, source, (warehouse, naming), (location, table))?;
    Ok((
        table,
        FilesSource {
            transform,
            archive: archive.map(|archive| archive.location),
            ..files
        },
    ))
}

/// `table` with the columns `call`'s function returned for its newest file (the first sampling reads), typed as a Python table's yields are (a column only ever NULL as text), and `input` with that file as its sample.
async fn transform_sample(
    (folder, host, call): (&OpenFolder, Arc<dyn PythonHost>, TransformCall),
    (input, encoding): (FilesTransform, &FilesEncoding),
    table: FilesTable,
) -> Result<(FilesTable, FilesTransform), LoaderError> {
    let Some(newest) = table.files.last() else {
        return Ok((table, input));
    };
    let request = TransformFiles {
        host,
        call,
        files: std::slice::from_ref(newest),
        location: folder.location.to_string(),
        encoding,
    };
    let mut returned: Vec<RecordBatch> = Vec::new();
    transform_files(
        &request,
        async |data| {
            Ok::<_, LoaderError>(file_rows(folder, &input_read(&input, encoding, data)).await?)
        },
        async |_, batch| {
            returned.push(batch);
            Ok(())
        },
    )
    .await?;
    let columns: Vec<FileColumn> = returned_columns(&returned)
        .into_iter()
        .map(|(name, column_type)| FileColumn {
            name,
            column_type: Ok(column_type),
            direct_refusal: None,
            stored_read: StoredRead::Plain,
            lists: None,
        })
        .collect();
    let sample = format!("{}{}", folder.location, newest.file.relative_key);
    Ok((
        FilesTable {
            columns,
            objects: Vec::new(),
            ..table
        },
        FilesTransform { sample, ..input },
    ))
}

/// The read of one file `data` for a `transform`: the file alone, in `input`'s columns.
fn input_read(input: &FilesTransform, encoding: &FilesEncoding, data: &DataFile) -> FilesRead {
    FilesRead {
        files: vec![data.clone()],
        encoding: encoding.clone(),
        checks: input.checks.clone(),
        columns: input.input.clone(),
        nanos: input.nanos.clone(),
        batch_rows: SOURCE_BATCH_ROWS,
    }
}

/// A files source's table and files from the files found: the file columns, partition columns and source-file columns as one source table.
pub(crate) fn files_source_parts(
    source_name: &str,
    source: &SourceConfig,
    (warehouse, naming): (Warehouse, Naming),
    (location, table): (Location, FilesTable),
) -> Result<(SourceTable, FilesSource), LoaderError> {
    let encoding = FilesEncoding {
        flatten_objects: naming == Naming::Dlt,
        ..source.files_encoding()
    };
    let (values, partition_types) = path_values((source_name, source), warehouse)?;
    let added: Vec<&str> = partition_types
        .iter()
        .map(|(name, _)| name.as_str())
        .chain([SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN, SOURCE_ROW_COLUMN])
        .collect();
    let overrides = source.tables.get(source_name);
    let rule = (naming, sql_adapter(warehouse).max_identifier_length());
    let pinned_only = pinned_unsampled(overrides, (&table, &added, rule), encoding.format)?;
    if encoding.flatten_objects {
        let problems =
            flattened_object_problems((source, overrides), (&table.columns, &table.objects), rule)
                .map_err(LoaderError::Invalid)?;
        if !problems.is_empty() {
            return Err(LoaderError::Invalid(problems.join("; ")));
        }
    }
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
            (SOURCE_ROW_COLUMN.to_owned(), Ok(ColumnType::Int)),
        ]);
    let columns: Vec<SourceColumn> = typed
        .map(|(name, column_type)| SourceColumn {
            name,
            postgres_type: encoding.format.to_string(),
            pg_type: PgType::Other,
            column_type,
            nullable: true,
            source_spelling: None,
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
        estimated_bytes: table
            .files
            .iter()
            .map(|data| i64::try_from(data.file.size).unwrap_or(i64::MAX))
            .sum(),
        ..SourceTable::of_columns(source_name.to_owned(), columns)
    };
    Ok((
        source_table,
        FilesSource {
            location,
            table,
            partition_types,
            constants: values.constants.into_iter().map(|(name, _)| name).collect(),
            encoding,
            checks,
            notes,
            copy_to_internal_stage: source.copy_to_internal_stage,
            transform: None,
            archive: None,
        },
    ))
}

/// What a files source's load reads: the files in the cursor window that are not loaded yet.
pub(crate) struct FilesPlanRequest<'a> {
    pub(crate) spec: &'a TableSpec,
    pub(crate) files: &'a FilesSource,
    pub(crate) loaded: LoadedSource<'a>,
    pub(crate) live: Option<&'a [LiveColumn]>,
    pub(crate) read_columns: &'a [ReadColumn],
    pub(crate) written: &'a [String],
    pub(crate) bounds: (Option<&'a str>, Option<&'a str>),
    pub(crate) options: &'a PlanOptions,
}

/// Where an `append` finds the source files already loaded.
#[derive(Debug, Clone, Copy)]
pub(crate) enum LoadedSource<'a> {
    /// The warehouse table's `_sqlload_source_file` values.
    Query(&'a Destination),
    /// A files target's records of what its loads loaded.
    Recorded(&'a [LoadedFile]),
}

/// Whether the live table has `column`.
pub(crate) fn live_has(live: Option<&[LiveColumn]>, column: &str) -> bool {
    match live {
        Some(live) => live
            .iter()
            .any(|existing| existing.name.eq_ignore_ascii_case(column)),
        None => false,
    }
}

/// How to give a table with rows a column a load needs, on `warehouse`.
pub(crate) fn column_fix(warehouse: Warehouse) -> &'static str {
    match sql_adapter(warehouse).keeps_tables() {
        true => KEPT_TABLES_COLUMN_FIX,
        false => RELOAD_FIX,
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
            .find(|(name, _)| name == spec.source_table.spelling(&cursor.column))
        {
            Some((name, column_type)) => Some((name.as_str(), column_type)),
            None => {
                let instead = match spec.strategy {
                    WriteStrategy::DeleteInsert => format!(
                        "; to replace every row of each {0} value the files hold, set replace_key = [\"{0}\"] and drop cursor_column",
                        cursor.column
                    ),
                    _ => String::new(),
                };
                return Ok(Err(format!(
                    "cursor_column {} must be one of the partition_columns or path_columns for a files source{instead}",
                    cursor.column
                )));
            }
        },
    };
    if let Some((name, _)) = cursor
        && request
            .files
            .constants
            .iter()
            .any(|constant| constant == name)
    {
        return Ok(Err(format!(
            "cursor_column {name} is a constant column, the same in every file, so it cannot pick the files to read; use a partition_columns or path_columns column"
        )));
    }
    if let AfterLoad::MoveTo(move_to) = &spec.after_load
        && let Err(problem) = move_to_prefix(&request.files.location, move_to)
    {
        return Ok(Err(problem));
    }
    if spec.after_load == AfterLoad::Delete && request.options.reload {
        return Ok(Err(
            "--reload would rebuild the table from the files still in its folder, and after_load has deleted or moved the ones already loaded".to_owned(),
        ));
    }
    let merged = match logs_files(spec) {
        true => match merge_ledger(request).await? {
            Ok(merged) => Some(merged),
            Err(problem) => return Ok(Err(problem)),
        },
        false => None,
    };
    let skips_loaded = spec.strategy == WriteStrategy::Append;
    let guards_window = spec.strategy == WriteStrategy::DeleteInsert
        && cursor.is_some()
        && request.options.start_cursor.is_none()
        && !request.options.reload;
    if let Some(problem) = file_column_problem(request, guards_window) {
        return Ok(Err(problem));
    }
    let live_has = |column: &str| live_has(request.live, column);
    let recorded: Vec<LoadedFile> = match (skips_loaded || guards_window)
        && live_has(SOURCE_FILE_COLUMN)
        && !request.options.reload
    {
        true => match request.loaded {
            LoadedSource::Query(destination) => {
                let version = live_has(SOURCE_VERSION_COLUMN).then_some(SOURCE_VERSION_COLUMN);
                let query = sql_adapter(spec.warehouse).render_loaded_files_query(
                    &spec.destination,
                    SOURCE_FILE_COLUMN,
                    version,
                );
                text_values(destination, query)
                    .await?
                    .into_iter()
                    .map(|text| loaded_file(text, version.is_some()))
                    .collect()
            }
            LoadedSource::Recorded(recorded) => recorded.to_vec(),
        },
        false => Vec::new(),
    };
    let (loaded, held) = match skips_loaded {
        true => (recorded, Vec::new()),
        false => (Vec::new(), recorded),
    };
    let loaded = merged
        .as_ref()
        .map_or(loaded, |(unchanged, _)| unchanged.clone());
    let location = request.files.location.to_string();
    let selection = FileSelection {
        files: &request.files.table.files,
        location: &location,
        loaded: &loaded,
        removed: spec.after_load != AfterLoad::Keep,
        held: &held,
        cursor,
        from: request.bounds.0,
        to: request.bounds.1,
    };
    let (columns, inferred) =
        files_read_as(spec, request.read_columns, &request.files.partition_types);
    let checks = TextChecks {
        inferred,
        ..request.files.checks.clone()
    };
    let nanos = nanosecond_columns(spec, &request.files.table.columns, request.live);
    let ordered = select_files(&selection).map(|files| match &merged {
        Some((_, recorded)) if request.options.reload => {
            reload_order(files, &location, recorded, request.files.archive.as_ref())
        }
        Some(_) => merge_order(files),
        None => files,
    });
    Ok(ordered.map(|files| FilesRead {
        files,
        encoding: request.files.encoding.clone(),
        checks,
        columns,
        nanos,
        batch_rows: SOURCE_BATCH_ROWS,
    }))
}

/// The stored `files` that `after_load` settles once their rows commit: under a `--reload` that lists the `move_to` folder too, those read from it stay where they are.
pub(crate) fn unarchived(table: &TablePlan, files: &[DataFile]) -> Vec<StoredFile> {
    let archive = table
        .spec
        .files
        .as_ref()
        .and_then(|files| files.archive.as_ref());
    files
        .iter()
        .filter(|data| {
            archive.is_none_or(|archive| !data.file.path.as_ref().starts_with(&archive.prefix))
        })
        .map(|data| data.file.clone())
        .collect()
}

/// One file's rows as `read` (of that file alone) reads them, whole, and their schema: what a `transform` gets.
pub(crate) async fn file_rows(
    folder: &OpenFolder,
    read: &FilesRead,
) -> Result<(Vec<RecordBatch>, SchemaRef), FilesError> {
    let mut rows: Vec<RecordBatch> = Vec::new();
    read_files(folder, read, &BatchFooters::default(), async |batch| {
        rows.push(batch);
        Ok(())
    })
    .await?;
    let fields = read.columns.iter().map(|column| {
        arrow_field(&column.name, &column.column_type)
            .unwrap_or_else(|| Field::new(&column.name, DataType::Utf8, true))
    });
    let schema = rows.first().map_or_else(
        || Arc::new(Schema::new(fields.collect::<Vec<Field>>())),
        RecordBatch::schema,
    );
    Ok((rows, schema))
}

/// Which part of a table's planned read one pass reads: a cursor window's filter, an order, or some of a files source's files.
#[derive(Debug, Clone, Default)]
pub(crate) struct ReadPart {
    /// The Postgres filter; `None` reads the plan's.
    pub(crate) filter: Option<String>,
    /// The values bound to `filter`'s `?` placeholders, in order.
    #[cfg_attr(
        not(feature = "odbc"),
        expect(dead_code, reason = "only ODBC sources bind parameters")
    )]
    pub(crate) parameters: Vec<BoundParameter>,
    /// `ORDER BY` this quoted column.
    pub(crate) order_by: Option<String>,
    /// The files to read; `None` reads every planned file.
    pub(crate) files: Option<Vec<DataFile>>,
    /// The Parquet footers already read for `files`.
    pub(crate) footers: BatchFooters,
    /// A smaller byte cap for the source's batches than the usual 64 MiB (the hidden `--batch-bytes`, for tests).
    pub(crate) batch_bytes: Option<usize>,
    /// What runs the table's `transform`; `None` without a Python host.
    pub(crate) transform: Option<TransformRun>,
}

/// Read `part` of `table`'s rows from `reader`, handing each batch to `on_batch`; when `on_batch` fails, the read fails with the reader's text, and stays retryable when `on_batch`'s error was.
pub(crate) async fn read_source(
    reader: &SourceReader,
    table: &TablePlan,
    part: ReadPart,
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), LoaderError>,
) -> Result<u64, LoaderError> {
    let mut failed: Option<LoaderError> = None;
    let read = Box::pin(read_each(reader, table, part, async |batch| {
        on_batch(batch).await.map_err(|error| {
            let text = error.to_string();
            failed = Some(error);
            text
        })
    }))
    .await;
    match (read, failed) {
        (Err(read), Some(failed)) if failed.is_retryable() => {
            Err(LoaderError::Transient(read.to_string()))
        }
        (read, _) => read,
    }
}

/// Read `part` of `table`'s data from `reader`, passing each batch to `on_batch`; returns the row count.
async fn read_each(
    reader: &SourceReader,
    table: &TablePlan,
    part: ReadPart,
    on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, LoaderError> {
    match (reader, &table.files) {
        (SourceReader::Postgres(client, helpers), _) => {
            let whole = part.filter.is_none() && part.order_by.is_none();
            let request = CopyRequest {
                table: &table.spec.source_table,
                columns: table.read_columns.clone(),
                filter: part.filter.or_else(|| table.source_filter.clone()),
                order_by: part.order_by,
                batch_rows: SOURCE_BATCH_ROWS,
                batch_bytes: part
                    .batch_bytes
                    .map_or(DEFAULT_BATCH_BYTES, |bytes| bytes.min(DEFAULT_BATCH_BYTES)),
                helpers: match whole {
                    true => helpers,
                    false => &[],
                },
            };
            Ok(copy_table(client, request, on_batch).await?)
        }
        (SourceReader::Files(folder), Some(files)) => {
            let read = match part.files.clone() {
                Some(subset) => FilesRead {
                    files: subset,
                    ..files.clone()
                },
                None => files.clone(),
            };
            let mut on_batch = on_batch;
            if let Some(transform) = table
                .spec
                .files
                .as_ref()
                .and_then(|files| files.transform.as_ref())
            {
                let run = part.transform.as_ref();
                return read_transformed(folder, (&read, transform), (table, run), on_batch).await;
            }
            let loaded = async |batch: RecordBatch| {
                on_batch(loaded_batch(&batch, &table.spec.source_table)?).await
            };
            Ok(read_files(folder, &read, &part.footers, loaded).await?)
        }
        (SourceReader::Snowflake(snowflake), _) => {
            let filter = part.filter.or_else(|| table.source_filter.clone());
            let request = unload_request(table, filter.as_deref());
            Ok(unload_rows(snowflake, &request, on_batch).await?)
        }
        #[cfg(feature = "odbc")]
        (SourceReader::Odbc(reader), _) => {
            let (filter, parameters) = match part.filter {
                Some(filter) => (Some(filter), &part.parameters[..]),
                None => (table.source_filter.clone(), &table.source_parameters[..]),
            };
            let request = crate::odbc_source::models::OdbcReadRequest {
                table: &table.spec.source_table,
                sql: table
                    .spec
                    .warehouse_source
                    .as_ref()
                    .and_then(|source| source.sql.as_deref()),
                columns: &table.read_columns,
                filter: filter.as_deref(),
                parameters,
                batch_rows: SOURCE_BATCH_ROWS,
                memory_budget: table.fetch_memory.as_ref().map_or(
                    crate::odbc_source::constants::DEFAULT_BATCH_MEMORY,
                    |fetch| fetch.budget,
                ),
            };
            Ok(crate::odbc_source::main::read_rows::read_rows(reader, &request, on_batch).await?)
        }
        (SourceReader::BigQuery(reader), _) => {
            let filter = part.filter.or_else(|| table.source_filter.clone());
            let request = BigQueryReadRequest {
                table: &table.spec.source_table,
                sql: table
                    .spec
                    .warehouse_source
                    .as_ref()
                    .and_then(|source| source.sql.as_deref()),
                columns: &table.read_columns,
                filter: filter.as_deref(),
            };
            Ok(read_bigquery_rows(reader, &request, on_batch).await?)
        }
        (SourceReader::Sheets(client, spreadsheet_id), _) => {
            let batches =
                sheet_rows((client, spreadsheet_id), table, part.transform.as_ref()).await?;
            let mut rows: u64 = 0;
            let mut on_batch = on_batch;
            for batch in batches {
                rows += batch.num_rows() as u64;
                on_batch(batch).await.map_err(LoaderError::Invalid)?;
            }
            Ok(rows)
        }
        (SourceReader::Files(_), None) => Err(LoaderError::Invalid(format!(
            "{}: a files source has no files planned",
            table.spec.source_table.qualified_name()
        ))),
    }
}

/// Read each of `read`'s files in order through `table`'s `transform`, passing what it returned for the file, shaped as the file's rows, to `on_batch`; a file whose function fails, or returns a column the plan did not see, fails the read there, naming the file. Returns the row count.
async fn read_transformed(
    folder: &OpenFolder,
    (read, input): (&FilesRead, &FilesTransform),
    (table, run): (&TablePlan, Option<&TransformRun>),
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, LoaderError> {
    let spec = &table.spec;
    let (call, run) = transform_call(spec, run)?;
    let request = TransformFiles {
        host: run.host.clone(),
        call,
        files: &read.files,
        location: folder.location.to_string(),
        encoding: &read.encoding,
    };
    let mut rows: u64 = 0;
    let times = transform_files(
        &request,
        async |data| {
            Ok::<_, LoaderError>(file_rows(folder, &input_read(input, &read.encoding, data)).await?)
        },
        async |(data, file_name, before), batch| {
            let failed = |reason: String| LoaderError::Invalid(format!("{file_name}: {reason}"));
            unplanned_column(&batch, spec, &input.sample).map_err(failed)?;
            let shaped = shape_rows(&batch, (data, file_name, before), read).map_err(failed)?;
            rows += shaped.num_rows() as u64;
            on_batch(loaded_batch(&shaped, &spec.source_table).map_err(failed)?)
                .await
                .map_err(LoaderError::Invalid)
        },
    )
    .await?;
    run.times
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .extend(times);
    Ok(rows)
}

/// The call of `spec`'s `transform`, before each file or range sets its context, and what runs it; why it cannot run without a Python host.
fn transform_call<'a>(
    spec: &TableSpec,
    run: Option<&'a TransformRun>,
) -> Result<(TransformCall, &'a TransformRun), LoaderError> {
    let (Some(run), Some(transform)) = (run, spec.transform.as_ref()) else {
        return Err(LoaderError::Invalid(NO_TRANSFORM_HOST.replace(
            "{place}",
            &format!("table {}", spec.source_table.qualified_name()),
        )));
    };
    let call = TransformCall {
        function: transform.function.clone(),
        context: TransformContext {
            unit: "file",
            table: spec.source_table.name.clone(),
            ..TransformContext::default()
        },
        masked: spec.masked.columns.keys().cloned().collect(),
        arrays: spec
            .pinned
            .iter()
            .filter(|(_, column_type)| matches!(column_type, ColumnType::Array(_)))
            .map(|(name, _)| name.clone())
            .collect(),
        naming: (
            spec.naming,
            sql_adapter(spec.warehouse).max_identifier_length(),
        ),
    };
    Ok((call, run))
}

/// Why `batch`, returned by `spec`'s `transform`, cannot load: it has a column the function did not return for `sample` at plan (nor `columns` pins).
fn unplanned_column(batch: &RecordBatch, spec: &TableSpec, sample: &str) -> Result<(), String> {
    let planned: Vec<&str> = spec
        .source_table
        .columns
        .iter()
        .map(|column| spec.source_table.spelling(&column.name))
        .collect();
    match batch
        .schema()
        .fields()
        .iter()
        .find(|field| !planned.contains(&field.name().as_str()))
    {
        Some(extra) => Err(format!(
            "transform {} returned column {}, which it did not return for {sample} at plan; return the same columns every time, or pin {} in columns",
            spec.transform
                .as_ref()
                .map_or("", |transform| transform.function.as_str()),
            extra.name(),
            extra.name()
        )),
        None => Ok(()),
    }
}

/// Unload `part` of `table`'s data from `reader` and pass the files to `load`, which has the destination read them itself; the files are removed afterwards.
pub(crate) async fn unload_source<T>(
    reader: &SourceReader,
    table: &TablePlan,
    part: ReadPart,
    load: impl AsyncFnOnce(&UnloadedFiles) -> Result<T, LoaderError>,
) -> Result<T, LoaderError> {
    let SourceReader::Snowflake(snowflake) = reader else {
        return Err(LoaderError::Invalid(format!(
            "{}: only a warehouse source unloads files",
            table.spec.source_table.qualified_name()
        )));
    };
    let filter = part.filter.or_else(|| table.source_filter.clone());
    unload_files(snowflake, &unload_request(table, filter.as_deref()), load).await
}

/// What one unload of `table` reads: its table or query, its read columns, and `filter`.
fn unload_request<'a>(table: &'a TablePlan, filter: Option<&'a str>) -> UnloadRequest<'a> {
    UnloadRequest {
        table: &table.spec.source_table,
        sql: table
            .spec
            .warehouse_source
            .as_ref()
            .and_then(|source| source.sql.as_deref()),
        columns: &table.read_columns,
        filter,
        batch_rows: SOURCE_BATCH_ROWS,
    }
}

/// `specs` with `source_end` filled for the selected Postgres tables whose integer `batch_size` has no `--end-cursor` to end its windows: the cursor's current maximum, read once at plan; the source is connected only when one needs it.
pub(crate) async fn source_ends(
    project: &ProjectConfig,
    (source_name, source): (&str, &SourceConfig),
    mut specs: Vec<TableSpec>,
    options: &PlanOptions,
) -> Result<Vec<TableSpec>, LoaderError> {
    if source.kind != SourceKind::Postgres || options.end_cursor.is_some() {
        return Ok(specs);
    }
    let mut client: Option<Client> = None;
    for spec in &mut specs {
        let integer_cursor = integer_cursor(spec);
        let wanted = matches!(spec.batch, Some(BatchStep::Integer(_)))
            && selected(options, (source_name, &spec_names(spec)));
        let Some(columns) = integer_cursor.filter(|_| wanted) else {
            continue;
        };
        if client.is_none() {
            client = Some(connect_source(project, source).await?.0);
        }
        let Some(connected) = &client else {
            continue;
        };
        spec.source_end = Some(
            match cursor_end(connected, &spec.source_table, &columns).await? {
                Some((maximum, end)) => SourceEnd::Maximum { maximum, end },
                None => SourceEnd::Empty,
            },
        );
    }
    Ok(specs)
}

/// Whether `--select` picks a table and `--exclude` does not, each by source name or one of the table's `names` (its destination name, the destination's display name, or its source's `schema.table`; empty ones match nothing); no `--select` picks every table.
pub(crate) fn selected(options: &PlanOptions, (source, names): (&str, &[String])) -> bool {
    let picks = |selector: &String| {
        selector == source
            || names
                .iter()
                .any(|name| !name.is_empty() && name == selector)
    };
    (options.select.is_empty() || options.select.iter().any(picks))
        && !options.exclude.iter().any(picks)
}

/// Source `source_name`'s tables resolved to their settings, with its files for a files source, or `Err` with its problem when it cannot list them (`None` when out of scope); its problems count only when the selection in `options` takes something from it (see [`source_in_scope`]), and are dropped otherwise.
pub(crate) async fn scoped_tables(
    (project, options): (&ProjectConfig, &PlanOptions),
    (source_name, source): (&str, &SourceConfig),
) -> Result<Result<(ResolvedTables, Option<FilesSource>), Option<SourceProblem>>, LoaderError> {
    let (discovered, files, warnings) =
        match source_tables((project, options), source_name, source).await? {
            Ok(found) => found,
            Err(problem) => {
                let in_scope = source_in_scope((project, options), (source_name, source), None);
                return Ok(Err(in_scope.then(|| SourceProblem {
                    message: format!("[sources.{source_name}]: {}", problem.message),
                    retryable: problem.retryable,
                })));
            }
        };
    if source.kind == SourceKind::Files && discovered.is_empty() {
        return Ok(Ok((
            drained_files((project, options), (source_name, source)),
            None,
        )));
    }
    let mut resolved = resolve_tables(project, source_name, &discovered);
    match source_in_scope((project, options), (source_name, source), Some(&discovered)) {
        true => resolved.warnings = warnings,
        false => resolved.problems.clear(),
    }
    Ok(Ok((resolved, files)))
}

/// A files source whose `after_load` emptied its folder, which a load leaves alone until new files come, as with `keep` every file in the folder is already loaded: its destination table, which [`found_sources`] checks exists before skipping it (when the selection in `options` takes it).
fn drained_files(
    (project, options): (&ProjectConfig, &PlanOptions),
    (source_name, source): (&str, &SourceConfig),
) -> ResolvedTables {
    let target = match source_target(project, source) {
        Ok(target) => target,
        Err(problem) => {
            return ResolvedTables {
                problems: vec![format!("[sources.{source_name}]: {problem}")],
                ..ResolvedTables::default()
            };
        }
    };
    let overrides = source.tables.get(source_name);
    let name = overrides.and_then(|table| table.name.as_deref());
    let unset = TableSettings::default();
    let naming = table_naming(
        (
            overrides.map_or(&unset, |overrides| &overrides.settings),
            &source.defaults,
        ),
        target.naming,
    );
    let rule = (
        naming,
        sql_adapter(target.warehouse).max_identifier_length(),
    );
    let listed =
        SourceTable::of_columns(source_name.to_owned(), referenced_columns(overrides, rule));
    let table = TableName {
        database: target.database,
        schema: target.schema,
        table: table_name((source_name, source), name, (&listed, rule)),
    };
    let shown = display_name(target.warehouse, &table);
    let in_scope = selected(
        options,
        (
            source_name,
            &[table.table.clone(), shown.clone(), source_name.to_owned()],
        ),
    );
    if target.path.is_some() {
        return ResolvedTables {
            problems: in_scope
                .then(|| {
                    format!("[sources.{source_name}]: after_load is for database targets so far, not a files target")
                })
                .into_iter()
                .collect(),
            ..ResolvedTables::default()
        };
    }
    let path = source.path.as_deref().unwrap_or_default();
    let spec = resolve_tables(project, source_name, std::slice::from_ref(&listed))
        .specs
        .pop();
    ResolvedTables {
        drained: Some(DrainedFiles {
            spec,
            in_scope,
            target: target.name,
            skipped: SkippedTable {
                source_name: source_name.to_owned(),
                source_table: source_name.to_owned(),
                destination: shown,
                reason: format!("no files to load in {path}{DRAINED_REASON}"),
            },
            problem: format!(
                "[sources.{source_name}]: no files to load in {path}, and {} was never loaded: after_load skips an emptied folder only once its table exists; check the source's path and connection",
                table.table
            ),
            table,
        }),
        ..ResolvedTables::default()
    }
}

/// Whether the selection in `options` takes anything from source `source_name`: a table it `listed`, or one named under its `tables` but not listed, by destination name or `schema.table`, or the source itself when it holds no table. A source that could not list its tables (`None`) counts when selected by name, or for any `--select` selector naming no source, since that table could be its. Only then do the source's problems count, so one source's bad settings do not fail a command run on another.
pub(crate) fn source_in_scope(
    (project, options): (&ProjectConfig, &PlanOptions),
    (source_name, source): (&str, &SourceConfig),
    listed: Option<&[SourceTable]>,
) -> bool {
    let Some(discovered) = listed else {
        return selected(options, (source_name, &[]))
            || (!options
                .exclude
                .iter()
                .any(|selector| selector == source_name)
                && options
                    .select
                    .iter()
                    .any(|selector| !project.sources.contains_key(selector)));
    };
    let found: Vec<String> = discovered.iter().map(SourceTable::qualified_name).collect();
    let target = source_target(project, source);
    let (target_naming, limit) = target.as_ref().map_or((None, None), |target| {
        (
            target.naming,
            sql_adapter(target.warehouse).max_identifier_length(),
        )
    });
    let names = |destination: String, qualified: String| {
        let shown = match (&target, destination.is_empty()) {
            (Ok(target), false) => target_display_name(target, destination.clone()),
            _ => String::new(),
        };
        [destination, shown, qualified]
    };
    let unset = TableSettings::default();
    let rule = |overrides: Option<&TableOverride>| {
        (
            table_naming(
                (
                    overrides.map_or(&unset, |overrides| &overrides.settings),
                    &source.defaults,
                ),
                target_naming,
            ),
            limit,
        )
    };
    let named = source
        .tables
        .iter()
        .filter(|(qualified, _)| !found.contains(qualified))
        .map(|(qualified, overrides)| {
            let destination = match overrides.name.as_deref() {
                Some(name) if !name.contains('{') => {
                    let (naming, limit) = rule(Some(overrides));
                    loaded_name(naming, name, limit)
                }
                _ => String::new(),
            };
            names(destination, qualified.clone())
        });
    let tables: Vec<[String; 3]> = discovered
        .iter()
        .map(|table| {
            let overrides = source.tables.get(&table.qualified_name());
            let name = overrides.and_then(|overrides| overrides.name.as_deref());
            names(
                table_name((source_name, source), name, (table, rule(overrides))),
                table.qualified_name(),
            )
        })
        .chain(named)
        .collect();
    (tables.is_empty() && selected(options, (source_name, &[])))
        || tables
            .iter()
            .any(|names| selected(options, (source_name, names)))
}

/// Every source's tables, resolved before any is planned so each knows which others share its destination table, with the folders they claim; a source whose tables cannot be read is a problem in the plan.
pub(crate) async fn found_sources<'a>(
    project: &'a ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
) -> Result<FoundSources<'a>, LoaderError> {
    let mut found = FoundSources::default();
    let mut drained_sources: Vec<(usize, DrainedFiles)> = Vec::new();
    for (source_name, source) in &project.sources {
        let (mut resolved, files) =
            match scoped_tables((project, options), (source_name, source)).await? {
                Ok(tables) => tables,
                Err(problem) => {
                    found.unresolved.push(source_name.clone());
                    if let Some(problem) = problem {
                        found.plan.unread_sources.insert(
                            source_name.clone(),
                            unread_source(project, (source_name, source)),
                        );
                        if problem.retryable {
                            found.plan.retryable_problems.push(problem.message.clone());
                        }
                        found.plan.failed_sources.insert(source_name.clone());
                        found.plan.problems.push(problem.message);
                    }
                    continue;
                }
            };
        if let Some(drained) = resolved.drained.take() {
            drained_sources.push((found.sources.len(), drained));
        }
        found
            .claims
            .extend(folder_claims(&resolved.specs, files.as_ref(), destinations));
        found.configured.extend(resolved.specs.iter().cloned());
        found.sources.push(FoundSource {
            name: source_name,
            source,
            resolved,
            files,
        });
    }
    let mut drained_specs: Vec<TableSpec> = Vec::new();
    for (index, drained) in drained_sources {
        let source = &mut found.sources[index];
        let shared = found
            .configured
            .iter()
            .any(|spec| spec.target == drained.target && spec.destination == drained.table);
        if shared {
            found.unresolved.push(source.name.clone());
        }
        found.drained.extend([
            drained.table.table.clone(),
            drained.skipped.destination.clone(),
            source.name.clone(),
        ]);
        drained_specs.extend(drained.spec);
        if !drained.in_scope {
            continue;
        }
        let exists = match destinations.get(&drained.target) {
            Some(destination) => live_columns(destination, &drained.table).await?.is_some(),
            None => false,
        };
        let path = source.source.path.as_deref().unwrap_or_default();
        let problem = match (exists, shared, options.reload) {
            (false, _, _) => Some(drained.problem),
            (true, true, _) => Some(format!(
                "[sources.{}]: no files to load in {path}, and other tables load into {} too: a load that cannot see this source could misread or remove its rows, so it fails until files come; run the others with --exclude {}",
                source.name, drained.table.table, source.name
            )),
            (true, false, true) if source.source.after_load == AfterLoad::Delete => Some(format!(
                "[sources.{}]: --reload would rebuild the table from the files still in its folder, and after_load has deleted or moved the ones already loaded",
                source.name
            )),
            (true, false, _) => None,
        };
        match problem {
            Some(problem) => source.resolved.problems.push(problem),
            None => source.resolved.skipped.push(drained.skipped),
        }
    }
    found.configured.extend(drained_specs);
    Ok(found)
}
