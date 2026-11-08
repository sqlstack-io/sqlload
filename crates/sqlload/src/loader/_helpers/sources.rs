use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use arrow_array::RecordBatch;
use std::collections::BTreeMap;
use tokio_postgres::Client;

use crate::adapters::dispatch::main::dialect::files_destination::files_destination;
use crate::adapters::dispatch::main::session::live_columns::live_columns;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::adapters::dispatch::models::Destination;
use crate::column_types::main::parse_type::parse_type;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::config::models::{DestinationConfig, DestinationKind};
use crate::file_store::main::open_folder::open_folder;
use crate::files_source::constants::{
    NANOSECOND_SCALE, SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN, SOURCE_VERSION_COLUMN,
    VERSION_SEPARATOR,
};
use crate::files_source::main::constant_columns::constant_columns;
use crate::files_source::main::discover_files::discover_files;
use crate::files_source::main::path_template::path_template;
use crate::files_source::main::read_files::read_files;
use crate::files_source::main::select_files::select_files;
use crate::files_source::models::{
    BatchFooters, DataFile, FileColumn, FileSelection, FilesRead, FilesTable, LoadedFile,
    PathValues, SourceFormat, StoredRead, TextChecks,
};
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;
use crate::loader::_helpers::python::{fitting_type, python_tables};
use crate::loader::_helpers::resolve::{resolve_tables, source_target};
use crate::loader::_helpers::specs::{missing_description, table_name};
use crate::loader::_helpers::table_plan::{blocked_plan, plan_table};
use crate::loader::constants::{
    KEPT_TABLES_COLUMN_FIX, LOCALHOST, PARALLEL_READ_MIN_BYTES, PARALLEL_READERS, RELOAD_FIX,
    SOURCE_BATCH_ROWS,
};
use crate::loader::errors::LoaderError;
use crate::loader::models::{
    CommitRule, FilesSource, PlanOptions, ProjectPlan, ResolvedTables, SkippedTable, SourceEnd,
    SourceReader, TablePlan, TableSpec,
};
use crate::postgres_source::constants::DEFAULT_BATCH_BYTES;
use crate::postgres_source::main::connect_postgres::connect_postgres;
use crate::postgres_source::main::copy_table::copy_table;
use crate::postgres_source::main::cursor_end::cursor_end;
use crate::postgres_source::main::read_catalog::read_catalog;
use crate::postgres_source::models::{
    CopyRequest, Encryption, PgType, PostgresAddress, ReadColumn, SourceColumn, SourceTable,
    SslMode, WireType,
};
use crate::project::main::postgres_address::postgres_address;
use crate::project::models::{
    BatchStep, ConnectionConfig, NewTables, ProjectConfig, SourceConfig, SourceKind, TableOverride,
    TableSettings, WriteStrategy,
};
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn, LiveColumn};
use crate::sheets_source::constants::{EMPTY_SHEET_COLUMN_KIND, SHEET_COLUMN_KIND};
use crate::sheets_source::main::duplicate_key::duplicate_key;
use crate::sheets_source::main::read_grids::read_grids;
use crate::sheets_source::main::sheet_batch::sheet_batch;
use crate::sheets_source::main::sheet_table::sheet_table;
use crate::sheets_source::main::sheets_client::sheets_client;
use crate::sheets_source::main::tab_name::tab_name;
use crate::sheets_source::main::tab_ranges::tab_ranges;
use crate::sheets_source::models::{Grid, HeaderRule, KeySource, SheetsClient};
use crate::similar_names::main::close_name::close_name;
use crate::similar_names::main::nearest_name::nearest_name;
use crate::snowflake_source::constants::SESSION_SCHEMA;
use crate::snowflake_source::main::connect_snowflake_source::connect_snowflake_source;
use crate::snowflake_source::main::read_catalog::read_catalog as read_snowflake_catalog;
use crate::snowflake_source::main::unload_rows::unload_rows;
use crate::snowflake_source::models::UnloadRequest;
use crate::store_paths::main::resolve_location::resolve_location;
use crate::store_paths::models::{Location, StoreKind};

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

/// The Postgres connections a table's read uses: several for a large Postgres table read whole and in no order (windows and cursor-ordered reads keep one connection).
pub(crate) fn source_readers(table: &TablePlan) -> usize {
    let whole = matches!(table.commits, CommitRule::Size | CommitRule::Whole(_));
    let postgres =
        table.spec.python.is_none() && table.spec.files.is_none() && table.spec.sheet.is_none();
    match whole && postgres && table.spec.source_table.estimated_bytes >= PARALLEL_READ_MIN_BYTES {
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
    project: &ProjectConfig,
    source_name: &str,
    source: &SourceConfig,
) -> Result<Result<(Vec<SourceTable>, Option<FilesSource>, Vec<String>), String>, LoaderError> {
    match source.kind {
        SourceKind::Postgres | SourceKind::PostgresCdc => {
            let address = match postgres_address(project, &source.connection) {
                Ok(address) => address,
                Err(problem) => return Ok(Err(problem)),
            };
            let (client, encryption) = match connect_postgres(address.clone()).await {
                Ok(connected) => connected,
                Err(error) => return Ok(Err(LoaderError::from(error).to_string())),
            };
            let warnings: Vec<String> =
                plain_text_warning((source_name, &source.connection), &address, encryption)
                    .into_iter()
                    .collect();
            Ok(
                match read_catalog(&client, &source.include, &source.exclude).await {
                    Ok(discovered) => Ok((discovered, None, warnings)),
                    Err(error) => Err(LoaderError::from(error).to_string()),
                },
            )
        }
        SourceKind::Files => {
            let warehouse = match source_target(project, source) {
                Ok(target) => target.warehouse,
                Err(problem) => return Ok(Err(problem)),
            };
            Ok(
                match discover_files_table(project, source_name, source, warehouse).await {
                    Ok((table, files)) => Ok((vec![table], Some(files), Vec::new())),
                    Err(error) => Err(error.to_string()),
                },
            )
        }
        SourceKind::Snowflake => {
            let reader = match snowflake_session(project, source)
                .and_then(|session| Ok(connect_snowflake_source(session, None)?))
            {
                Ok(reader) => reader,
                Err(error) => return Ok(Err(error.to_string())),
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
                    Err(error) => Err(error.to_string()),
                },
            )
        }
        SourceKind::Python => Ok(Ok((python_tables(source), None, Vec::new()))),
        SourceKind::GoogleSheets => Ok(match sheets_tables(project, source).await {
            Ok((tables, warnings)) => Ok((tables, None, warnings)),
            Err(error) => Err(error.to_string()),
        }),
    }
}

/// A Sheets client for a `google_sheets` source's connection, with the key it names (or `GOOGLE_APPLICATION_CREDENTIALS`'s), and the spreadsheet it reads.
pub(crate) fn sheets_reader(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<(SheetsClient, String), LoaderError> {
    let Some(ConnectionConfig::GoogleSheets {
        service_account_file,
        service_account_json,
        service_account_json_base64,
        base_url,
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
    ];
    if given.into_iter().filter(|given| *given).count() > 1 {
        return Err(LoaderError::Invalid(format!(
            "connection {:?} sets more than one of service_account_json, service_account_json_base64 and service_account_file; keep one",
            source.connection
        )));
    }
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
    let http = build_http_client(&ConnectionCounter::default())?;
    let client = sheets_client(http, &key, base_url.as_deref())?;
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
        let mut read = match sheet_table(&grid, sheet_rule(source, &name)) {
            Ok(read) => read,
            Err(found) => {
                problems.extend(found);
                continue;
            }
        };
        let settings = source.tables.get(&name).map(|table| &table.settings);
        let key = settings
            .and_then(|settings| settings.unique_key.as_ref())
            .or(source.defaults.unique_key.as_ref());
        let merge = settings
            .and_then(|settings| settings.write_strategy)
            .or(source.defaults.write_strategy)
            == Some(WriteStrategy::Merge);
        let duplicate = key.filter(|_| merge).and_then(|key| {
            let problem = duplicate_key(&grid, sheet_rule(source, &name), &read, key)?;
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
        });
    }
    match problems.is_empty() {
        true => Ok((tables, warnings)),
        false => Err(LoaderError::Invalid(problems.join("; "))),
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

/// A `google_sheets` table's rows as they are now, in its read columns' types.
async fn sheet_rows(
    client: &SheetsClient,
    spreadsheet_id: &str,
    table: &TablePlan,
) -> Result<RecordBatch, LoaderError> {
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
    let columns: Vec<(String, ColumnType)> = table
        .read_columns
        .iter()
        .map(|column| (column.name.clone(), column.output.clone()))
        .collect();
    sheet_batch(&grid, read.rule, &columns).map_err(LoaderError::Invalid)
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

/// A files source as one source table (file columns, partition columns, the source-file column), and its files.
pub(crate) async fn discover_files_table(
    project: &ProjectConfig,
    source_name: &str,
    source: &SourceConfig,
    warehouse: Warehouse,
) -> Result<(SourceTable, FilesSource), LoaderError> {
    let location = files_location(project, source)?;
    let folder = open_folder(&location)?;
    let (values, _) = path_values(source, warehouse)?;
    let encoding = source.files_encoding();
    let table = discover_files(&folder, &values, &encoding).await?;
    if let Some(problem) = table.problems.first() {
        return Err(LoaderError::Invalid(problem.clone()));
    }
    files_source_parts(source_name, source, warehouse, (location, table))
}

/// What a files source fills from each file's path and its configuration, and the types of those columns in table order: `partition_columns`, `path_columns` captures, then `constant_columns`.
fn path_values(
    source: &SourceConfig,
    warehouse: Warehouse,
) -> Result<(PathValues, Vec<(String, ColumnType)>), LoaderError> {
    let mut types: Vec<(String, ColumnType)> = Vec::new();
    for (name, text) in &source.partition_columns {
        let parsed = parse_type(text, &[warehouse])
            .map_err(|error| LoaderError::Invalid(format!("partition column {name}: {error}")))?;
        types.push((name.clone(), parsed.column_type));
    }
    let template = source
        .path_columns
        .as_deref()
        .map(|text| path_template(text, &[warehouse]))
        .transpose()
        .map_err(LoaderError::Invalid)?;
    types.extend(
        template
            .iter()
            .flat_map(|template| template.captures.clone()),
    );
    let constants =
        constant_columns(&source.constant_columns, &[warehouse]).map_err(LoaderError::Invalid)?;
    let values = PathValues {
        partition_columns: source.partition_columns.keys().cloned().collect(),
        template,
        unmatched: source.unmatched_paths.unwrap_or_default(),
        constants: constants
            .iter()
            .map(|(name, _, text)| (name.clone(), text.clone()))
            .collect(),
    };
    types.extend(
        constants
            .into_iter()
            .map(|(name, column_type, _)| (name, column_type)),
    );
    Ok((values, types))
}

/// A files source's table and files from the files found: the file columns, partition columns and source-file columns as one source table.
pub(crate) fn files_source_parts(
    source_name: &str,
    source: &SourceConfig,
    warehouse: Warehouse,
    (location, table): (Location, FilesTable),
) -> Result<(SourceTable, FilesSource), LoaderError> {
    let encoding = source.files_encoding();
    let (values, partition_types) = path_values(source, warehouse)?;
    let added: Vec<&str> = partition_types
        .iter()
        .map(|(name, _)| name.as_str())
        .chain([SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN, SOURCE_ROW_COLUMN])
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
        schema: String::new(),
        name: source_name.to_owned(),
        columns,
        primary_key: Vec::new(),
        indexed_columns: Vec::new(),
        estimated_rows: -1,
        comment: None,
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
            constants: values.constants.into_iter().map(|(name, _)| name).collect(),
            encoding,
            checks,
            notes,
            copy_to_internal_stage: source.copy_to_internal_stage,
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

/// The columns of `columns` SQLLoad's reader keeps at nanoseconds for `spec`'s warehouse: those the files store at nanoseconds, unless pinned or `live` holds fewer digits.
pub(crate) fn nanosecond_columns(
    spec: &TableSpec,
    columns: &[FileColumn],
    live: Option<&[LiveColumn]>,
) -> Vec<String> {
    match sql_adapter(spec.warehouse).keeps_nanoseconds() {
        true => columns
            .iter()
            .filter(|column| {
                matches!(
                    column.stored_read,
                    StoredRead::Nanos | StoredRead::Epoch(NANOSECOND_SCALE)
                )
            })
            .filter(|column| !spec.pinned.contains_key(&column.name))
            .filter(|column| fewer_digits(live, &column.name).is_none())
            .map(|column| column.name.clone())
            .collect(),
        false => Vec::new(),
    }
}

/// A warning for each column the files store at nanoseconds but the live table holds at fewer digits, so the load cuts them; a column whose type is pinned is the user's choice and is not warned about.
pub(crate) fn truncated_nanoseconds(spec: &TableSpec, live: Option<&[LiveColumn]>) -> Vec<String> {
    let Some(files) = &spec.files else {
        return Vec::new();
    };
    files
        .table
        .columns
        .iter()
        .filter(|column| {
            matches!(
                column.stored_read,
                StoredRead::Nanos | StoredRead::Epoch(NANOSECOND_SCALE)
            ) && !spec.pinned.contains_key(&column.name)
        })
        .filter_map(|column| truncated_warning(live, &column.name))
        .collect()
}

/// The warning for column `name` when the live column holds fewer than nanosecond digits.
fn truncated_warning(live: Option<&[LiveColumn]>, name: &str) -> Option<String> {
    let digits = fewer_digits(live, name)?;
    Some(format!(
        "{name}: nanoseconds in the files, but the column holds {digits} digits after the second, so they are truncated.\nRebuild the column at (9) (--reload) to keep them."
    ))
}

/// The digits after the second the live column `name` holds, when fewer than nanoseconds, so SQLLoad's reader cuts its values.
fn fewer_digits(live: Option<&[LiveColumn]>, name: &str) -> Option<i64> {
    live.into_iter()
        .flatten()
        .find(|existing| existing.name.eq_ignore_ascii_case(name))
        .and_then(|existing| existing.fraction_digits)
        .filter(|digits| *digits < i64::from(NANOSECOND_SCALE))
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
            .find(|(name, _)| *name == cursor.column)
        {
            Some((name, column_type)) => Some((name.as_str(), column_type)),
            None => {
                return Ok(Err(format!(
                    "cursor_column {} must be one of the partition_columns or path_columns for a files source",
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
            "the table has no {SOURCE_FILE_COLUMN} column, so append cannot tell which files are loaded and would load them all again; {}",
            column_fix(spec.warehouse)
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
        .chain([SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN, SOURCE_ROW_COLUMN])
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
    let nanos = nanosecond_columns(spec, &request.files.table.columns, request.live);
    Ok(select_files(&selection).map(|files| FilesRead {
        files,
        encoding: request.files.encoding.clone(),
        checks,
        columns,
        nanos,
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
    /// The Parquet footers already read for `files`.
    pub(crate) footers: BatchFooters,
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
            Ok(read_files(folder, &read, &part.footers, on_batch).await?)
        }
        (SourceReader::Snowflake(snowflake), _) => {
            let filter = part.filter.or_else(|| table.source_filter.clone());
            let request = UnloadRequest {
                table: &table.spec.source_table,
                sql: table
                    .spec
                    .warehouse_source
                    .as_ref()
                    .and_then(|source| source.sql.as_deref()),
                columns: &table.read_columns,
                filter: filter.as_deref(),
                batch_rows: SOURCE_BATCH_ROWS,
            };
            Ok(unload_rows(snowflake, &request, on_batch).await?)
        }
        (SourceReader::Sheets(client, spreadsheet_id), _) => {
            let batch = sheet_rows(client, spreadsheet_id, table).await?;
            let rows = batch.num_rows() as u64;
            let mut on_batch = on_batch;
            on_batch(batch).await.map_err(LoaderError::Invalid)?;
            Ok(rows)
        }
        (SourceReader::Files(_), None) => Err(LoaderError::Invalid(format!(
            "{}: a files source has no files planned",
            table.spec.source_table.qualified_name()
        ))),
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
        let integer_cursor = spec.cursor.as_ref().and_then(|cursor| {
            spec.source_table
                .columns
                .iter()
                .find(|column| column.name == cursor.column)
                .filter(|column| {
                    matches!(
                        column.column_type,
                        Ok(ColumnType::Int | ColumnType::SmallInt)
                    )
                })
                .map(|column| column.name.clone())
        });
        let wanted = matches!(spec.batch, Some(BatchStep::Integer(_)))
            && selected(
                options,
                (
                    source_name,
                    &spec.destination.table,
                    &spec.source_table.qualified_name(),
                ),
            );
        let Some(column) = integer_cursor.filter(|_| wanted) else {
            continue;
        };
        if client.is_none() {
            client = Some(connect_source(project, source).await?.0);
        }
        let Some(connected) = &client else {
            continue;
        };
        spec.source_end = Some(
            match cursor_end(connected, &spec.source_table, &column).await? {
                Some((maximum, end)) => SourceEnd::Maximum { maximum, end },
                None => SourceEnd::Empty,
            },
        );
    }
    Ok(specs)
}

/// A warning for each selector matching no source or table, with the nearest name: `load` would otherwise say only `0 selected`.
pub(crate) fn unmatched_selectors(
    project: &ProjectConfig,
    options: &PlanOptions,
    names: &[(String, String)],
) -> Vec<String> {
    let known: Vec<&str> = names
        .iter()
        .flat_map(|(destination, source_table)| [destination.as_str(), source_table.as_str()])
        .chain(project.sources.keys().map(String::as_str))
        .collect();
    options
        .select
        .iter()
        .chain(&options.exclude)
        .filter(|selector| !known.contains(&selector.as_str()))
        .map(
            |selector| match nearest_name(selector, known.iter().copied()) {
                Some(nearest) => format!("no table matches {selector}; did you mean {nearest}?"),
                None => format!("no table matches {selector}"),
            },
        )
        .collect()
}

/// Whether `--select` picks a table and `--exclude` does not, each by source name, destination name or `schema.table`; no `--select` picks every table.
pub(crate) fn selected(
    options: &PlanOptions,
    (source, destination, source_table): (&str, &str, &str),
) -> bool {
    let picks = |selector: &String| {
        selector == source || selector == destination || selector == source_table
    };
    (options.select.is_empty() || options.select.iter().any(picks))
        && !options.exclude.iter().any(picks)
}

/// Source `source_name`'s tables resolved to their settings, with its files for a files source, or `Err` with its problem when it cannot list them; its problems count only when the selection in `options` takes something from it (see [`source_in_scope`]), and are dropped otherwise.
pub(crate) async fn scoped_tables(
    (project, options): (&ProjectConfig, &PlanOptions),
    (source_name, source): (&str, &SourceConfig),
) -> Result<Result<(ResolvedTables, Option<FilesSource>), Vec<String>>, LoaderError> {
    let (discovered, files, warnings) = match source_tables(project, source_name, source).await? {
        Ok(found) => found,
        Err(problem) => {
            let in_scope = source_in_scope((project, options), (source_name, source), None);
            return Ok(Err(in_scope
                .then(|| format!("[sources.{source_name}]: {problem}"))
                .into_iter()
                .collect()));
        }
    };
    let mut resolved = resolve_tables(project, source_name, &discovered);
    match source_in_scope((project, options), (source_name, source), Some(&discovered)) {
        true => resolved.warnings = warnings,
        false => resolved.problems.clear(),
    }
    Ok(Ok((resolved, files)))
}

/// Whether the selection in `options` takes anything from source `source_name`: a table it `listed`, or one named under its `tables` but not listed, by destination name or `schema.table`, or the source itself when it holds no table. A source that could not list its tables (`None`) counts when selected by name, or for any `--select` selector naming no source, since that table could be its. Only then do the source's problems count, so one source's bad settings do not fail a command run on another.
pub(crate) fn source_in_scope(
    (project, options): (&ProjectConfig, &PlanOptions),
    (source_name, source): (&str, &SourceConfig),
    listed: Option<&[SourceTable]>,
) -> bool {
    let Some(discovered) = listed else {
        return selected(options, (source_name, "", ""))
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
    let named = source
        .tables
        .iter()
        .filter(|(qualified, _)| !found.contains(qualified))
        .map(|(qualified, overrides)| {
            let destination = match overrides.name.as_deref() {
                Some(name) if !name.contains('{') => name,
                _ => "",
            };
            (destination.to_owned(), qualified.clone())
        });
    let tables: Vec<(String, String)> = discovered
        .iter()
        .map(|table| {
            let name = source
                .tables
                .get(&table.qualified_name())
                .and_then(|overrides| overrides.name.as_deref());
            (
                table_name((source_name, source), name, table),
                table.qualified_name(),
            )
        })
        .chain(named)
        .collect();
    (tables.is_empty() && selected(options, (source_name, "", "")))
        || tables.iter().any(|(destination, qualified)| {
            selected(options, (source_name, destination, qualified))
        })
}

/// `planned`, the selected tables of one source, added to `plan`, which is returned (`unselected` and `found`, the destinations and options, tell whether this is the source's first load): a table that appeared after the source's first load (`first_load`: none of its tables, selected or not, existed yet) and is not named exactly in `include` is skipped or fails as `new_tables` says, and a table without a description is blocked.
pub(crate) async fn admit_tables(
    mut plan: ProjectPlan,
    (source_name, source): (&String, &SourceConfig),
    (planned, unselected): (Vec<TablePlan>, &[TableSpec]),
    found: (&BTreeMap<String, Destination>, &PlanOptions),
) -> Result<ProjectPlan, LoaderError> {
    let first_load = first_load(&planned, unselected, found).await?;
    let mut planned = planned;
    if let Some(problem) = partial_capture(source_name, source.kind, (&planned, unselected)) {
        for table in &mut planned {
            table.schema = blocked_plan(vec![problem.clone()]);
        }
    }
    for mut table in planned {
        let spec = &table.spec;
        let qualified = spec.source_table.qualified_name();
        let is_new = !first_load && !table.exists && !spec.approved;
        let has_settings = source.tables.contains_key(&qualified);
        match (is_new, spec.new_tables, has_settings) {
            (true, NewTables::Skip, true) => plan.skipped.push(SkippedTable {
                source_name: source_name.clone(),
                source_table: qualified,
                reason: "new table (new_tables = \"skip\") has settings but is not approved; add it to include".to_owned(),
            }),
            (true, NewTables::Skip, false) => plan.skipped.push(SkippedTable {
                source_name: source_name.clone(),
                source_table: qualified,
                reason: "new table (new_tables = \"skip\"); add it to include to sync it".to_owned(),
            }),
            (true, NewTables::Fail, _) => plan.problems.push(format!(
                "{qualified}: new table and new_tables = \"fail\"; add it to include or exclude it"
            )),
            _ => {
                if let Some(problem) = missing_description(source_name, source, &spec.source_table)
                {
                    table.schema = blocked_plan(vec![problem]);
                }
                plan.tables.push(table);
            }
        }
    }
    Ok(plan)
}

/// Why the selection cannot load `postgres_cdc` source `name`, which blocks every table it picks: it picks some of its tables (`planned`) but not all (`unselected`), and the slot they share would confirm the left-out tables' changes unwritten. A plan problem alone would not stop the picked tables loading.
fn partial_capture(
    name: &str,
    kind: SourceKind,
    (planned, unselected): (&[TablePlan], &[TableSpec]),
) -> Option<String> {
    let partial = kind == SourceKind::PostgresCdc && !planned.is_empty() && !unselected.is_empty();
    partial.then(|| {
        let left_out: Vec<String> = unselected
            .iter()
            .map(|spec| spec.source_table.qualified_name())
            .collect();
        format!(
            "this selection leaves out {} of source {name}, whose changes share its one slot and would be skipped; select the whole source, or move those tables to another postgres_cdc source",
            left_out.join(", ")
        )
    })
}

/// Whether this is the source's first load: none of its tables exists in the destination, whether selected (`planned`) or not (`unselected`, looked up only while none is found, a table that cannot be read counting as absent), so `--select` cannot make a later table look present at the first load.
async fn first_load(
    planned: &[TablePlan],
    unselected: &[TableSpec],
    (destinations, options): (&BTreeMap<String, Destination>, &PlanOptions),
) -> Result<bool, LoaderError> {
    if planned.iter().any(|table| table.exists) {
        return Ok(false);
    }
    for spec in unselected {
        let Some(destination) = destinations.get(&spec.target) else {
            continue;
        };
        let exists = match files_destination(destination) {
            Some(_) => plan_table(spec, destination, options)
                .await
                .is_ok_and(|table| table.exists),
            None => live_columns(destination, &spec.destination)
                .await
                .is_ok_and(|live| live.is_some()),
        };
        if exists {
            return Ok(false);
        }
    }
    Ok(true)
}
