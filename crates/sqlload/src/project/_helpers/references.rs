use crate::files_source::constants::LOADER_COLUMNS;
use crate::files_source::main::constant_columns::constant_columns;
use crate::files_source::main::path_template::path_template;
use crate::files_source::main::text_charset::text_charset;
use crate::files_source::models::{AfterLoad, Compression, RecordShape, SourceFormat};
use crate::project::_helpers::targets::{
    destination_kind_problems, is_files_target, stage_problems, target_problems,
};
use crate::project::constants::{
    FILES_CONNECTION_KINDS, MAX_POSTGRES_NAME_BYTES, MIN_BATCH_INTERVAL, STAGE_NAME_PARTS,
    TABLE_PLACEHOLDER,
};
use crate::project::models::{
    CdcDeletes, CdcMode, ConnectionConfig, ProjectConfig, SourceConfig, SourceKind, TableSettings,
    WriteStrategy,
};
use crate::store_paths::main::resolve_location::resolve_location;
use crate::store_paths::models::{Location, StoreKind};

/// Problems with names one section uses to refer to another.
pub(crate) fn reference_problems(project: &ProjectConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    if let Some(default_target) = &project.default_target
        && !project.targets.contains_key(default_target)
    {
        problems.push(format!(
            "default_target {default_target:?} is not a [targets] entry"
        ));
    }
    for (name, target) in &project.targets {
        problems.extend(target_problems(project, name, target));
    }
    for (name, connection) in &project.connections {
        let targeted = project
            .targets
            .values()
            .any(|target| target.connection == *name);
        problems.extend(stage_problems(name, connection, targeted));
        problems.extend(odbc_connection_problems(name, connection));
        if let ConnectionConfig::S3 {
            profile: Some(profile),
            ..
        } = connection
            && profile.is_empty()
        {
            problems.push(format!(
                "[connections.{name}] profile is empty; omit it to use AWS_PROFILE, else default"
            ));
        }
    }
    for (name, source) in &project.sources {
        problems.extend(source_problems(project, name, source));
    }
    problems
}

/// Why an `odbc` connection cannot connect: it needs exactly one of `connection_string` and `dsn`; `user` and `password` go with `dsn`; and `dsn` and `user` are written unbraced, as driver managers read them, so they cannot hold `;`, `{` or `}`.
fn odbc_connection_problems(name: &str, connection: &ConnectionConfig) -> Vec<String> {
    let ConnectionConfig::Odbc {
        connection_string,
        dsn,
        user,
        password,
    } = connection
    else {
        return Vec::new();
    };
    let place = format!("[connections.{name}]");
    let mut problems: Vec<String> = Vec::new();
    match (connection_string.is_some(), dsn.is_some()) {
        (false, false) => problems.push(format!("{place} needs connection_string, or dsn")),
        (true, true) => problems.push(format!(
            "{place} takes connection_string or dsn, not both"
        )),
        (true, false) if user.is_some() || password.is_some() => problems.push(format!(
            "{place} user and password go with dsn; with connection_string, put them in it (UID=...;PWD=...)"
        )),
        _ => {}
    }
    let unbraced = [dsn.as_deref(), user.as_deref()];
    if unbraced
        .into_iter()
        .flatten()
        .any(|value| value.contains([';', '{', '}']))
    {
        problems.push(format!(
            "{place} dsn and user cannot contain ';', '{{' or '}}'; use connection_string instead"
        ));
    }
    problems
}

fn source_problems(project: &ProjectConfig, name: &str, source: &SourceConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let place = source_place(project, name);
    let (kinds, expected): (&[&str], &str) = match source.kind {
        SourceKind::Postgres | SourceKind::PostgresCdc => (&["postgres"], "postgres"),
        SourceKind::Snowflake => (&["snowflake"], "snowflake"),
        SourceKind::Odbc => (&["odbc"], "odbc"),
        SourceKind::BigQuery => (&["bigquery"], "bigquery"),
        SourceKind::Files => (&["s3", "filesystem", "sftp"], "s3, filesystem or sftp"),
        SourceKind::Python => (&[], "any"),
        SourceKind::GoogleSheets => (&["google_sheets"], "google_sheets"),
    };
    match project.connections.get(&source.connection) {
        None if source.connection.is_empty() && source.kind == SourceKind::Python => {}
        None if source.connection.is_empty() => {
            problems.push(format!("{place} needs a connection"));
        }
        None => problems.push(format!(
            "{place} connection {:?} is not a [connections] entry",
            source.connection
        )),
        Some(connection) if source.kind == SourceKind::Python => {
            problems.extend(shared_warehouse_problems(project, name, source, connection));
        }
        Some(connection) if !kinds.contains(&connection.kind()) => {
            problems.push(format!(
                "{place} has kind = {:?} but connection {:?} is a {} connection (expected {expected})",
                source_kind_name(source.kind),
                source.connection,
                connection.kind()
            ));
        }
        Some(ConnectionConfig::Sftp { .. }) if source.after_load != AfterLoad::Keep => {
            problems.push(format!(
                "{place} after_load is not for an sftp connection: SQLLoad only reads from SFTP servers, so it cannot delete or move their files"
            ));
        }
        Some(ConnectionConfig::Sftp { .. }) if source.copy_to_internal_stage => {
            problems.push(format!(
                "{place} copy_to_internal_stage is not for an sftp connection: files on an SFTP server always pass through SQLLoad's reader"
            ));
        }
        Some(_) => {}
    }
    let only_target = (project.targets.len() == 1)
        .then(|| project.targets.keys().next())
        .flatten();
    match source
        .destination
        .as_ref()
        .or(project.default_target.as_ref())
        .or(only_target)
    {
        Some(target) if !project.targets.contains_key(target) => {
            problems.push(format!("{place} destination {target:?} is not a [targets] entry"));
        }
        Some(target) => problems.extend(destination_kind_problems(
            &place,
            source,
            is_files_target(project, target),
        )),
        None => problems.push(format!(
            "{place} needs a destination: set destination, or default_target when there are several targets"
        )),
    }
    problems.extend(kind_problems(&place, source));
    if source.kind == SourceKind::PostgresCdc {
        problems.extend(cdc_name_problems(&place, name, source));
    }
    problems
}

fn source_kind_name(kind: SourceKind) -> &'static str {
    match kind {
        SourceKind::Postgres => "postgres",
        SourceKind::PostgresCdc => "postgres_cdc",
        SourceKind::Files => "files",
        SourceKind::Snowflake => "snowflake",
        SourceKind::Odbc => "odbc",
        SourceKind::BigQuery => "bigquery",
        SourceKind::Python => "python",
        SourceKind::GoogleSheets => "google_sheets",
    }
}

/// A Python source reading a connection a warehouse target loads into, whose credentials Python code must never get (a files target's s3 or filesystem connection may be shared).
fn shared_warehouse_problems(
    project: &ProjectConfig,
    name: &str,
    source: &SourceConfig,
    connection: &ConnectionConfig,
) -> Vec<String> {
    if FILES_CONNECTION_KINDS.contains(&connection.kind()) {
        return Vec::new();
    }
    project
        .targets
        .iter()
        .filter(|(_, target)| target.connection == source.connection)
        .map(|(target, _)| {
            format!(
                "Python source {name} uses connection {}, which target {target} loads into; Python code never gets warehouse credentials — give the source its own connection",
                source.connection
            )
        })
        .collect()
}

/// How problems name a source: its TOML section, or the Python file declaring it.
fn source_place(project: &ProjectConfig, name: &str) -> String {
    match project.python_modules.get(name) {
        Some(module) => format!("Python source {name} ({module})"),
        None => format!("[sources.{name}]"),
    }
}

/// Settings each kind requires or rejects.
fn kind_problems(place: &str, source: &SourceConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let template = source.table_name_template();
    let cdc_only = source.publication.is_some()
        || source.slot.is_some()
        || source.cdc_mode != CdcMode::Merge
        || source.deletes != CdcDeletes::Soft
        || source.batch_interval.is_some();
    if cdc_only && source.kind != SourceKind::PostgresCdc {
        problems.push(format!(
            "{place} publication, slot, cdc_mode, deletes and batch_interval are for kind = \"postgres_cdc\""
        ));
    }
    if source.kind == SourceKind::PostgresCdc {
        problems.extend(cdc_problems(place, source));
    }
    let queries = source.tables.values().any(|table| table.sql.is_some());
    if !matches!(
        source.kind,
        SourceKind::Snowflake | SourceKind::Odbc | SourceKind::BigQuery
    ) && queries
    {
        problems.push(format!(
            "{place} sql is for kind = \"snowflake\", \"odbc\" and \"bigquery\""
        ));
    }
    if source.kind != SourceKind::BigQuery && source.project.is_some() {
        problems.push(format!("{place} project is for kind = \"bigquery\""));
    }
    if source.kind != SourceKind::Snowflake
        && (source.warehouse.is_some() || source.stage.is_some())
    {
        problems.push(format!(
            "{place} warehouse and stage are for kind = \"snowflake\""
        ));
    }
    if source.kind != SourceKind::Odbc
        && (source.max_text_size.is_some() || source.max_binary_size.is_some())
    {
        problems.push(format!(
            "{place} max_text_size and max_binary_size are for kind = \"odbc\""
        ));
    }
    if source.kind == SourceKind::Snowflake {
        problems.extend(unload_stage_problems(place, source));
    }
    let sheets_only = source.spreadsheet_id.is_some()
        || !source.ranges.is_empty()
        || sheet_settings(source)
            .any(|settings| settings.header.is_some() || settings.header_row.is_some());
    if sheets_only && source.kind != SourceKind::GoogleSheets {
        problems.push(format!(
            "{place} spreadsheet_id, ranges, header and header_row are for kind = \"google_sheets\""
        ));
    }
    match source.kind {
        SourceKind::Postgres
        | SourceKind::PostgresCdc
        | SourceKind::Snowflake
        | SourceKind::Odbc
        | SourceKind::BigQuery => {
            if source.include.is_empty() && !queries {
                let tables = match source.kind {
                    SourceKind::Snowflake | SourceKind::Odbc | SourceKind::BigQuery => {
                        "; list tables in include, or give a table a sql query"
                    }
                    _ => "",
                };
                problems.push(format!(
                    "{place} include is empty, so it selects no tables{tables}"
                ));
            }
            if !template.contains(TABLE_PLACEHOLDER) {
                problems.push(format!(
                    "{place} table_name {template:?} must contain {TABLE_PLACEHOLDER}, or every table gets the same name"
                ));
            }
            problems.extend(files_only_problems(place, source));
        }
        SourceKind::Files => {
            if source.path.is_none() {
                problems.push(format!("{place} kind = \"files\" needs a path"));
            }
            if !source.include.is_empty() || !source.exclude.is_empty() {
                problems.push(format!("{place} include and exclude are for database sources; a files source is one table"));
            }
            problems.extend(encoding_problems(place, source));
            problems.extend(path_value_problems(place, source));
            problems.extend(after_load_problems(place, source));
        }
        SourceKind::GoogleSheets => {
            if source.ranges.len() != 1 && !template.contains(TABLE_PLACEHOLDER) {
                problems.push(format!(
                    "{place} table_name {template:?} must contain {TABLE_PLACEHOLDER}, or every table gets the same name"
                ));
            }
            problems.extend(files_only_problems(place, source));
            problems.extend(sheets_problems(place, source));
        }
        SourceKind::Python => {
            if source.tables.len() > 1 && !template.contains(TABLE_PLACEHOLDER) {
                problems.push(format!(
                    "{place} table_name {template:?} must contain {TABLE_PLACEHOLDER}, or every table gets the same name"
                ));
            }
        }
    }
    problems
}

/// Problems with a `snowflake` source's `stage`: an S3 `url` with its `storage_integration`, or an existing stage's `name`, not both.
fn unload_stage_problems(place: &str, source: &SourceConfig) -> Vec<String> {
    let Some(stage) = &source.stage else {
        return Vec::new();
    };
    let place = format!("{place} stage");
    let mut problems: Vec<String> = Vec::new();
    match (&stage.url, &stage.storage_integration, &stage.name) {
        (Some(url), Some(_), None) => match resolve_location(None, url) {
            Ok(Location {
                store: StoreKind::S3 { .. },
                ..
            }) => {}
            Ok(_) => problems.push(format!("{place} url {url:?} must be an s3:// URL")),
            Err(error) => problems.push(format!("{place} url {url:?}: {error}")),
        },
        (Some(_), None, None) => problems.push(format!(
            "{place} url needs the storage_integration Snowflake writes to it with"
        )),
        (None, None, Some(name))
            if name.split('.').filter(|part| !part.is_empty()).count() != STAGE_NAME_PARTS =>
        {
            problems.push(format!("{place} name {name:?} must be DATABASE.SCHEMA.STAGE"));
        }
        (None, None, Some(_)) => {}
        _ => problems.push(format!(
            "{place} takes either url and storage_integration (an S3 folder) or name (an existing stage)"
        )),
    }
    problems
}

/// The settings only a files source takes, set on a source of another kind.
fn files_only_problems(place: &str, source: &SourceConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    if source.path.is_some()
        || !source.partition_columns.is_empty()
        || source.path_columns.is_some()
        || source.unmatched_paths.is_some()
        || !source.constant_columns.is_empty()
        || source.copy_to_internal_stage
        || source.after_load != AfterLoad::Keep
    {
        problems.push(format!(
            "{place} path, partition_columns, path_columns, unmatched_paths, constant_columns, copy_to_internal_stage and after_load are for kind = \"files\""
        ));
    }
    if source.format != SourceFormat::Parquet
        || source.compression.is_some()
        || source.csv.is_some()
        || source.xlsx.is_some()
        || source.xml.is_some()
        || source.members.is_some()
        || source.shape != RecordShape::Columns
        || source.record_column.is_some()
    {
        problems.push(format!(
            "{place} format, compression, csv, xlsx, xml, members, shape and record_column are for kind = \"files\""
        ));
    }
    problems
}

/// A source's settings and each of its tables'.
fn sheet_settings(source: &SourceConfig) -> impl Iterator<Item = &TableSettings> {
    std::iter::once(&source.defaults).chain(source.tables.values().map(|table| &table.settings))
}

/// What a `google_sheets` source needs and cannot take: a spreadsheet, and no cursor, since a sheet has no column to read after.
fn sheets_problems(place: &str, source: &SourceConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    if source
        .spreadsheet_id
        .as_deref()
        .is_none_or(|id| id.trim().is_empty())
    {
        problems.push(format!("{place} kind = \"google_sheets\" needs a spreadsheet_id, the ID in the spreadsheet's URL"));
    }
    if !source.include.is_empty() || !source.exclude.is_empty() {
        problems.push(format!("{place} include and exclude are not for kind = \"google_sheets\"; name its tables in ranges"));
    }
    for settings in sheet_settings(source) {
        if settings.cursor_column.is_some()
            || settings.batch_size.is_some()
            || settings.write_strategy == Some(WriteStrategy::DeleteInsert)
        {
            problems.push(format!(
                "{place} a sheet has no cursor, so cursor_column, batch_size and write_strategy = \"delete_insert\" do not apply; use replace (the default), merge on a unique_key, or append"
            ));
            break;
        }
    }
    if sheet_settings(source).any(|settings| settings.header_row == Some(0)) {
        problems.push(format!("{place} header_row counts from 1"));
    }
    problems
}

/// A files source's `path_columns` and `constant_columns` that do not parse, and a column any two of them and `partition_columns` both fill.
fn path_value_problems(place: &str, source: &SourceConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let mut names: Vec<(String, &str)> = source
        .partition_columns
        .keys()
        .map(|name| (name.clone(), "partition_columns"))
        .collect();
    match source
        .path_columns
        .as_deref()
        .map(|text| path_template(text, &[]))
    {
        None if source.unmatched_paths.is_some() => problems.push(format!(
            "{place} unmatched_paths is for path_columns, which is not set"
        )),
        None => {}
        Some(Ok(template)) => names.extend(
            template
                .captures
                .into_iter()
                .map(|(name, _)| (name, "path_columns")),
        ),
        Some(Err(problem)) => problems.push(format!("{place} {problem}")),
    }
    if let Some(record) = &source.files_encoding().record {
        names.push((record.clone(), "record_column"));
    }
    match constant_columns(&source.constant_columns, &[]) {
        Ok(constants) => names.extend(
            constants
                .into_iter()
                .map(|(name, _, _)| (name, "constant_columns")),
        ),
        Err(problem) => problems.push(format!("{place} {problem}")),
    }
    for (index, (name, setting)) in names.iter().enumerate() {
        if let Some((_, first)) = names[..index]
            .iter()
            .find(|(seen, _)| seen.eq_ignore_ascii_case(name))
        {
            problems.push(format!(
                "{place} column {name} is in both {first} and {setting}; give it one source"
            ));
        }
    }
    problems
}

/// A files source's `after_load` that cannot apply: to a ZIP archive's members, which share their archive (another source may still read it), or moving files to an empty folder or one inside `path`, where they would be listed again.
fn after_load_problems(place: &str, source: &SourceConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    if source.after_load != AfterLoad::Keep && source.compression == Some(Compression::Zip) {
        problems.push(format!(
            "{place} after_load is not for compression = \"zip\": an archive's members load one by one, and another source may read the same archive"
        ));
    }
    if let AfterLoad::MoveTo(folder) = &source.after_load {
        let folder = folder.trim_matches('/');
        let path = source.path.as_deref().unwrap_or_default().trim_matches('/');
        let inside = path.is_empty() || folder == path || folder.starts_with(&format!("{path}/"));
        match (folder.is_empty(), inside) {
            (true, _) => problems.push(format!(
                "{place} after_load.move_to must name a folder of the connection"
            )),
            (false, true) => problems.push(format!(
                "{place} after_load.move_to {folder:?} is inside path {path:?}, so moved files would be listed and loaded again; move them outside it"
            )),
            (false, false) => {}
        }
    }
    problems
}

/// Settings of the xlsx and xml formats and of ZIP archives that do not fit the source.
fn container_problems(place: &str, source: &SourceConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let zip = source.compression == Some(Compression::Zip);
    if source.xlsx.is_some() && source.format != SourceFormat::Xlsx {
        problems.push(format!("{place} xlsx is for format = \"xlsx\""));
    }
    match (&source.xml, source.format) {
        (Some(_), format) if format != SourceFormat::Xml => {
            problems.push(format!("{place} xml is for format = \"xml\""));
        }
        (Some(xml), _) if !xml.record_path.trim_matches('/').is_empty() => {}
        (_, SourceFormat::Xml) => problems.push(format!(
            "{place} format = \"xml\" needs xml.record_path, the element names from the root to each row's element, such as \"orders/order\""
        )),
        _ => {}
    }
    match (&source.members, zip) {
        (Some(_), false) => problems.push(format!("{place} members is for compression = \"zip\"")),
        (Some(pattern), true) => {
            if let Err(error) = globset::Glob::new(pattern) {
                problems.push(format!("{place} members {pattern:?}: {error}"));
            }
        }
        (None, _) => {}
    }
    let loader_only = zip || matches!(source.format, SourceFormat::Xlsx | SourceFormat::Xml);
    if source.copy_to_internal_stage && loader_only {
        problems.push(format!(
            "{place} copy_to_internal_stage is for files Snowflake reads itself; SQLLoad reads xlsx and xml files and ZIP archives' members"
        ));
    }
    problems
}

/// Settings that do not fit a files source's format.
fn encoding_problems(place: &str, source: &SourceConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    match (source.shape, source.format, &source.record_column) {
        (RecordShape::Record, SourceFormat::Parquet, _) => problems.push(format!(
            "{place} shape = \"record\" is for ndjson, json and csv files; Parquet files already have typed columns"
        )),
        (RecordShape::Record, format @ (SourceFormat::Xlsx | SourceFormat::Xml), _) => {
            problems.push(format!(
                "{place} shape = \"record\" is for ndjson, json and csv files, not {format}"
            ));
        }
        (RecordShape::Columns, _, Some(_)) => problems.push(format!(
            "{place} record_column is for shape = \"record\", which is not set"
        )),
        (_, _, Some(name)) if name.trim().is_empty() => {
            problems.push(format!("{place} record_column must not be empty"));
        }
        (_, _, Some(name))
            if LOADER_COLUMNS
                .iter()
                .any(|loader| loader.eq_ignore_ascii_case(name)) =>
        {
            problems.push(format!(
                "{place} record_column {name} is a column SQLLoad adds itself; choose another name"
            ));
        }
        _ => {}
    }
    if let Some(record) = &source.files_encoding().record {
        for (table, overrides) in &source.tables {
            let pinned: Vec<&str> = overrides
                .columns
                .keys()
                .map(String::as_str)
                .filter(|name| name != record)
                .collect();
            if !pinned.is_empty() {
                problems.push(format!(
                    "{place} shape = \"record\" loads each record into one json column, {record}, so [tables.{table}] columns cannot pin {}",
                    pinned.join(", ")
                ));
            }
        }
    }
    problems.extend(container_problems(place, source));
    if source.format == SourceFormat::Parquet && source.compression.is_some() {
        problems.push(format!(
            "{place} compression is for ndjson, json, csv, xlsx and xml files; Parquet files compress inside the file, and SQLLoad does not read them from ZIP archives"
        ));
    }
    match (&source.csv, source.format) {
        (None, _) => {}
        (Some(csv), SourceFormat::Csv) => {
            for (setting, character) in [("delimiter", csv.delimiter), ("quote", csv.quote)] {
                if !character.is_ascii() || character == '\n' || character == '\r' {
                    problems.push(format!(
                        "{place} csv {setting} must be one ASCII character other than a line break, not {character:?}"
                    ));
                }
            }
            if csv.delimiter == csv.quote {
                problems.push(format!("{place} csv delimiter and quote must differ"));
            }
            if let Err(problem) = text_charset(&csv.encoding) {
                problems.push(format!("{place} {problem}"));
            }
        }
        (Some(_), format) => problems.push(format!(
            "{place} csv is for format = \"csv\", not {format:?}",
            format = format.to_string()
        )),
    }
    problems
}

/// Settings a `postgres_cdc` source cannot take: the change stream decides how rows are written, so no strategy or cursor; `deletes = "hard"` needs `merge`.
fn cdc_problems(place: &str, source: &SourceConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let settings = std::iter::once(&source.defaults)
        .chain(source.tables.values().map(|table| &table.settings));
    for settings in settings {
        if settings.write_strategy.is_some()
            || settings.cursor_column.is_some()
            || settings.batch_size.is_some()
            || settings.state.is_some()
            || settings.checks.is_some()
        {
            problems.push(format!(
                "{place} kind = \"postgres_cdc\" writes every captured change, so it takes no write_strategy, cursor_column, batch_size, state or checks; choose cdc_mode = \"merge\" or \"changelog\""
            ));
            break;
        }
    }
    if source
        .batch_interval
        .is_some_and(|interval| interval < MIN_BATCH_INTERVAL)
    {
        problems.push(format!(
            "{place} batch_interval must be at least 1s, or `load --follow` would write a batch for almost every change"
        ));
    }
    if source.deletes == CdcDeletes::Hard && source.cdc_mode == CdcMode::Changelog {
        problems.push(format!(
            "{place} deletes = \"hard\" applies to cdc_mode = \"merge\"; a changelog keeps every delete as a row"
        ));
    }
    problems
}

/// A `postgres_cdc` source's slot and publication names, as set or by default (`sqlload_<source>`), must be plain Postgres names.
fn cdc_name_problems(place: &str, name: &str, source: &SourceConfig) -> Vec<String> {
    let default_name = format!("sqlload_{}", name.to_lowercase());
    let mut problems: Vec<String> = Vec::new();
    for (setting, chosen) in [("slot", &source.slot), ("publication", &source.publication)] {
        let effective = chosen.as_deref().unwrap_or(&default_name);
        let valid = !effective.is_empty()
            && effective.len() <= MAX_POSTGRES_NAME_BYTES
            && effective
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        if !valid {
            problems.push(format!(
                "{place} {setting} {effective:?} must be lower-case letters, digits and underscores, at most {MAX_POSTGRES_NAME_BYTES} characters; set {setting} = \"...\""
            ));
        }
    }
    problems
}
