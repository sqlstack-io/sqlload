use crate::files_source::constants::LOADER_COLUMNS;
use crate::files_source::main::constant_columns::constant_columns;
use crate::files_source::main::path_template::path_template;
use crate::files_source::models::{RecordShape, SourceFormat};
use crate::project::_helpers::targets::{
    destination_kind_problems, is_files_target, stage_problems, target_problems,
};
use crate::project::constants::{
    FILES_CONNECTION_KINDS, MAX_POSTGRES_NAME_BYTES, MIN_BATCH_INTERVAL, STAGE_NAME_PARTS,
    TABLE_PLACEHOLDER,
};
use crate::project::models::{
    CdcDeletes, CdcMode, ConnectionConfig, ProjectConfig, SourceConfig, SourceKind,
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
    }
    for (name, source) in &project.sources {
        problems.extend(source_problems(project, name, source));
    }
    problems
}

fn source_problems(project: &ProjectConfig, name: &str, source: &SourceConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let place = source_place(project, name);
    let expected = match source.kind {
        SourceKind::Postgres | SourceKind::PostgresCdc => "postgres",
        SourceKind::Snowflake => "snowflake",
        SourceKind::Files => "s3 or filesystem",
        SourceKind::Python => "any",
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
        Some(connection) if !expected.split(" or ").any(|kind| kind == connection.kind()) => {
            problems.push(format!(
                "{place} has kind = {:?} but connection {:?} is a {} connection (expected {expected})",
                source_kind_name(source.kind),
                source.connection,
                connection.kind()
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
        SourceKind::Python => "python",
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
    if source.kind != SourceKind::Snowflake
        && (queries || source.warehouse.is_some() || source.stage.is_some())
    {
        problems.push(format!(
            "{place} sql, warehouse and stage are for kind = \"snowflake\""
        ));
    }
    if source.kind == SourceKind::Snowflake {
        problems.extend(unload_stage_problems(place, source));
    }
    match source.kind {
        SourceKind::Postgres | SourceKind::PostgresCdc | SourceKind::Snowflake => {
            if source.include.is_empty() && !queries {
                let tables = match source.kind {
                    SourceKind::Snowflake => {
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
            if source.path.is_some()
                || !source.partition_columns.is_empty()
                || source.path_columns.is_some()
                || source.unmatched_paths.is_some()
                || !source.constant_columns.is_empty()
                || source.copy_to_internal_stage
            {
                problems.push(format!(
                    "{place} path, partition_columns, path_columns, unmatched_paths, constant_columns and copy_to_internal_stage are for kind = \"files\""
                ));
            }
            if source.format != SourceFormat::Parquet
                || source.compression.is_some()
                || source.csv.is_some()
                || source.shape != RecordShape::Columns
                || source.record_column.is_some()
            {
                problems.push(format!(
                    "{place} format, compression, csv, shape and record_column are for kind = \"files\""
                ));
            }
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

/// Settings that do not fit a files source's format.
fn encoding_problems(place: &str, source: &SourceConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    match (source.shape, source.format, &source.record_column) {
        (RecordShape::Record, SourceFormat::Parquet, _) => problems.push(format!(
            "{place} shape = \"record\" is for ndjson, json and csv files; Parquet files already have typed columns"
        )),
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
    if source.format == SourceFormat::Parquet && source.compression.is_some() {
        problems.push(format!(
            "{place} compression is for ndjson, json and csv files; Parquet files compress inside the file"
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
