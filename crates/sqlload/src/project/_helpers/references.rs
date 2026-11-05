use crate::files_source::models::SourceFormat;
use crate::project::_helpers::targets::{
    destination_kind_problems, is_files_target, stage_problems, target_problems,
};
use crate::project::constants::{FILES_CONNECTION_KINDS, TABLE_PLACEHOLDER};
use crate::project::models::{ConnectionConfig, ProjectConfig, SourceConfig, SourceKind};

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
        problems.extend(stage_problems(name, connection));
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
        SourceKind::Postgres => "postgres",
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
    problems
}

fn source_kind_name(kind: SourceKind) -> &'static str {
    match kind {
        SourceKind::Postgres => "postgres",
        SourceKind::Files => "files",
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
    match source.kind {
        SourceKind::Postgres => {
            if source.include.is_empty() {
                problems.push(format!("{place} include is empty, so it selects no tables"));
            }
            if !template.contains(TABLE_PLACEHOLDER) {
                problems.push(format!(
                    "{place} table_name {template:?} must contain {TABLE_PLACEHOLDER}, or every table gets the same name"
                ));
            }
            if source.path.is_some()
                || !source.partition_columns.is_empty()
                || source.copy_to_internal_stage
            {
                problems.push(format!(
                    "{place} path, partition_columns and copy_to_internal_stage are for kind = \"files\""
                ));
            }
            if source.format != SourceFormat::Parquet
                || source.compression.is_some()
                || source.csv.is_some()
            {
                problems.push(format!(
                    "{place} format, compression and csv are for kind = \"files\""
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

/// Settings that do not fit a files source's format.
fn encoding_problems(place: &str, source: &SourceConfig) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
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
