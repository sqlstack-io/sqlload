//! The one-table project a copy runs.

use std::collections::BTreeMap;

use toml::{Table, Value};

use crate::copy::_helpers::endpoints::endpoint;
use crate::copy::constants::COPY_NAME;
use crate::copy::models::{CopyProject, CopyRequest, Endpoint, Role};

/// Connection kinds that hold files: a copy from one is a files source, and into one a files target.
const FILE_KINDS: &[&str] = &["s3", "filesystem"];

/// The project copying `request.source` into `request.destination`, with `connections` the project's own (as written, references unresolved).
pub(crate) fn project(request: &CopyRequest, connections: &Table) -> Result<CopyProject, String> {
    let source = endpoint(&request.source, Role::Source, connections)?;
    let destination = endpoint(&request.destination, Role::Destination, connections)?;
    let table = source_table(&source)?;
    let mut text = Table::new();
    text.insert("default_target".to_owned(), text_value(COPY_NAME));
    text.insert("connections".to_owned(), Value::Table(connection_tables(&source, &destination)));
    let (target, schema, name) = target(&destination, &table)?;
    text.insert("targets".to_owned(), Value::Table(single(COPY_NAME, target)));
    let source_table = source_settings(request, &source, schema, &name)?;
    text.insert("sources".to_owned(), Value::Table(single(COPY_NAME, source_table)));
    let variables: BTreeMap<String, String> = [&source.secret, &destination.secret]
        .into_iter()
        .flatten()
        .cloned()
        .collect();
    let warnings = [&source, &destination]
        .iter()
        .filter(|end| end.secret.is_some())
        .map(|end| format!("{} has a literal password; use ${{ENV:NAME}} in the URI so it stays out of shell history", end.shown))
        .collect();
    let body = toml::to_string(&text).map_err(|error| format!("cannot write the copy's project: {error}"))?;
    Ok(CopyProject {
        text: body,
        variables,
        warnings,
    })
}

/// The source's table name: the part after the last dot of its locator, or of its folder for files.
fn source_table(source: &Endpoint) -> Result<String, String> {
    let name = source
        .locator
        .trim_end_matches('/')
        .rsplit(['.', '/'])
        .next()
        .filter(|name| !name.is_empty())
        .or_else(|| {
            source
                .settings
                .get("root")
                .and_then(Value::as_str)
                .and_then(|root| root.trim_end_matches('/').rsplit('/').next())
        })
        .unwrap_or_default();
    match name.is_empty() {
        true => Err(format!(
            "the source {} names no table; add one, as connection:schema.table or a URI ending #schema.table",
            source.shown
        )),
        false => Ok(name.to_owned()),
    }
}

fn connection_tables(source: &Endpoint, destination: &Endpoint) -> Table {
    let mut tables = Table::new();
    tables.insert(source.connection.clone(), Value::Table(source.settings.clone()));
    tables.insert(destination.connection.clone(), Value::Table(destination.settings.clone()));
    tables
}

/// The target section, and the destination's schema and table name.
fn target(destination: &Endpoint, table: &str) -> Result<(Table, Option<String>, String), String> {
    let mut target = Table::new();
    target.insert("connection".to_owned(), text_value(&destination.connection));
    match FILE_KINDS.contains(&kind(destination)) {
        true => {
            let folder = destination.locator.trim_matches('/');
            let path = match folder.is_empty() {
                true => "{table}/".to_owned(),
                false => format!("{folder}/{{table}}/"),
            };
            target.insert("path".to_owned(), Value::String(path));
            Ok((target, None, table.to_owned()))
        }
        false => {
            let (schema, name) = destination.locator.split_once('.').ok_or_else(|| {
                format!(
                    "the destination {} names no table; end it with SCHEMA.TABLE",
                    destination.shown
                )
            })?;
            target.insert("loader_schema".to_owned(), text_value(schema));
            Ok((target, Some(schema.to_owned()), name.to_owned()))
        }
    }
}

/// The source section: a database table by `schema.table`, or a folder of files.
fn source_settings(
    request: &CopyRequest,
    source: &Endpoint,
    schema: Option<String>,
    name: &str,
) -> Result<Table, String> {
    let mut settings = Table::new();
    settings.insert("connection".to_owned(), text_value(&source.connection));
    settings.insert("description".to_owned(), Value::String(format!("Copied from {}.", source.shown)));
    settings.extend(schema.map(|schema| ("schema".to_owned(), Value::String(schema))));
    settings.extend(table_settings(request));
    match kind(source) {
        "postgres" => {
            settings.insert("kind".to_owned(), text_value("postgres"));
            settings.insert("include".to_owned(), Value::Array(vec![text_value(&source.locator)]));
            let mut named = Table::new();
            named.insert("name".to_owned(), text_value(name));
            settings.insert("tables".to_owned(), Value::Table(single(&source.locator, named)));
        }
        files if FILE_KINDS.contains(&files) => {
            settings.insert("kind".to_owned(), text_value("files"));
            settings.insert("path".to_owned(), text_value(&source.locator));
            settings.insert("table_name".to_owned(), text_value(name));
            settings.extend(request.format.clone().map(|format| ("format".to_owned(), Value::String(format))));
        }
        other => {
            return Err(format!(
                "the source {} is a {other} connection; copy reads Postgres tables and files (s3://, file://, a folder)",
                source.shown
            ));
        }
    }
    Ok(settings)
}

/// The write settings the command line gives.
fn table_settings(request: &CopyRequest) -> Vec<(String, Value)> {
    let mut settings: Vec<(String, Value)> = Vec::new();
    settings.extend(request.write_strategy.clone().map(|strategy| ("write_strategy".to_owned(), Value::String(strategy))));
    settings.extend((!request.unique_key.is_empty()).then(|| {
        let keys = request.unique_key.iter().map(|key| text_value(key)).collect();
        ("unique_key".to_owned(), Value::Array(keys))
    }));
    settings.extend(request.cursor_column.clone().map(|column| ("cursor_column".to_owned(), Value::String(column))));
    settings
}

fn kind(end: &Endpoint) -> &str {
    end.settings.get("kind").and_then(Value::as_str).unwrap_or_default()
}

fn single(key: &str, table: Table) -> Table {
    let mut outer = Table::new();
    outer.insert(key.to_owned(), Value::Table(table));
    outer
}

fn text_value(text: &str) -> Value {
    Value::String(text.to_owned())
}
