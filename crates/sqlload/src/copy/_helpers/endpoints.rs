//! Reading `sqlload copy`'s endpoints: a connection named in the project, a URI, or a local folder.

use toml::{Table, Value};

use crate::copy::constants::{POSTGRES_SETTINGS, REDACTED, S3_SETTINGS};
use crate::copy::models::{Endpoint, Role};

/// The endpoint `text` names at `role`: `connection:locator` for a connection in `connections` (or a bare name), a URI, or a local folder; every message shows `text` with its password redacted.
pub(crate) fn endpoint(text: &str, role: Role, connections: &Table) -> Result<Endpoint, String> {
    let shown = redacted_uri(text);
    let known = |name: &str| connections.contains_key(name);
    let uri = text
        .split_once("://")
        .filter(|(scheme, _)| bare_name(scheme));
    match (uri, text.split_once(':')) {
        (Some((scheme, rest)), _) => uri_endpoint(&shown, scheme, rest, role),
        (None, Some((name, locator)))
            if one_slash_uri(name, locator) && (!known(name) || text.contains('@')) =>
        {
            Err(format!(
                "the {} {shown} looks like a URI with one slash; write {name}://...",
                role.label()
            ))
        }
        (None, _) if text.contains('@') => Err(format!(
            "the {} {shown} has an @ but no scheme; write a URI such as postgresql://user:${{ENV:PGPASSWORD}}@host/db#schema.table, or connection:schema.table",
            role.label()
        )),
        (None, Some((name, locator))) if known(name) || names_connection(name, locator) => {
            named((name, locator), &shown, connections, role)
        }
        (None, Some(_) | None) => Ok(folder(text, &shown)),
    }
}

/// `text` with the password of any `user:password@` redacted, whether or not it parses as a URI: everything from the first `:` after a `scheme://` (or from the start) up to the last `@`.
pub(crate) fn redacted_uri(text: &str) -> String {
    let start = text
        .split_once("://")
        .filter(|(scheme, _)| bare_name(scheme))
        .map_or(0, |(scheme, _)| scheme.len() + 3);
    let Some(at) = text.rfind('@').filter(|at| *at >= start) else {
        return text.to_owned();
    };
    match text[start..at].find(':') {
        Some(colon) => format!("{}{REDACTED}{}", &text[..start + colon + 1], &text[at..]),
        None => text.to_owned(),
    }
}

/// `scheme:/rest`: a URI typed with one slash, never a connection or a folder (a one-letter name is a Windows drive).
fn one_slash_uri(name: &str, locator: &str) -> bool {
    name.len() > 1 && bare_name(name) && locator.starts_with('/') && !locator.starts_with("//")
}

/// Whether `name:locator` not in sqlload.toml still reads as a connection name, so it is refused rather than read as a folder: a bare name before something that is not a path (so `C:\data` and `./a:b` stay folders).
fn names_connection(name: &str, locator: &str) -> bool {
    bare_name(name) && !locator.starts_with(['/', '\\'])
}

fn bare_name(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '_' || character == '-'
        })
}

fn named(
    (name, locator): (&str, &str),
    shown: &str,
    connections: &Table,
    role: Role,
) -> Result<Endpoint, String> {
    let settings = connections
        .get(name)
        .and_then(Value::as_table)
        .cloned()
        .ok_or_else(|| {
            let known: Vec<&str> = connections.keys().map(String::as_str).collect();
            format!(
                "the {} {shown} names connection {name}, which is not in sqlload.toml (it has: {}); name one of those, or give a URI such as postgresql://user@host/db#schema.table",
                role.label(),
                match known.is_empty() {
                    true => "none".to_owned(),
                    false => known.join(", "),
                }
            )
        })?;
    Ok(Endpoint {
        connection: name.to_owned(),
        named: true,
        settings,
        locator: locator.to_owned(),
        secret: None,
        shown: format!("{name}:{locator}"),
        described: format!("{name}:{locator}"),
    })
}

fn uri_endpoint(shown: &str, scheme: &str, rest: &str, role: Role) -> Result<Endpoint, String> {
    let (body, locator) = rest.split_once('#').unwrap_or((rest, ""));
    match scheme {
        "postgres" | "postgresql" => postgres(shown, scheme, rest, role),
        "s3" if body
            .split('/')
            .next()
            .is_some_and(|bucket| bucket.contains('@')) =>
        {
            Err(format!(
                "the {} s3://{REDACTED}@{} carries credentials, which s3:// URIs do not take (S3 credentials come from the environment or a connection in sqlload.toml)",
                role.label(),
                rest.rsplit_once('@').map_or("", |(_, after)| after)
            ))
        }
        "duckdb" | "s3" | "file" if shown != format!("{scheme}://{rest}") => Err(format!(
            "the {} {shown} carries credentials, which {scheme}:// URIs do not take (S3 credentials come from the environment or a connection in sqlload.toml)",
            role.label()
        )),
        "duckdb" => Ok(unnamed(
            settings(&[("kind", "duckdb"), ("database", body)]),
            locator,
            shown,
            format!("DuckDB table {locator}"),
        )),
        "s3" => s3(shown, body, role),
        "file" => Ok(folder(body, shown)),
        "snowflake" => Err(format!(
            "the {} {shown}: a Snowflake connection needs auth and a load_method, which a URI does not carry; declare it under [connections] in sqlload.toml and name it, as warehouse:SCHEMA.TABLE",
            role.label()
        )),
        "databricks" => Err(format!(
            "the {} {shown}: a Databricks connection needs OAuth credentials, a warehouse and a volume, which a URI does not carry; declare it under [connections] in sqlload.toml and name it, as warehouse:schema.table",
            role.label()
        )),
        "bigquery" => Err(format!(
            "the {} {shown}: a BigQuery connection needs a service-account key and a location, which a URI does not carry; declare it under [connections] in sqlload.toml and name it, as warehouse:dataset.table",
            role.label()
        )),
        other => Err(format!(
            "the {} {shown} has the scheme {other}://; use postgresql://, duckdb://, s3://, file:// or a connection named in sqlload.toml",
            role.label()
        )),
    }
}

/// `postgresql://user[:password]@host[:port]/database[?sslmode=...]#schema.table`; `${ENV:NAME}` works in every part, and a literal password moves into a variable so the project only references it.
fn postgres(shown: &str, scheme: &str, rest: &str, role: Role) -> Result<Endpoint, String> {
    let problem = |what: &str| {
        format!(
            "the {} {shown}: {what}; write it as {scheme}://user:${{ENV:PGPASSWORD}}@host:5432/database#schema.table",
            role.label()
        )
    };
    let at = userinfo_end(rest).ok_or_else(|| problem("it names no user"))?;
    let (userinfo, after) = (&rest[..at], &rest[at + 1..]);
    let (user, password) = match find_outside(userinfo, ':', false) {
        Some(colon) => (&userinfo[..colon], decoded(&userinfo[colon + 1..])),
        None => (userinfo, String::new()),
    };
    let literal = !password.is_empty() && !is_reference(&password);
    if literal && outside_positions(rest, '@').last() != Some(&at) {
        return Err(problem(
            "it has a literal password and another @, so where the password ends is ambiguous; give the password as ${ENV:NAME}",
        ));
    }
    let (place, locator) = after.split_once('#').unwrap_or((after, ""));
    let slash = find_outside(place, '/', false)
        .filter(|_| find_outside(place, '@', false).is_none())
        .ok_or_else(|| problem("it names no database"))?;
    let (host_port, path) = (&place[..slash], &place[slash + 1..]);
    let (database, query) = path.split_once('?').unwrap_or((path, ""));
    if database.is_empty() || database.contains('/') {
        return Err(problem("it names no database"));
    }
    let (host, port) = host_and_port(host_port);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let port = match port {
        Some(port) => Value::Integer(
            port.parse()
                .map_err(|_| problem(&format!("port {port} is not a number")))?,
        ),
        None => Value::Integer(5432),
    };
    let mut table = settings(&[
        ("kind", "postgres"),
        ("host", host),
        ("user", &decoded(user)),
        ("database", &decoded(database)),
    ]);
    table.insert("port".to_owned(), port);
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if !POSTGRES_SETTINGS.contains(&key) {
            return Err(problem(&format!(
                "it sets {key}; a postgresql:// URI takes only {}",
                POSTGRES_SETTINGS.join(", ")
            )));
        }
        table.insert(key.to_owned(), Value::String(decoded(value)));
    }
    let reference = format!("${{ENV:{}}}", role.password_variable());
    let (stored, password_shown) = match (password.is_empty(), literal) {
        (true, _) => (None, String::new()),
        (false, true) => (Some(reference), format!(":{REDACTED}")),
        (false, false) => (Some(password.clone()), format!(":{password}")),
    };
    table.extend(stored.map(|stored| ("password".to_owned(), Value::String(stored))));
    let mut endpoint = unnamed(
        table,
        locator,
        &format!("{scheme}://{user}{password_shown}@{place}#{locator}"),
        format!("Postgres table {locator}"),
    );
    endpoint.secret = literal.then(|| (role.password_variable().to_owned(), password));
    Ok(endpoint)
}

/// The host and port of `host:port`, `[v6]:port` or a host alone; `${ENV:...}` spans never split.
fn host_and_port(text: &str) -> (&str, Option<&str>) {
    let colon = match text.starts_with('[') {
        true => text
            .find(']')
            .and_then(|close| text[close..].find(':').map(|colon| close + colon)),
        false => find_outside(text, ':', true),
    };
    match colon {
        Some(colon) => (&text[..colon], Some(&text[colon + 1..])),
        None => (text, None),
    }
}

/// The `@` ending the userinfo: the last one outside closed `${...}` spans that a `/database` follows before any `#`, so `#` may be in the password and `@` in the table; else the last such `@`.
fn userinfo_end(rest: &str) -> Option<usize> {
    let ends_userinfo = |at: &usize| {
        let after = &rest[at + 1..];
        after
            .find('/')
            .is_some_and(|slash| after.find('#').is_none_or(|hash| slash < hash))
    };
    let outside = outside_positions(rest, '@');
    outside
        .iter()
        .rev()
        .find(|at| ends_userinfo(at))
        .or(outside.last())
        .copied()
}

/// Whether a password is one `${ENV:NAME}` reference rather than literal text.
fn is_reference(password: &str) -> bool {
    password
        .strip_prefix("${ENV:")
        .and_then(|rest| rest.strip_suffix('}'))
        .is_some_and(|name| {
            name.chars()
                .next()
                .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
                && name
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '_')
        })
}

/// Where `character` is in `text` outside closed `${...}` spans: its first place, or its last with `last`.
fn find_outside(text: &str, character: char, last: bool) -> Option<usize> {
    let positions = outside_positions(text, character);
    match last {
        true => positions.last().copied(),
        false => positions.first().copied(),
    }
}

/// Every place `character` is in `text` outside closed `${...}` spans.
fn outside_positions(text: &str, character: char) -> Vec<usize> {
    let mut depth = 0_usize;
    let mut found: Vec<usize> = Vec::new();
    let mut previous = '\0';
    for (index, current) in text.char_indices() {
        match current {
            '{' if previous == '$' && text[index..].contains('}') => depth += 1,
            '}' if depth > 0 => depth -= 1,
            _ if current == character && depth == 0 => found.push(index),
            _ => {}
        }
        previous = current;
    }
    found
}

/// `s3://bucket/prefix/`, with `?region=...` and `&endpoint=...`.
fn s3(shown: &str, body: &str, role: Role) -> Result<Endpoint, String> {
    let (path, query) = body.split_once('?').unwrap_or((body, ""));
    let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
    let mut table = settings(&[("kind", "s3"), ("bucket", bucket)]);
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if !S3_SETTINGS.contains(&key) {
            return Err(format!(
                "the {} {shown} sets {key}; an s3:// URI takes only {}",
                role.label(),
                S3_SETTINGS.join(" and ")
            ));
        }
        table.insert(key.to_owned(), Value::String(decoded(value)));
    }
    Ok(unnamed(
        table,
        prefix,
        shown,
        format!("s3://{bucket}/{prefix}"),
    ))
}

/// A local folder: the connection's root, with nothing after it.
fn folder(path: &str, shown: &str) -> Endpoint {
    unnamed(
        settings(&[("kind", "filesystem"), ("root", path)]),
        "",
        shown,
        format!("folder {path}"),
    )
}

/// An endpoint given as a URI or folder; its connection is named once the copy's table is known.
fn unnamed(settings: Table, locator: &str, shown: &str, described: String) -> Endpoint {
    Endpoint {
        connection: String::new(),
        named: false,
        settings,
        locator: locator.to_owned(),
        secret: None,
        shown: shown.to_owned(),
        described,
    }
}

fn settings(pairs: &[(&str, &str)]) -> Table {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), Value::String((*value).to_owned())))
        .collect()
}

/// `text` with `%XX` escapes decoded; an escape that is not hex stays as written.
fn decoded(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let escape = bytes
            .get(index + 1..index + 3)
            .filter(|_| bytes[index] == b'%')
            .and_then(hex_byte);
        match escape {
            Some(byte) => {
                out.push(byte);
                index += 3;
            }
            None => {
                out.push(bytes[index]);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The byte two hex digits spell, if they do.
fn hex_byte(hex: &[u8]) -> Option<u8> {
    let digits = match std::str::from_utf8(hex) {
        Ok(digits) => digits,
        Err(_) => return None,
    };
    match u8::from_str_radix(digits, 16) {
        Ok(byte) => Some(byte),
        Err(_) => None,
    }
}
