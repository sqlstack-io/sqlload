//! Reading `sqlload copy`'s endpoints: a connection named in the project, or a URI.

use toml::{Table, Value};

use crate::copy::constants::{REDACTED, S3_SETTINGS};
use crate::copy::models::{Endpoint, Role};

/// The endpoint `text` names at `role`: `connection:locator` for a connection in `connections`, a URI, or a local folder.
pub(crate) fn endpoint(text: &str, role: Role, connections: &Table) -> Result<Endpoint, String> {
    match (text.split_once("://"), text.split_once(':')) {
        (Some((scheme, rest)), _) => uri_endpoint(text, scheme, rest, role),
        (None, Some((name, locator))) => named(name, locator, connections, role),
        (None, None) => Ok(folder(text, text, role)),
    }
}

fn named(name: &str, locator: &str, connections: &Table, role: Role) -> Result<Endpoint, String> {
    let settings = connections
        .get(name)
        .and_then(Value::as_table)
        .cloned()
        .ok_or_else(|| {
            let known: Vec<&str> = connections.keys().map(String::as_str).collect();
            format!(
                "the {} {name}:{locator} names connection {name}, which is not in sqlload.toml (it has: {}); name one of those, or give a URI such as postgresql://user@host/db#schema.table",
                role.label(),
                match known.is_empty() {
                    true => "none".to_owned(),
                    false => known.join(", "),
                }
            )
        })?;
    Ok(Endpoint {
        connection: name.to_owned(),
        settings,
        locator: locator.to_owned(),
        secret: None,
        shown: format!("{name}:{locator}"),
    })
}

fn uri_endpoint(text: &str, scheme: &str, rest: &str, role: Role) -> Result<Endpoint, String> {
    let (body, locator) = rest.split_once('#').unwrap_or((rest, ""));
    match scheme {
        "postgres" | "postgresql" => postgres(scheme, body, locator, role),
        "duckdb" => Ok(Endpoint {
            connection: role.connection().to_owned(),
            settings: settings(&[("kind", "duckdb"), ("database", body)]),
            locator: locator.to_owned(),
            secret: None,
            shown: text.to_owned(),
        }),
        "s3" => s3(text, body, role),
        "file" => Ok(folder(body, text, role)),
        "snowflake" => Err(format!(
            "the {} {text}: a Snowflake connection needs auth and a load_method, which a URI does not carry; declare it under [connections] in sqlload.toml and name it, as warehouse:SCHEMA.TABLE",
            role.label()
        )),
        other => Err(format!(
            "the {} {text} has the scheme {other}://; use postgresql://, duckdb://, s3://, file:// or a connection named in sqlload.toml",
            role.label()
        )),
    }
}

/// `postgresql://user[:password]@host[:port]/database#schema.table`; a literal password moves into a variable, so the project only references it.
fn postgres(scheme: &str, body: &str, locator: &str, role: Role) -> Result<Endpoint, String> {
    let problem = |what: &str| {
        format!(
            "the {} {scheme}://...: {what}; write it as {scheme}://user:${{ENV:PGPASSWORD}}@host:5432/database#schema.table",
            role.label()
        )
    };
    let (userinfo, place) = body
        .rsplit_once('@')
        .ok_or_else(|| problem("it names no user"))?;
    let (host_port, database) = place
        .split_once('/')
        .filter(|(_, database)| !database.is_empty() && !database.contains(['/', '?']))
        .ok_or_else(|| problem("it names no database, or has a query"))?;
    let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));
    let (host, port) = host_port.split_once(':').unwrap_or((host_port, "5432"));
    let port: i64 = port
        .parse()
        .map_err(|_| problem(&format!("port {port} is not a number")))?;
    let password = decoded(password);
    let literal = !password.is_empty() && !password.contains("${");
    let secret = literal.then(|| (role.password_variable().to_owned(), password.clone()));
    let mut table = settings(&[
        ("kind", "postgres"),
        ("host", host),
        ("user", &decoded(user)),
        ("database", &decoded(database)),
    ]);
    table.insert("port".to_owned(), Value::Integer(port));
    let reference = format!("${{ENV:{}}}", role.password_variable());
    match (password.is_empty(), literal) {
        (true, _) => {}
        (false, true) => {
            table.insert("password".to_owned(), Value::String(reference));
        }
        (false, false) => {
            table.insert("password".to_owned(), Value::String(password));
        }
    }
    let shown_user = match userinfo.split_once(':') {
        Some((user, _)) => format!("{user}:{REDACTED}"),
        None => user.to_owned(),
    };
    Ok(Endpoint {
        connection: role.connection().to_owned(),
        settings: table,
        locator: locator.to_owned(),
        secret,
        shown: format!("{scheme}://{shown_user}@{place}#{locator}"),
    })
}

/// `s3://bucket/prefix/`, with `?region=...` and `&endpoint=...`.
fn s3(text: &str, body: &str, role: Role) -> Result<Endpoint, String> {
    let (path, query) = body.split_once('?').unwrap_or((body, ""));
    let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
    let mut table = settings(&[("kind", "s3"), ("bucket", bucket)]);
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if !S3_SETTINGS.contains(&key) {
            return Err(format!(
                "the {} {text} sets {key}; an s3:// URI takes only {}",
                role.label(),
                S3_SETTINGS.join(" and ")
            ));
        }
        table.insert(key.to_owned(), Value::String(decoded(value)));
    }
    Ok(Endpoint {
        connection: role.connection().to_owned(),
        settings: table,
        locator: prefix.to_owned(),
        secret: None,
        shown: text.to_owned(),
    })
}

/// A local folder: the connection's root, with nothing after it.
fn folder(path: &str, shown: &str, role: Role) -> Endpoint {
    Endpoint {
        connection: role.connection().to_owned(),
        settings: settings(&[("kind", "filesystem"), ("root", path)]),
        locator: String::new(),
        secret: None,
        shown: shown.to_owned(),
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
            .and_then(|hex| u8::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok());
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
