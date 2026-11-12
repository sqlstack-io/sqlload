//! The parts of opening a MotherDuck database: its path, token and connection string, and why it did not open.

use crate::adapters::duckdb::constants::{
    EXTENSION_EXCEPTION, MOTHERDUCK_TOKEN_VARIABLES, MOTHERDUCK_UNSUPPORTED_DUCKDB,
};
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::motherduck_bridge::constants::{PYTHON_VARIABLE, SUPPORTED_DUCKDB};
use crate::auth::models::Secret;
use crate::project::constants::MOTHERDUCK_PREFIX;
use crate::project::main::motherduck_path::motherduck_path;

/// `database` as an `md:` path, with its options. A token in its options or name is refused, as the path appears in messages.
pub(in crate::adapters::duckdb) fn path_text(database: &str) -> Result<String, DuckDbError> {
    let parts = motherduck_path(database);
    if parts.options.to_ascii_lowercase().contains("token")
        || parts
            .name
            .is_some_and(|name| name.to_ascii_lowercase().contains("motherduck_token"))
    {
        return Err(DuckDbError::MotherDuck(
            "a motherduck connection's database holds a token; set it in token instead".to_owned(),
        ));
    }
    let options = match parts.options {
        "" => String::new(),
        options => format!("?{options}"),
    };
    Ok(format!(
        "{MOTHERDUCK_PREFIX}{}{options}",
        parts.name.unwrap_or_default()
    ))
}

/// The token to sign in with: `token`, else the first non-empty variable of `MOTHERDUCK_TOKEN_VARIABLES` that `variable` reads.
pub(in crate::adapters::duckdb) fn chosen_token(
    token: Option<&Secret>,
    variable: impl Fn(&str) -> Option<String>,
) -> Option<String> {
    if let Some(token) = token {
        return Some(token.expose().to_owned()).filter(|token| !token.is_empty());
    }
    for name in MOTHERDUCK_TOKEN_VARIABLES {
        if let Some(token) = variable(name)
            && !token.is_empty()
        {
            return Some(token);
        }
    }
    None
}

/// `name`'s value in the process environment, when set as text.
pub(in crate::adapters::duckdb) fn environment_variable(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent | std::env::VarError::NotUnicode(_)) => None,
    }
}

/// The connection string DuckDB opens: `path` with the percent-encoded `token` as its `motherduck_token` option.
pub(in crate::adapters::duckdb) fn connection_string(path: &str, token: &str) -> String {
    let separator = match path.contains('?') {
        true => '&',
        false => '?',
    };
    format!("{path}{separator}motherduck_token={}", query_value(token))
}

/// Why `shown` did not open, from the bridge's `error` with `token` taken out: MotherDuck's own words when its extension refuses the bridge's DuckDB version.
pub(in crate::adapters::duckdb) fn refused(error: &str, token: &str, shown: &str) -> DuckDbError {
    let error = error
        .replace(&query_value(token), "<redacted>")
        .replace(token, "<redacted>");
    if !error.contains(MOTHERDUCK_UNSUPPORTED_DUCKDB) {
        return DuckDbError::MotherDuck(format!("cannot open {shown}: {error}"));
    }
    let said = error
        .rsplit_once(EXTENSION_EXCEPTION)
        .map_or(error.as_str(), |(_, said)| said)
        .trim_matches('"')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    DuckDbError::MotherDuck(format!(
        "cannot open {shown}: MotherDuck doesn't accept the version of Python's duckdb package that SQLLoad connects through: install {SUPPORTED_DUCKDB} there (pip install 'sqlload[motherduck]'), or set {PYTHON_VARIABLE} to a Python that has it. MotherDuck says: {said}"
    ))
}

/// `value` percent-encoded for a connection string's query, keeping only unreserved characters, so a token holding `&`, `=` or `#` reaches MotherDuck whole.
pub(in crate::adapters::duckdb) fn query_value(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(char::from(byte));
            }
            byte => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}
