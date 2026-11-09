//! Open a MotherDuck database.

use crate::adapters::duckdb::constants::{
    EXTENSION_EXCEPTION, MOTHERDUCK_PREFIX, MOTHERDUCK_TOKEN_VARIABLES,
    MOTHERDUCK_UNSUPPORTED_DUCKDB,
};
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::main::_open_duckdb::shared_destination;
use crate::adapters::duckdb::models::DuckDbDestination;
use crate::auth::models::Secret;

/// Open MotherDuck's `database` (with or without `md:`; the account's default database when `None`) through DuckDB's `md:` connection, signed in with `token`, else the token in the `motherduck_token` or `MOTHERDUCK_TOKEN` variable. Without a token MotherDuck would wait for a browser sign-in, so it is refused instead.
pub(in crate::adapters) fn open_motherduck(
    database: Option<&str>,
    token: Option<&Secret>,
) -> Result<DuckDbDestination, DuckDbError> {
    let database = database.unwrap_or_default();
    let shown = format!(
        "{MOTHERDUCK_PREFIX}{}",
        database.strip_prefix(MOTHERDUCK_PREFIX).unwrap_or(database)
    );
    let token = token
        .map(|token| token.expose().to_owned())
        .or_else(|| {
            MOTHERDUCK_TOKEN_VARIABLES
                .iter()
                .find_map(|name| std::env::var(name).ok())
        })
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            DuckDbError::MotherDuck(format!(
                "{shown} needs an access token: set token in the connection, or the motherduck_token environment variable"
            ))
        })?;
    let separator = match shown.contains('?') {
        true => '&',
        false => '?',
    };
    let connection = duckdb::Connection::open(format!(
        "{shown}{separator}motherduck_token={}",
        query_value(&token)
    ))
    .map_err(|error| refused(&error.to_string(), &token, &shown))?;
    shared_destination(connection, true)
}

/// Why `shown` did not open, from DuckDB's `error` with `token` taken out: MotherDuck's own words when its extension refuses this DuckDB version.
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
        "cannot open {shown}: MotherDuck doesn't accept the DuckDB version this SQLLoad bundles yet, so no SQLLoad release can connect to it until it does. MotherDuck says: {said}"
    ))
}

/// `value` percent-encoded for a connection string's query, keeping only unreserved characters, so a token holding `&`, `=` or `#` reaches MotherDuck whole.
pub(in crate::adapters::duckdb) fn query_value(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                char::from(byte).to_string()
            }
            byte => format!("%{byte:02X}"),
        })
        .collect()
}
