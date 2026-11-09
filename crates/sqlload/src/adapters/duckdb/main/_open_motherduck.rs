//! Open a MotherDuck database.

use crate::adapters::duckdb::_helpers::motherduck_connection::{
    chosen_token, connection_string, environment_variable, path_text, refused,
};
use crate::adapters::duckdb::_helpers::shared_destination::shared_destination;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::DuckDbDestination;
use crate::project::models::ConnectionConfig;

/// Open the MotherDuck database of `connection` (`target_database` when the target names one) through DuckDB's `md:` connection, signed in with its token, else the token in the `motherduck_token` or `MOTHERDUCK_TOKEN` variable. Without a token MotherDuck would wait for a browser sign-in, so it is refused instead.
pub(in crate::adapters) fn open_motherduck(
    connection: &ConnectionConfig,
    target_database: Option<&str>,
) -> Result<DuckDbDestination, DuckDbError> {
    let ConnectionConfig::MotherDuck { database, token } = connection else {
        return Err(DuckDbError::MotherDuck(format!(
            "a {} connection is not a MotherDuck one",
            connection.kind()
        )));
    };
    let shown = path_text(target_database.or(database.as_deref()).unwrap_or_default())?;
    let token = chosen_token(token.as_ref(), environment_variable).ok_or_else(|| {
        DuckDbError::MotherDuck(format!(
            "{shown} needs an access token: set token in the connection, or the motherduck_token or MOTHERDUCK_TOKEN environment variable"
        ))
    })?;
    let connection = duckdb::Connection::open(connection_string(&shown, &token))
        .map_err(|error| refused(&error.to_string(), &token, &shown))?;
    shared_destination(connection, true)
}
