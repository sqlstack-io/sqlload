//! Open a MotherDuck database.

use std::sync::Arc;

use crate::adapters::duckdb::_helpers::motherduck_connection::{
    chosen_token, connection_string, environment_variable, path_text, query_value, refused,
};
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::{DuckDbConnection, DuckDbDestination};
use crate::adapters::motherduck_bridge::main::_open_bridge::open_bridge;
use crate::project::models::ConnectionConfig;

/// Open the MotherDuck database of `connection` (`target_database` when the target names one) through DuckDB's `md:` connection on the MotherDuck bridge, signed in with its token, else the token in the `motherduck_token` or `MOTHERDUCK_TOKEN` variable. Without a token MotherDuck would wait for a browser sign-in, so it is refused instead. `python` runs the bridge; `None` picks it as the bridge does.
pub(in crate::adapters) fn open_motherduck(
    connection: &ConnectionConfig,
    target_database: Option<&str>,
    python: Option<&str>,
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
    let config = serde_json::json!({
        "custom_user_agent": format!("sqlload/{}", env!("CARGO_PKG_VERSION")),
    });
    let hidden = vec![query_value(&token), token.clone()];
    let (bridge, _) = open_bridge(&connection_string(&shown, &token), config, (python, hidden))
        .map_err(|error| refused(&error.to_string(), &token, &shown))?;
    Ok(DuckDbDestination {
        connection: DuckDbConnection::Bridge(bridge),
        ddl: Arc::default(),
        motherduck: true,
    })
}
