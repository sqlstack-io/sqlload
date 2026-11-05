//! Run several statements in one session and keep what each returned.

use crate::http::errors::ApiError;
use crate::sql_api::_helpers::requests::script_body;
use crate::sql_api::_helpers::run::run_script;
use crate::sql_api::models::{SqlClient, SqlRows};

/// Run `statements` in order in one session, one request; what each returned, in order. Stops at the first failure.
pub async fn execute_script_rows(
    client: &SqlClient,
    statements: &[String],
) -> Result<Vec<SqlRows>, ApiError> {
    run_script(client, script_body(client, statements)).await
}
