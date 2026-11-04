//! Run statements through the Snowflake SQL API.

use crate::http::errors::ApiError;
use crate::sql_api::_helpers::requests::statement_body;
use crate::sql_api::_helpers::run::run_body;
use crate::sql_api::models::{SqlClient, SqlRows};

/// Run one statement in the configured database, schema, warehouse and role, waiting for it to finish.
pub async fn execute_sql(client: &SqlClient, statement: &str) -> Result<SqlRows, ApiError> {
    run_body(client, statement_body(client, statement)).await
}
