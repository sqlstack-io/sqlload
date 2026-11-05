//! Run SQL statements in one session.

use crate::http::errors::ApiError;
use crate::adapters::snowflake::models::SnowflakeDestination;
use crate::sql_api::main::execute_script::execute_script;
use crate::sql_api::main::execute_sql::execute_sql;

/// Run `statements` in order in one session, so `BEGIN` ... `COMMIT` among them is one transaction.
pub async fn run_statements(
    destination: &SnowflakeDestination,
    statements: &[String],
) -> Result<(), ApiError> {
    match statements {
        [] => Ok(()),
        [statement] => execute_sql(&destination.sql, statement).await.map(|_| ()),
        _ => execute_script(&destination.sql, statements).await,
    }
}
