//! Run several statements in one session.

use crate::http::errors::ApiError;
use crate::sql_api::_helpers::requests::script_body;
use crate::sql_api::_helpers::run::run_body;
use crate::sql_api::models::SqlClient;

/// Run `statements` in order in one session (a `BEGIN` ... `COMMIT` among them is one transaction); stops at the first failure.
pub async fn execute_script(client: &SqlClient, statements: &[String]) -> Result<(), ApiError> {
    run_body(client, script_body(client, statements))
        .await
        .map(|_| ())
}
