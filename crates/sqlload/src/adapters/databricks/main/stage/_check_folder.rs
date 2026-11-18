//! Check that Databricks can read a folder of files through its external location.

use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::databricks::models::{DatabricksDestination, DatabricksSql};
use crate::databricks_api::main::execute_statement::execute_statement;
use crate::http::errors::ApiError;

/// Check that `destination`'s service principal can read `folder`, which a location it sees covers: seeing a location does not grant READ FILES on it, which `LIST` needs as `COPY INTO` does.
pub(in crate::adapters) async fn check_folder(
    destination: &DatabricksDestination,
    folder: &str,
) -> Result<(), ApiError> {
    let statement = format!(
        "LIST {} LIMIT 1",
        DatabricksSql.render_loader_value_literal(folder)
    );
    execute_statement(&destination.api, &statement)
        .await
        .map(|_| ())
}
