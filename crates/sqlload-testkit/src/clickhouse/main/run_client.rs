//! Run SQL with `clickhouse-client` inside the container.

use testcontainers::core::ExecCommand;

use crate::clickhouse::constants::{PASSWORD, USER};
use crate::clickhouse::models::ClickHouse;

/// Run `query` with `clickhouse-client` in the container, with extra client `settings` (such as `--allow_deprecated_database_ordinary=1`, which SQLLoad never sends); its output.
pub async fn run_client(
    clickhouse: &ClickHouse,
    query: &str,
    settings: &[&str],
) -> Result<String, String> {
    let mut command = vec![
        "clickhouse-client",
        "--user",
        USER,
        "--password",
        PASSWORD,
        "--query",
        query,
    ];
    command.extend_from_slice(settings);
    let mut result = clickhouse
        .container
        .exec(ExecCommand::new(command))
        .await
        .map_err(|error| error.to_string())?;
    let output = result
        .stdout_to_vec()
        .await
        .map_err(|error| error.to_string())?;
    let errors = result
        .stderr_to_vec()
        .await
        .map_err(|error| error.to_string())?;
    match result
        .exit_code()
        .await
        .map_err(|error| error.to_string())?
    {
        Some(0) => Ok(String::from_utf8_lossy(&output).trim().to_owned()),
        code => Err(format!(
            "clickhouse-client exited with {code:?}: {}",
            String::from_utf8_lossy(&errors)
        )),
    }
}
