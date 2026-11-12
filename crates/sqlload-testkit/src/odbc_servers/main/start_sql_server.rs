//! Start SQL Server.

use std::time::Duration;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use crate::odbc_servers::constants::{
    SQL_SERVER_IMAGE, SQL_SERVER_PASSWORD, SQL_SERVER_PORT, SQL_SERVER_READY, SQL_SERVER_TAG,
};
use crate::odbc_servers::models::OdbcServer;

/// A SQL Server (Developer edition, licence accepted for testing) on a free host port, and a connection string for Microsoft's ODBC Driver 18 that signs in as `sa` to `master` and trusts the server's own certificate.
pub async fn start_sql_server() -> Result<OdbcServer, String> {
    let container = GenericImage::new(SQL_SERVER_IMAGE, SQL_SERVER_TAG)
        .with_exposed_port(SQL_SERVER_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout(SQL_SERVER_READY))
        .with_env_var("ACCEPT_EULA", "Y")
        .with_env_var("MSSQL_SA_PASSWORD", SQL_SERVER_PASSWORD)
        .with_startup_timeout(Duration::from_secs(180))
        .start()
        .await
        .map_err(|error| format!("cannot start SQL Server: {error}"))?;
    let port = container
        .get_host_port_ipv4(SQL_SERVER_PORT.tcp())
        .await
        .map_err(|error| error.to_string())?;
    Ok(OdbcServer {
        connection_string: format!(
            "Driver={{ODBC Driver 18 for SQL Server}};Server=127.0.0.1,{port};Database=master;Uid=sa;Pwd={SQL_SERVER_PASSWORD};TrustServerCertificate=yes;"
        ),
        container,
    })
}
