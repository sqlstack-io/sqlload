//! Start MySQL.

use std::time::Duration;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use crate::odbc_servers::constants::{
    DATABASE, MYSQL_IMAGE, MYSQL_PORT, MYSQL_READY, MYSQL_TAG, PASSWORD, USER,
};
use crate::odbc_servers::models::OdbcServer;

/// A MySQL server with database `app` and user `sqlload`, on a free host port, and a connection string for MySQL Connector/ODBC that sets the session's SQL mode to read ANSI double quotes, as SQLLoad writes them.
pub async fn start_mysql() -> Result<OdbcServer, String> {
    let container = GenericImage::new(MYSQL_IMAGE, MYSQL_TAG)
        .with_exposed_port(MYSQL_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr(MYSQL_READY))
        .with_env_var("MYSQL_ROOT_PASSWORD", PASSWORD)
        .with_env_var("MYSQL_DATABASE", DATABASE)
        .with_env_var("MYSQL_USER", USER)
        .with_env_var("MYSQL_PASSWORD", PASSWORD)
        .with_startup_timeout(Duration::from_secs(180))
        .start()
        .await
        .map_err(|error| format!("cannot start MySQL: {error}"))?;
    let port = container
        .get_host_port_ipv4(MYSQL_PORT.tcp())
        .await
        .map_err(|error| error.to_string())?;
    Ok(OdbcServer {
        connection_string: format!(
            "Driver={{MySQL ODBC Unicode Driver}};Server=127.0.0.1;Port={port};Database={DATABASE};Uid={USER};Pwd={PASSWORD};INITSTMT=SET SESSION sql_mode=CONCAT(@@SESSION.sql_mode,',ANSI_QUOTES');"
        ),
        container,
    })
}
