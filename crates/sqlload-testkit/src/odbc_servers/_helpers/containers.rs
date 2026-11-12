//! The database containers the ODBC drivers are tested against, each with its driver's connection string.

use std::time::Duration;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use crate::odbc_servers::constants::{
    DATABASE, MYSQL_IMAGE, MYSQL_PORT, MYSQL_READY, MYSQL_TAG, PASSWORD, SQL_SERVER_IMAGE,
    SQL_SERVER_PASSWORD, SQL_SERVER_PORT, SQL_SERVER_READY, SQL_SERVER_TAG, USER,
};
use crate::odbc_servers::models::OdbcServer;
use crate::postgres::main::connect::connect;
use crate::postgres::main::start_postgres::start_postgres;

/// A MySQL server with database `app` and user `sqlload`, on a free host port, and a connection string for MySQL Connector/ODBC that sets the session's SQL mode to read ANSI double quotes, as SQLLoad writes them.
pub(crate) async fn start_mysql() -> Result<OdbcServer, String> {
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

/// A SQL Server (Developer edition, licence accepted for testing) on a free host port, and a connection string for Microsoft's ODBC Driver 18 that signs in as `sa` to `master` and trusts the server's own certificate.
pub(crate) async fn start_sql_server() -> Result<OdbcServer, String> {
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

/// A Postgres server with logical replication, once it accepts connections, and a connection string for psqlODBC.
pub(crate) async fn start_postgres_server() -> Result<OdbcServer, String> {
    let postgres = start_postgres().await?;
    connect(&postgres).await?;
    Ok(OdbcServer {
        connection_string: format!(
            "Driver={{PostgreSQL Unicode}};Server=127.0.0.1;Port={};Database={DATABASE};Uid={USER};Pwd={PASSWORD};",
            postgres.port
        ),
        container: postgres.container,
    })
}
