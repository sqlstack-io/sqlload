//! Start a database an ODBC driver test reads.

use crate::odbc_servers::_helpers::containers::{
    start_mysql, start_postgres_server, start_sql_server,
};
use crate::odbc_servers::models::{OdbcDatabase, OdbcServer};

/// A server of `database` on a free host port, and the connection string its ODBC driver reaches it by.
pub async fn start_odbc_server(database: OdbcDatabase) -> Result<OdbcServer, String> {
    match database {
        OdbcDatabase::SqlServer => start_sql_server().await,
        OdbcDatabase::MySql => start_mysql().await,
        OdbcDatabase::Postgres => start_postgres_server().await,
    }
}
