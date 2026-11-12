use testcontainers::{ContainerAsync, GenericImage};

/// A running database server and the ODBC connection string that reaches it; the container stops when this is dropped.
#[derive(Debug)]
pub struct OdbcServer {
    pub connection_string: String,
    pub container: ContainerAsync<GenericImage>,
}

/// A database the ODBC driver tests read through its own driver.
#[derive(Debug, Clone, Copy)]
pub enum OdbcDatabase {
    /// Microsoft's ODBC Driver 18 for SQL Server.
    SqlServer,
    /// MySQL Connector/ODBC.
    MySql,
    /// psqlODBC.
    Postgres,
}
