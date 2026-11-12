pub const MYSQL_IMAGE: &str = "mysql";
pub const MYSQL_TAG: &str = "8.4";
pub const MYSQL_PORT: u16 = 3306;
/// The line MySQL logs once the real server, not the image's setup server, listens on its port.
pub const MYSQL_READY: &str = "port: 3306  MySQL Community Server";
pub const SQL_SERVER_IMAGE: &str = "mcr.microsoft.com/mssql/server";
pub const SQL_SERVER_TAG: &str = "2022-latest";
pub const SQL_SERVER_PORT: u16 = 1433;
pub const SQL_SERVER_READY: &str = "SQL Server is now ready for client connections";
/// The SQL Server administrator's password, which must meet its complexity rules.
pub const SQL_SERVER_PASSWORD: &str = "Sqlload-Test-1";
pub const USER: &str = "sqlload";
pub const PASSWORD: &str = "sqlload";
pub const DATABASE: &str = "app";
