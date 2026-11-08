use crate::project::constants::{DEFAULT_CLICKHOUSE_USER, DEFAULT_POSTGRES_PORT};

pub(crate) fn postgres_port() -> u16 {
    DEFAULT_POSTGRES_PORT
}

pub(crate) fn clickhouse_user() -> String {
    DEFAULT_CLICKHOUSE_USER.to_owned()
}
