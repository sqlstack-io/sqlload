use crate::project::constants::{DEFAULT_POSTGRES_PORT, DEFAULT_TABLE_NAME};

pub(crate) fn postgres_port() -> u16 {
    DEFAULT_POSTGRES_PORT
}

pub(crate) fn table_name() -> String {
    DEFAULT_TABLE_NAME.to_owned()
}
