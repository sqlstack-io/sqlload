use crate::column_types::models::Warehouse;
use crate::write_sql::models::WriteMode;

pub(crate) struct WriteStatementsTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    pub(crate) mode: WriteMode,
    pub(crate) expected_statements: &'static [&'static str],
}
