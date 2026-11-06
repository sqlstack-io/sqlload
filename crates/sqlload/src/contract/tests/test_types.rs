use crate::column_types::models::{ColumnType, Warehouse};
use crate::project::models::Contract;

pub(crate) struct FindingsTestCase {
    pub(crate) description: &'static str,
    pub(crate) declared: Vec<(&'static str, ColumnType)>,
    pub(crate) contract: Contract,
    pub(crate) warehouse: Warehouse,
    pub(crate) live: Option<Vec<(&'static str, ColumnType)>>,
    pub(crate) expected_messages: &'static [&'static str],
}
