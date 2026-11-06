use crate::column_types::models::ColumnType;
use crate::project::models::Contract;

pub(crate) struct FindingsTestCase {
    pub(crate) description: &'static str,
    pub(crate) declared: &'static [(&'static str, ColumnType)],
    pub(crate) contract: Contract,
    pub(crate) live: Option<&'static [(&'static str, ColumnType)]>,
    pub(crate) expected_messages: &'static [&'static str],
}
