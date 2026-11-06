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

/// One table's adoption from its live columns.
pub(crate) struct AdoptionTestCase {
    pub(crate) description: &'static str,
    /// The table's sqlload.toml settings for Postgres table `public.orders` of source `app` (source columns `id`, `Status`, `amount`, `note`), or a Python table's settings as JSON.
    pub(crate) settings: &'static str,
    pub(crate) python: bool,
    pub(crate) warehouse: Warehouse,
    pub(crate) live: Vec<(&'static str, ColumnType)>,
    pub(crate) overwrite: bool,
    /// `+ column type`, `~ column type (declared type)`, `- column`, then `paste ...` for Python; empty when nothing changes.
    pub(crate) expected: &'static [&'static str],
}

/// sqlload.toml before and after writing the adoptions.
pub(crate) struct WriteTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    /// The table's description (its source comment when it has no entry).
    pub(crate) table_description: &'static str,
    /// The file written (empty when unchanged) and the problems left, or the error.
    pub(crate) expected: Result<(&'static str, &'static [&'static str]), &'static str>,
}
