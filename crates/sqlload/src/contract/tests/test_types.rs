use crate::column_types::models::{ColumnType, Warehouse};
use crate::loader::models::TableSpec;
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
    /// The table's settings: sqlload.toml lines for Postgres table `public.orders` of source `app` (source columns `id`, `Status`, `amount`, `note`), or JSON for Python table `t` declared in `src/s.py`.
    pub(crate) settings: &'static str,
    /// The table's spec from `settings` on a warehouse.
    pub(crate) spec: fn(&str, Warehouse) -> Result<TableSpec, String>,
    pub(crate) warehouse: Warehouse,
    pub(crate) live: Vec<(&'static str, ColumnType)>,
    pub(crate) overwrite: bool,
    /// What `contract generate` prints for the table.
    pub(crate) expected_lines: &'static [&'static str],
}

/// sqlload.toml before and after writing the adoptions.
pub(crate) struct WriteTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    /// The table's description (its source comment when it has no entry).
    pub(crate) table_description: &'static str,
    /// The file written (empty when unchanged), then the problems left; or `error: ...`.
    pub(crate) expected_lines: &'static [&'static str],
}

/// Which sources `--from` would read from another kind of warehouse than their own.
pub(crate) struct OtherWarehouseTestCase {
    pub(crate) description: &'static str,
    pub(crate) from: Option<&'static str>,
    pub(crate) expected_sources: &'static [&'static str],
}

/// Every file's columns against the newest load's.
pub(crate) struct FileDriftTestCase {
    pub(crate) description: &'static str,
    pub(crate) live: Vec<(&'static str, ColumnType)>,
    pub(crate) files: Vec<(&'static str, Vec<(&'static str, ColumnType)>)>,
    pub(crate) expected_messages: &'static [&'static str],
}
