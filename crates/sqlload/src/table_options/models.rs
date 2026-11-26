use crate::column_parse::models::ColumnParse;
use crate::column_types::models::Warehouse;
use crate::load_naming::models::{Compat, Naming};
use crate::loader::models::{CdcSpec, CursorSpec, SettingOrigin, TableLayout};
use crate::project::models::{
    Deletes, NullKeys, OnDuplicateKeys, OnOverlappingFiles, SourceConfig, TableChecks,
    WriteStrategy,
};
use crate::row_batches::models::{Flatten, Unnest};

/// What a table's options read of the target it loads into.
#[derive(Debug, Clone)]
pub struct OptionTarget {
    pub warehouse: Warehouse,
    /// The table folder template, for a files target.
    pub path: Option<String>,
    pub naming: Option<Naming>,
    pub compat: Option<Compat>,
}

/// The source a table belongs to, by name, and the target it loads into.
#[derive(Debug, Clone, Copy)]
pub struct OptionContext<'a> {
    pub name: &'a str,
    pub source: &'a SourceConfig,
    pub target: &'a OptionTarget,
}

/// A table's write strategy (its own, its source's, or a `postgres_cdc` source's `append`), cursor and keys it is replaced by.
#[derive(Debug, Clone)]
pub struct TableStrategy {
    pub cursor: Option<CursorSpec>,
    pub cdc: Option<CdcSpec>,
    pub strategy: WriteStrategy,
    pub strategy_origin: SettingOrigin,
    /// It merges on a `unique_key` (not a captured table's merge).
    pub merges: bool,
    pub replace_key: Vec<String>,
    /// A `delete_insert` by `replace_key`.
    pub keyed: bool,
}

/// What a table's options come to once they are checked together.
#[derive(Debug, Clone)]
pub struct TableOptions {
    /// The strategy it loads with: a captured table's is `append`.
    pub strategy: WriteStrategy,
    pub on_duplicate_keys: Option<(OnDuplicateKeys, SettingOrigin)>,
    pub newest_by: Vec<String>,
    pub overlapping: Option<OnOverlappingFiles>,
    pub null_keys: Option<NullKeys>,
    pub deletes: Option<Deletes>,
    pub checks: Option<TableChecks>,
    pub nesting: Nesting,
    pub layout: TableLayout,
    pub parses: std::collections::BTreeMap<String, ColumnParse>,
    pub timezone: Option<(chrono_tz::Tz, SettingOrigin)>,
}

/// What a table's nesting settings come to.
#[derive(Debug, Clone)]
pub struct Nesting {
    pub naming: Naming,
    pub compat: Compat,
    pub row_id: bool,
    pub unnest: Unnest,
    pub max_depth: u32,
    pub flatten: Flatten,
}
