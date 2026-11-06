use crate::column_types::models::{ColumnType, Warehouse};
use crate::project::models::{Contract, OnSchemaChange, Policy};

/// Where a desired column's type comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnOrigin {
    /// Pinned in the project or code.
    Pinned,
    /// Mapped from the source's own type (catalog or file schema).
    Source,
    /// Inferred from sampled values.
    Inferred,
    /// A `_sqlload_*` column SQLLoad adds.
    Metadata,
}

/// A column the load wants the table to have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredColumn {
    pub name: String,
    pub column_type: ColumnType,
    pub origin: ColumnOrigin,
}

/// A column the live table has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveColumn {
    pub name: String,
    pub column_type: ColumnType,
    /// Digits after the second a live time or timestamp column holds, where the warehouse reports them.
    pub fraction_digits: Option<i64>,
}

/// The rules for one table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaRules {
    pub warehouse: Warehouse,
    pub contract: Contract,
    pub on_schema_change: OnSchemaChange,
    /// Inferred columns widen along the ladder (bool -> int -> decimal, timestamp_ntz -> timestamp_tz) instead of failing.
    pub auto_widen: bool,
    pub type_change_rebuild: Policy,
    /// `--allow-type-change-rebuild` was given.
    pub rebuild_allowed: bool,
}

/// One change to an existing table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnChange {
    Add {
        column: DesiredColumn,
        sql_type: String,
    },
    /// Widen in place, such as a longer `VARCHAR`.
    Widen {
        column: String,
        from: ColumnType,
        to: ColumnType,
        sql_type: String,
    },
    /// Widening that rewrites the table (rebuild and swap).
    Rebuild {
        column: String,
        from: ColumnType,
        to: ColumnType,
        sql_type: String,
    },
}

impl std::fmt::Display for ColumnChange {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Add { column, sql_type } => write!(
                formatter,
                "add column  {} {} -> {sql_type}",
                column.name, column.column_type
            ),
            Self::Widen {
                column,
                from,
                to,
                sql_type,
            } => write!(
                formatter,
                "widen  {column} {from} -> {to} ({sql_type})  in place"
            ),
            Self::Rebuild {
                column,
                from,
                to,
                sql_type,
            } => write!(formatter, "rebuild  {column} {from} -> {to} ({sql_type})"),
        }
    }
}

/// What happens to the table before data is written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableAction {
    Create,
    /// Rebuild the existing table from the source with the current columns (`--reload`).
    Recreate,
    Alter(Vec<ColumnChange>),
    Unchanged,
}

impl TableAction {
    /// One line per change; `create` or `unchanged` otherwise.
    pub fn summary(&self) -> Vec<String> {
        match self {
            Self::Create => vec!["create".to_owned()],
            Self::Recreate => vec!["recreate".to_owned()],
            Self::Unchanged => vec!["unchanged".to_owned()],
            Self::Alter(changes) => changes.iter().map(ToString::to_string).collect(),
        }
    }
}

/// Why a table cannot load as planned, and how to let it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blocked {
    pub reasons: Vec<String>,
    /// A command-line flag that unblocks it, when one exists.
    pub fix_flag: Option<&'static str>,
}

/// The decision for one table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaPlan {
    /// The columns the source would load, pinned or not, before any is kept, added or refused; empty when planning stopped before the columns were known.
    pub desired: Vec<DesiredColumn>,
    pub action: TableAction,
    /// The columns each loaded row carries, in table order.
    pub write_columns: Vec<DesiredColumn>,
    pub warnings: Vec<String>,
    pub blocked: Option<Blocked>,
}
