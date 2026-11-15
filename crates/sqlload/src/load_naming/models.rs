/// How a load names the source's tables and columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Naming {
    /// dlt's snake_case: `OrderID` loads as `order_id`, shortened past the destination's name limit.
    #[default]
    SnakeCase,
    /// As the source spells them.
    Source,
}

/// Whose names the columns a load adds beside the data take.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compat {
    /// `_sqlload_*` names.
    #[default]
    Sqlload,
    /// dlt's `_dlt_*` names, where dlt has the column, with a row id on every row.
    Dlt,
}

/// What a load does with columns that would load under one name: repeated headers, and names that meet once `naming` changes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnDuplicateNames {
    /// The first keeps the name, the next ones get `_1`, `_2`, … skipping names already taken.
    #[default]
    Number,
    /// Fail the plan, naming the columns.
    Fail,
}

/// A column a load adds beside the data, by what it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataRole {
    /// The load that wrote the row.
    LoadId,
    /// When that load started.
    LoadedAt,
    /// The row's own id (`row_id`, and always under `compat = "dlt"`).
    RowId,
    /// A child row's parent row's id.
    ParentId,
    /// A child row's position in its parent's list, from 0.
    ListIndex,
    /// A child row's root row's id, under a merge.
    RootId,
}

/// One column of a table as its source gives it, for the name pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnName {
    /// Its place among the source's columns, from 1.
    pub position: usize,
    /// The name the source gives it: a repeated header's text, every time it repeats.
    pub source: String,
    /// The name a reader reads it by, unique in the table (a repeated header's is numbered: `Qty_1`).
    pub read_as: String,
}

/// What the name pipeline needs besides the columns.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NameRules {
    pub naming: Naming,
    /// The destination's longest name.
    pub max_length: Option<usize>,
    /// `rename` as a map, source name to loaded name.
    pub renamed: std::collections::BTreeMap<String, String>,
    pub on_duplicate_names: OnDuplicateNames,
    /// The names numbering gave this table before, which stay with their source names.
    pub kept: Vec<NumberedName>,
    /// The name `@sl.rename` registered the table's rename function under, when `rename` names one.
    pub function: Option<String>,
    /// The columns the table already has, when it exists: a clash group whose name is one of them, which `kept` doesn't say which source column holds, can't be numbered without guessing.
    pub existing: Vec<String>,
}

/// A column a rename function is given.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RenameColumn {
    /// Its place in the table, from 1.
    pub position: usize,
    pub source: String,
    /// The name it loads as without the rename: after `naming`.
    pub default: String,
}

/// Why a column loads under another name than its source's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum NameChange {
    /// Only `naming` changed it.
    Named,
    /// `rename` gave it its name.
    Renamed,
    /// Numbered after a clash.
    Numbered,
}

/// One column through the name pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedColumn {
    /// Its place in the table, from 1.
    pub position: usize,
    pub source: String,
    pub read_as: String,
    pub loaded: String,
    /// What named it: `rename` (even to its default name), numbering, or `naming` alone; `None` when it loads under its source name untouched.
    pub change: Option<NameChange>,
}

/// The columns of a table through the name pipeline, and the names numbering gave.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadedNames {
    pub columns: Vec<LoadedColumn>,
    /// Every column of each group numbering named, the first one included.
    pub numbered: Vec<NumberedName>,
    /// The names numbering gave in the table's earlier loads, as its run log recorded them, which these keep.
    pub kept: Vec<NumberedName>,
    /// Whether the load rebuilds the table (`--reload`) whose run log holds numbering from before, which its own record must replace even when it gives no numbers.
    pub replaces_record: bool,
}

/// The names a load has given the fields of a table whose columns arrive as it loads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NameMemo {
    /// Every field seen, in the order they first came.
    pub seen: Vec<String>,
    /// Each field's loaded name.
    pub loaded: std::collections::BTreeMap<String, String>,
    /// The fields through the name pipeline, as last run.
    pub columns: Vec<LoadedColumn>,
    /// The names numbering gave.
    pub numbered: Vec<NumberedName>,
}

/// A name numbering gave a column, which later loads keep for the same source column.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct NumberedName {
    /// The column it loads as.
    pub column: String,
    /// Its source name.
    pub source: String,
    /// How many columns with that source name come before it: 0, or a repeated header's place among its repeats.
    #[serde(default)]
    pub repeat: usize,
}
