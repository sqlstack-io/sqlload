use serde::Serialize;

/// One difference between a table's declaration and its live table; field names follow `sqb contract diff --json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContractFinding {
    pub column: Option<String>,
    pub declared_type: Option<String>,
    pub kind: &'static str,
    pub message: String,
    pub physical_type: Option<String>,
}

/// One table's comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContractResource {
    pub findings: Vec<ContractFinding>,
    /// The live table, `schema.table`.
    pub relation: String,
    /// The table as selectors name it.
    pub resource_name: String,
    pub resource_type: &'static str,
    /// What `contract generate` would declare from the live table; absent for `diff` and when nothing changes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adoption: Option<Adoption>,
}

/// The declaration changes that adopt one live table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Adoption {
    /// Where the declaration lives: `[sources.<source>.tables.<table>]` in sqlload.toml, or the Python file declaring the table.
    pub location: String,
    /// Columns declared, or retyped under `--overwrite`, with their live type, in table order.
    pub columns: Vec<AdoptedColumn>,
    /// Declared columns the live table lacks, removed under `--overwrite`.
    pub removed: Vec<String>,
    /// For a Python table, the whole `columns={...}` argument to paste into its declaration, which is never edited.
    pub paste: Option<String>,
    /// The source and its `tables` key, for the sqlload.toml edit.
    #[serde(skip)]
    pub(crate) toml_key: (String, String),
    /// A database table's description, written into a `[sources.*.tables.*]` entry the edit creates, since an entry needs one; `None` for files and Python tables.
    #[serde(skip)]
    pub(crate) description: Option<String>,
}

/// One column an adoption declares.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AdoptedColumn {
    pub column: String,
    /// The type declared now, when the adoption retypes the column.
    pub declared_type: Option<String>,
    pub physical_type: String,
}

/// Which comparison `compare_contracts` runs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContractRequest {
    /// The target read instead of each table's own (`--from`).
    pub from: Option<String>,
    /// `contract generate`: propose declarations from the live tables; `Some(true)` also retypes declared columns and removes ones the table lacks (`--overwrite`).
    pub generate: Option<bool>,
}

/// Every selected table's comparison.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ContractReport {
    pub differences: usize,
    /// The target read instead of each table's own, from `--from`.
    pub from_target: Option<String>,
    pub resources: Vec<ContractResource>,
    /// Problems that stopped tables from being compared, such as a source that could not be listed; any makes the command fail.
    pub problems: Vec<String>,
    /// Files `contract generate --write` updated, relative to the project folder.
    pub written_paths: Vec<String>,
}
