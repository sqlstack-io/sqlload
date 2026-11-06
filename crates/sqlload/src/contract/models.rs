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
}

/// Every selected table's comparison.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ContractReport {
    pub differences: usize,
    /// The target read instead of each table's own, from `--from`.
    pub from_target: Option<String>,
    pub resources: Vec<ContractResource>,
    /// Problems that stopped tables from being compared, such as a source that could not be listed.
    #[serde(skip)]
    pub problems: Vec<String>,
}
