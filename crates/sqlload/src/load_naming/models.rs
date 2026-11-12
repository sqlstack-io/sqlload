/// How a load names the columns it adds and the tables it generates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Naming {
    /// `_sqlload_*` names.
    #[default]
    Sqlload,
    /// dlt's `_dlt_*` names, where dlt has the column.
    Dlt,
}

/// A column a load adds beside the data, by what it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataRole {
    /// The load that wrote the row.
    LoadId,
    /// When that load started.
    LoadedAt,
    /// The row's own id (`row_id`, and always under dlt's naming).
    RowId,
}
