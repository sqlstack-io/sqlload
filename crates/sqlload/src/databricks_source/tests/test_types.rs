use crate::column_types::models::ColumnType;

pub(crate) struct SourceColumnTestCase {
    pub(crate) description: &'static str,
    /// The type as `information_schema.columns.full_data_type` or a result's `type_text` writes it.
    pub(crate) sql_type: &'static str,
    /// The portable type; `None` when a type must be pinned.
    pub(crate) expected_type: Option<ColumnType>,
}

pub(crate) struct SelectQueryTestCase {
    pub(crate) description: &'static str,
    pub(crate) sql: Option<&'static str>,
    pub(crate) filter: Option<&'static str>,
    pub(crate) expected_query: &'static str,
}
