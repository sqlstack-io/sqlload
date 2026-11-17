pub(crate) struct ParquetColumnTestCase {
    pub(crate) description: &'static str,
    pub(crate) column: &'static str,
    pub(crate) expected_type: arrow_schema::DataType,
    /// Reads the first row's value as text.
    pub(crate) read: fn(&dyn arrow_array::Array) -> String,
    pub(crate) expected_value: &'static str,
}

pub(crate) struct LoadSelectTestCase {
    pub(crate) description: &'static str,
    pub(crate) time_zone: &'static str,
    pub(crate) column: &'static str,
    pub(crate) field: &'static str,
    pub(crate) sql_type: &'static str,
    pub(crate) expected_expression: &'static str,
}

pub(crate) struct DatabricksCopyTestCase {
    pub(crate) description: &'static str,
    /// Column names and types.
    pub(crate) columns: &'static [(&'static str, &'static str)],
    pub(crate) files: &'static [&'static str],
    pub(crate) expected_statement: &'static str,
}
