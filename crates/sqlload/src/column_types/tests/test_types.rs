use crate::column_types::models::{ColumnType, Warehouse};

pub(crate) struct ParseTypeTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    pub(crate) destinations: &'static [Warehouse],
    pub(crate) expected_type: ColumnType,
    pub(crate) expected_written_as: Option<&'static str>,
    pub(crate) expected_warnings: usize,
}

pub(crate) struct ParseTypeErrorTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    pub(crate) destinations: &'static [Warehouse],
    /// Text the error message must contain.
    pub(crate) expected_message: &'static str,
}

pub(crate) struct ArrowFieldTestCase {
    pub(crate) description: &'static str,
    pub(crate) column_type: ColumnType,
    /// The field's data type as Arrow prints it, or None when there is no field.
    pub(crate) expected_data_type: Option<&'static str>,
    pub(crate) expected_metadata: Option<&'static str>,
}

pub(crate) struct ExactCastTestCase {
    pub(crate) description: &'static str,
    pub(crate) input: arrow_array::ArrayRef,
    pub(crate) to: arrow_schema::DataType,
    /// The first value as it converts, or text the error must contain.
    pub(crate) expected: Result<&'static str, &'static str>,
}
