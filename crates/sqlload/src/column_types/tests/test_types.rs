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
    /// The first value as it converts.
    pub(crate) expected_value: &'static str,
}

pub(crate) struct ExactCastErrorTestCase {
    pub(crate) description: &'static str,
    pub(crate) input: arrow_array::ArrayRef,
    pub(crate) to: arrow_schema::DataType,
    /// Text the error must contain.
    pub(crate) expected_message: &'static str,
}

/// A portable type a warehouse creates as a type that reads back as another, on purpose.
pub(crate) struct RoundTripException {
    /// Why the warehouse stores it as another type.
    pub(crate) description: &'static str,
    pub(crate) column_type: ColumnType,
    /// The warehouse's type for `column_type`.
    pub(crate) expected_rendered: &'static str,
    /// What that type reads back as, for a table loading only into the warehouse.
    pub(crate) expected_read_back: ColumnType,
}

/// A warehouse whose types a declaration takes, and the portable types it creates as types that read back as others.
pub(crate) struct RoundTripTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    pub(crate) expected_exceptions: Vec<RoundTripException>,
}
