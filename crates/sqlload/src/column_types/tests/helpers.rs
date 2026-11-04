use crate::column_types::models::{ColumnType, LiveColumnType};

/// A decimal type.
pub(crate) const fn decimal(precision: u8, scale: u8) -> ColumnType {
    ColumnType::Decimal { precision, scale }
}

/// A reported column type.
pub(crate) fn live(
    data_type: &str,
    precision: Option<i64>,
    scale: Option<i64>,
    char_length: Option<i64>,
) -> LiveColumnType {
    LiveColumnType {
        data_type: data_type.to_owned(),
        precision,
        scale,
        char_length,
    }
}
