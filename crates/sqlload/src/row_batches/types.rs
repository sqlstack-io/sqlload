/// One column of a batch: its field and values.
pub type NamedColumn = (std::sync::Arc<arrow_schema::Field>, arrow_array::ArrayRef);
