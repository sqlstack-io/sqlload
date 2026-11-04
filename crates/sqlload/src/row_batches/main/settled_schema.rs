//! The schema to write once every batch has been seen.

use arrow_schema::{DataType, Field, Schema};

/// `schema` with columns that only ever held NULL as `text`.
pub fn settled_schema(schema: &Schema) -> Schema {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Null => Field::new(field.name(), DataType::Utf8, true),
            _ => field.as_ref().clone(),
        })
        .collect();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}
