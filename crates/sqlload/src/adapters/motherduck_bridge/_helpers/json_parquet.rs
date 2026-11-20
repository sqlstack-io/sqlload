//! Parquet schemas whose text columns hold JSON.

use std::sync::Arc;

use arrow_array::RecordBatch;
use parquet::arrow::ArrowSchemaConverter;
use parquet::basic::{ConvertedType, LogicalType, Type as PhysicalType};
use parquet::schema::types::{SchemaDescriptor, Type, TypePtr};

/// The Parquet schema of `batch`, with its text columns named in `json` typed `JSON`.
pub(crate) fn json_schema(
    batch: &RecordBatch,
    json: &[String],
) -> parquet::errors::Result<SchemaDescriptor> {
    let converted = ArrowSchemaConverter::new().convert(&batch.schema())?;
    let root = converted.root_schema();
    let fields = (root.get_fields().iter())
        .map(|field| json_field(field, json))
        .collect::<parquet::errors::Result<Vec<TypePtr>>>()?;
    Ok(SchemaDescriptor::new(Arc::new(
        Type::group_type_builder(root.name())
            .with_fields(fields)
            .build()?,
    )))
}

/// `field` typed `JSON` when it is a text column named in `json`, else as it is.
fn json_field(field: &TypePtr, json: &[String]) -> parquet::errors::Result<TypePtr> {
    let text = field.is_primitive() && field.get_physical_type() == PhysicalType::BYTE_ARRAY;
    if !(text && json.iter().any(|name| name == field.name())) {
        return Ok(field.clone());
    }
    Ok(Arc::new(
        Type::primitive_type_builder(field.name(), PhysicalType::BYTE_ARRAY)
            .with_repetition(field.get_basic_info().repetition())
            .with_logical_type(Some(LogicalType::Json))
            .with_converted_type(ConvertedType::JSON)
            .build()?,
    ))
}
