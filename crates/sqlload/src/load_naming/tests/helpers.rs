use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use crate::load_naming::main::_dlt_child_table::dlt_child_table;

/// The child table of `orders` for `field`.
pub(crate) fn child_of_orders(field: &str, max_length: Option<usize>) -> String {
    dlt_child_table("orders", field, max_length)
}

/// One row of `1` in a field of each name.
pub(crate) fn int_batch(fields: &[&str]) -> RecordBatch {
    let schema = Schema::new(
        fields
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, true))
            .collect::<Vec<Field>>(),
    );
    let columns: Vec<ArrayRef> = fields
        .iter()
        .map(|_| Arc::new(Int64Array::from(vec![1])) as ArrayRef)
        .collect();
    RecordBatch::try_new(Arc::new(schema), columns).expect("batch")
}

/// `batch`'s field names, joined by `,`.
pub(crate) fn field_names(batch: &RecordBatch) -> String {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<String>>()
        .join(",")
}
