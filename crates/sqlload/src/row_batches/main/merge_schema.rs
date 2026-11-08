//! Merge the schema of one more batch into the schema seen so far.

use arrow_schema::{DataType, Field, Schema};

use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::models::ColumnType;

/// `seen` with `batch`'s columns merged in: new columns are added at the end; a column seen as null takes the other type, `int` and `float` make `float` (also as list elements), a list and anything else make `json`, and any other difference makes `text`.
pub fn merge_schema(seen: &Schema, batch: &Schema) -> Schema {
    let mut fields: Vec<Field> = seen
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    for field in batch.fields() {
        match fields.iter().position(|seen| seen.name() == field.name()) {
            Some(position) => fields[position] = merge_field(&fields[position], field),
            None => fields.push(field.as_ref().clone()),
        }
    }
    Schema::new(fields)
}

fn merge_field(seen: &Field, field: &Field) -> Field {
    match (seen.data_type(), field.data_type()) {
        (left, right) if left == right && seen.metadata() == field.metadata() => seen.clone(),
        (DataType::Null, _) => field.clone(),
        (_, DataType::Null) => seen.clone(),
        (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64) => {
            Field::new(seen.name(), DataType::Float64, true)
        }
        (DataType::List(left), DataType::List(right))
            if *merge_field(left, right).data_type() == DataType::Float64 =>
        {
            let element = Field::new(left.name(), DataType::Float64, true);
            Field::new(seen.name(), DataType::List(element.into()), true)
        }
        (DataType::List(_), _) | (_, DataType::List(_)) => {
            arrow_field(seen.name(), &ColumnType::Json)
                .unwrap_or_else(|| Field::new(seen.name(), DataType::Utf8, true))
        }
        _ => Field::new(seen.name(), DataType::Utf8, true),
    }
}
