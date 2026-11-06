use std::sync::Arc;

use arrow_array::builder::{Int32Builder, ListBuilder};
use arrow_array::{
    ArrayRef, Float32Array, Int8Array, LargeStringArray, RecordBatch, StructArray,
    TimestampNanosecondArray, UInt64Array, new_null_array,
};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::column_types::constants::ARROW_TYPE_METADATA_KEY;
use crate::row_batches::main::columns_batch::columns_batch;
use crate::row_batches::main::conform_batch::conform_batch;
use crate::row_batches::main::flatten_batch::flatten_batch;
use crate::row_batches::main::json_rows::json_rows;
use crate::row_batches::main::merge_schema::merge_schema;
use crate::row_batches::main::settled_schema::settled_schema;
use crate::row_batches::models::{JsonLayout, JsonRead};
use crate::row_batches::tests::test_types::{FlattenBatchTestCase, JsonRowsTestCase};

/// `batch` as `name:type,... / v|v|...; ...`, with `(json)` after JSON columns and `NULL` for nulls.
pub(crate) fn render(batch: &RecordBatch) -> String {
    let schema = batch.schema();
    let fields: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| {
            let marker = field
                .metadata()
                .get(ARROW_TYPE_METADATA_KEY)
                .map_or(String::new(), |kind| format!("({kind})"));
            format!("{}:{}{marker}", field.name(), field.data_type())
        })
        .collect();
    let options = FormatOptions::default().with_null("NULL");
    let formatters: Vec<ArrayFormatter<'_>> = batch
        .columns()
        .iter()
        .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
        .collect::<Result<_, _>>()
        .unwrap_or_default();
    let rows: Vec<String> = (0..batch.num_rows())
        .map(|row| {
            formatters
                .iter()
                .map(|formatter| formatter.value(row).to_string())
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    format!("{} / {}", fields.join(","), rows.join("; "))
}

/// The batches `json_rows` produces for the test case, rendered.
pub(crate) fn read_json(test_case: &JsonRowsTestCase) -> Result<Vec<String>, String> {
    let read = JsonRead {
        layout: test_case.layout,
        path: test_case.path.map(str::to_owned),
        batch_rows: test_case.batch_rows,
    };
    let mut batches: Vec<String> = Vec::new();
    json_rows(test_case.text.as_bytes(), &read, &mut |columns| {
        batches.push(render(&columns_batch(columns)?));
        Ok(())
    })?;
    Ok(batches)
}

/// The NDJSON `texts` read as batches, merged, settled and cast to one schema, rendered as one batch.
pub(crate) fn merged_table(texts: &[&str]) -> Result<String, String> {
    let read = JsonRead {
        layout: JsonLayout::Lines,
        path: None,
        batch_rows: usize::MAX,
    };
    let mut batches: Vec<RecordBatch> = Vec::new();
    for text in texts {
        json_rows(text.as_bytes(), &read, &mut |columns| {
            batches.push(columns_batch(columns)?);
            Ok(())
        })?;
    }
    let merged = batches.iter().fold(Schema::empty(), |seen, batch| {
        merge_schema(&seen, &batch.schema())
    });
    let schema: SchemaRef = Arc::new(settled_schema(&merged));
    let conformed: Vec<RecordBatch> = batches
        .iter()
        .map(|batch| conform_batch(batch, &schema))
        .collect::<Result<_, String>>()?;
    let table = arrow_select::concat::concat_batches(&schema, &conformed)
        .map_err(|error| error.to_string())?;
    Ok(render(&table))
}

/// Narrow ints and floats, large strings, u64 values in and beyond i64, and nanosecond timestamps with an offset.
pub(crate) fn widths() -> RecordBatch {
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("i", Arc::new(Int8Array::from(vec![Some(1), None]))),
        ("f", Arc::new(Float32Array::from(vec![0.5, 1.25]))),
        ("s", Arc::new(LargeStringArray::from(vec!["a", "b"]))),
        ("u", Arc::new(UInt64Array::from(vec![1, 2]))),
        ("big", Arc::new(UInt64Array::from(vec![1, u64::MAX]))),
        (
            "t",
            Arc::new(
                TimestampNanosecondArray::from(vec![1_700_000_000_123_456_789, 0])
                    .with_timezone("+01:00"),
            ),
        ),
    ];
    RecordBatch::try_from_iter(columns)
        .unwrap_or_else(|_| RecordBatch::new_empty(Arc::new(Schema::empty())))
}

/// A list column and a struct column, with a null list.
pub(crate) fn nested() -> RecordBatch {
    let mut list = ListBuilder::new(Int32Builder::new());
    list.values().append_value(1);
    list.values().append_value(2);
    list.append(true);
    list.append(false);
    let inner: ArrayRef = Arc::new(LargeStringArray::from(vec![Some("x\"y"), None]));
    let record = StructArray::from(vec![(
        Arc::new(Field::new("k", DataType::LargeUtf8, true)),
        inner,
    )]);
    let columns: Vec<(&str, ArrayRef)> =
        vec![("l", Arc::new(list.finish())), ("r", Arc::new(record))];
    RecordBatch::try_from_iter(columns)
        .unwrap_or_else(|_| RecordBatch::new_empty(Arc::new(Schema::empty())))
}

/// One column of Arrow's null type.
pub(crate) fn all_null() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Null, true)]));
    let arrays: Vec<ArrayRef> = vec![new_null_array(&DataType::Null, 2)];
    RecordBatch::try_new(schema, arrays)
        .unwrap_or_else(|_| RecordBatch::new_empty(Arc::new(Schema::empty())))
}

/// The NDJSON `test_case.rows` read as one batch, then flattened as the case says; the rendered batch, or the error.
pub(crate) fn flattened(test_case: &FlattenBatchTestCase) -> Result<String, String> {
    let read = JsonRead {
        layout: JsonLayout::Lines,
        path: None,
        batch_rows: usize::MAX,
    };
    let mut batches: Vec<RecordBatch> = Vec::new();
    json_rows(test_case.rows.as_bytes(), &read, |columns| {
        batches.push(columns_batch(columns)?);
        Ok(())
    })?;
    let batch = batches.first().ok_or("no rows")?;
    let flatten: Vec<String> = test_case
        .flatten
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    flatten_batch(batch, &flatten).map(|batch| render(&batch))
}
