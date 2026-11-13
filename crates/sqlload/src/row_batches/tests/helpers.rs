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
use crate::row_batches::main::projection::projection;
use crate::row_batches::main::settled_schema::settled_schema;
use crate::row_batches::models::{JsonLayout, JsonRead};
use crate::row_batches::tests::test_types::{
    FlattenBatchTestCase, JsonRowsTestCase, ProjectionTestCase,
};

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
        projection: None,
    };
    let mut batches: Vec<String> = Vec::new();
    json_rows(test_case.text.as_bytes(), &read, &mut |columns| {
        batches.push(render(&columns_batch(columns)?));
        Ok(())
    })?;
    Ok(batches)
}

/// The batches `json_rows` produces for the projection test case, rendered; a projection that can't be built is its error.
pub(crate) fn read_projected(test_case: &ProjectionTestCase) -> Result<Vec<String>, String> {
    let fields: Option<Vec<(String, String, bool)>> = test_case.fields.map(|fields| {
        fields
            .iter()
            .map(|(name, path, required)| ((*name).to_owned(), (*path).to_owned(), *required))
            .collect()
    });
    let read = JsonRead {
        layout: test_case.layout,
        path: None,
        batch_rows: test_case.batch_rows,
        projection: projection(fields.as_deref(), test_case.keep_record)?,
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
        projection: None,
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
        projection: None,
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

/// `texts` converted by [`json_lists`](crate::row_batches::main::json_lists::json_lists) to lists of `element`, joined by `|` with NULL as `null`; an error up to its first `: `.
pub(crate) fn shown_json_lists(
    texts: &[Option<&str>],
    element: &crate::column_types::models::ColumnType,
) -> Result<String, String> {
    let texts: arrow_array::ArrayRef =
        std::sync::Arc::new(arrow_array::StringArray::from(texts.to_vec()));
    let list_type = crate::column_types::main::arrow_field::arrow_field(
        "",
        &crate::column_types::models::ColumnType::Array(Box::new(element.clone())),
    )
    .ok_or("no Arrow list type")?
    .data_type()
    .clone();
    let cut = |error: String| error.split(": ").next().unwrap_or_default().to_owned();
    let lists =
        crate::row_batches::main::json_lists::json_lists(&texts, &list_type).map_err(cut)?;
    let options = arrow_cast::display::FormatOptions::default().with_null("null");
    let formatter = arrow_cast::display::ArrayFormatter::try_new(lists.as_ref(), &options)
        .map_err(|error| error.to_string())?;
    Ok((0..lists.len())
        .map(|row| formatter.value(row).to_string())
        .collect::<Vec<_>>()
        .join("|"))
}

/// `test_case`'s rows split by [`unnest_batch`](crate::row_batches::main::unnest_batch::unnest_batch) into `orders` and its child tables under SQLLoad's naming: the parent rendered, then each child as `table = rendered`.
pub(crate) fn unnested(
    test_case: &crate::row_batches::tests::test_types::UnnestBatchTestCase,
) -> Result<Vec<String>, String> {
    let read = JsonRead {
        layout: JsonLayout::Lines,
        path: None,
        batch_rows: usize::MAX,
        projection: None,
    };
    let mut batches: Vec<RecordBatch> = Vec::new();
    json_rows(test_case.rows.as_bytes(), &read, |columns| {
        batches.push(columns_batch(columns)?);
        Ok(())
    })?;
    let batch = batches.first().ok_or("no rows")?;
    let ids: Vec<String> = test_case.ids.iter().map(|id| (*id).to_owned()).collect();
    let roots: Option<Vec<String>> = test_case
        .roots
        .map(|roots| roots.iter().map(|id| (*id).to_owned()).collect());
    let unnest = (test_case.unnest)();
    let request = crate::row_batches::models::UnnestRequest {
        table: "orders",
        unnest: &unnest,
        max_depth: test_case.max_depth,
        naming: crate::load_naming::models::Naming::Sqlload,
        ids: &ids,
        roots: roots.as_deref(),
    };
    let mut choices: crate::row_batches::types::ListChoices = test_case
        .chosen
        .iter()
        .map(|(path, chosen)| ((*path).to_owned(), *chosen))
        .collect();
    let split =
        crate::row_batches::main::unnest_batch::unnest_batch(batch, &request, &mut choices)?;
    let mut shown = vec![render(&split.parent)];
    shown.extend(
        split
            .children
            .iter()
            .map(|child| format!("{} = {}", child.table, render(&child.batch))),
    );
    let chosen: Vec<String> = choices
        .iter()
        .map(|(path, chosen)| format!("{path}={chosen}"))
        .collect();
    shown.extend(
        (unnest == crate::row_batches::models::Unnest::All)
            .then(|| format!("chosen: {}", chosen.join(","))),
    );
    Ok(shown)
}

/// `unnest` naming `items`, `items.parts` and `tags`.
pub(crate) fn items_parts_tags() -> crate::row_batches::models::Unnest {
    crate::row_batches::models::Unnest::Paths(vec![
        "items".to_owned(),
        "items.parts".to_owned(),
        "tags".to_owned(),
    ])
}

/// `unnest = "all"`.
pub(crate) fn every_list() -> crate::row_batches::models::Unnest {
    crate::row_batches::models::Unnest::All
}

/// `unnest` naming `tags` alone.
pub(crate) fn tags_only() -> crate::row_batches::models::Unnest {
    crate::row_batches::models::Unnest::Paths(vec!["tags".to_owned()])
}
