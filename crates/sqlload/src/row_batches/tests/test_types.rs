use arrow_array::RecordBatch;

use crate::row_batches::models::JsonLayout;

pub(crate) struct JsonRowsTestCase {
    pub(crate) description: &'static str,
    pub(crate) layout: JsonLayout,
    pub(crate) path: Option<&'static str>,
    pub(crate) batch_rows: usize,
    pub(crate) text: &'static str,
    /// Each batch rendered as `schema / rows`, or the error text.
    pub(crate) expected_batches: Result<&'static [&'static str], &'static str>,
}

pub(crate) struct MergeSchemaTestCase {
    pub(crate) description: &'static str,
    /// NDJSON texts, each read as one batch.
    pub(crate) batches: &'static [&'static str],
    /// Every batch cast to the settled merged schema, rendered as `schema / rows`.
    pub(crate) expected_table: &'static str,
}

pub(crate) struct NormaliseBatchTestCase {
    pub(crate) description: &'static str,
    pub(crate) batch: fn() -> RecordBatch,
    pub(crate) expected_batch: &'static str,
}

pub(crate) struct TextRowsTestCase {
    pub(crate) description: &'static str,
    pub(crate) column: arrow_array::ArrayRef,
    pub(crate) expected_values: &'static [Option<&'static str>],
}

pub(crate) struct FlattenBatchTestCase {
    pub(crate) description: &'static str,
    pub(crate) rows: &'static str,
    pub(crate) flatten: &'static [&'static str],
    pub(crate) expected_batch: Result<&'static str, &'static str>,
}

pub(crate) struct JsonListsTestCase {
    pub(crate) description: &'static str,
    /// JSON array texts, `None` for NULL.
    pub(crate) texts: &'static [Option<&'static str>],
    /// The type of each list's elements.
    pub(crate) element: crate::column_types::models::ColumnType,
    /// The lists joined by `|`, or the error up to its first `: `.
    pub(crate) expected_lists: Result<&'static str, &'static str>,
}
