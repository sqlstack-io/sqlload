use arrow_schema::Schema;

use crate::column_types::models::{ColumnType, Warehouse};

pub(crate) struct FittingTypeTestCase {
    pub(crate) description: &'static str,
    pub(crate) inferred: ColumnType,
    pub(crate) live: Option<ColumnType>,
    pub(crate) expected_type: ColumnType,
}

pub(crate) struct InferredTableTestCase {
    pub(crate) description: &'static str,
    /// The schema the yielded batches merged to.
    pub(crate) schema: fn() -> Schema,
    /// Columns declared in `columns` before any data.
    pub(crate) declared: &'static [&'static str],
    /// Columns planning knew before the data: the declared ones and the live table's.
    pub(crate) planned: &'static [&'static str],
    /// Columns that only ever held NULL.
    pub(crate) null_columns: &'static [&'static str],
    pub(crate) live: &'static [(&'static str, ColumnType)],
    /// The table's columns as `name:type`, in order.
    pub(crate) expected_columns: &'static str,
}

pub(crate) struct SourceSpellingTestCase {
    pub(crate) description: &'static str,
    pub(crate) stored: &'static str,
    pub(crate) warehouse: Warehouse,
    pub(crate) expected_name: &'static str,
}

pub(crate) struct DeclaredProblemsTestCase {
    pub(crate) description: &'static str,
    /// The Python table's settings, as the host's JSON.
    pub(crate) settings: &'static str,
    /// The data's column names.
    pub(crate) columns: &'static [&'static str],
    pub(crate) rows: u64,
    /// The problems joined by `; `, empty when there are none.
    pub(crate) expected_problems: &'static str,
}

pub(crate) struct BoundArrayTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    pub(crate) cursor_type: ColumnType,
    /// The bound as Arrow displays it.
    pub(crate) expected_value: &'static str,
}

pub(crate) struct RenderTallyTestCase {
    pub(crate) description: &'static str,
    /// `verb`, name and detail of each line.
    pub(crate) lines: &'static [(&'static str, &'static str, &'static str)],
    pub(crate) rows: u64,
    pub(crate) elapsed_ms: u64,
    pub(crate) expected_output: &'static str,
}
