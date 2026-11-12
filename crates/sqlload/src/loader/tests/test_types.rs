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
    /// The lines `apply` printed.
    pub(crate) applied: &'static [&'static str],
    /// Each table, its rows or failure, and its milliseconds.
    pub(crate) outcomes: &'static [(&'static str, Result<u64, &'static str>, u64)],
    pub(crate) elapsed_ms: u64,
    pub(crate) expected_output: &'static str,
}

/// A plan item: its name, its action, and each detail's label and text.
pub(crate) type ItemSpec = (
    &'static str,
    &'static str,
    &'static [(&'static str, &'static str)],
);

/// A plan view built from items, and the exact text `plan` prints for it.
pub(crate) struct ViewLinesTestCase {
    pub(crate) description: &'static str,
    pub(crate) ok: bool,
    pub(crate) summary: &'static str,
    pub(crate) selected: usize,
    pub(crate) objects: &'static [ItemSpec],
    pub(crate) loads: &'static [ItemSpec],
    pub(crate) errors: &'static [ItemSpec],
    pub(crate) warnings: &'static [ItemSpec],
    pub(crate) expected_output: &'static str,
}

/// Errors items before merging, and each merged item's name.
pub(crate) struct MergedErrorsTestCase {
    pub(crate) description: &'static str,
    pub(crate) errors: &'static [ItemSpec],
    pub(crate) expected_names: &'static [&'static str],
}

pub(crate) struct SourceFilterTestCase {
    pub(crate) description: &'static str,
    pub(crate) cursor_type: ColumnType,
    pub(crate) dialect: Option<Warehouse>,
    /// Whether text bounds are bound as parameters, as an ODBC source's are.
    pub(crate) binds: bool,
    pub(crate) lower: Option<(&'static str, &'static str)>,
    pub(crate) upper: Option<&'static str>,
    pub(crate) expected_filter: Option<&'static str>,
    /// The values bound to the filter's placeholders, in order.
    pub(crate) expected_parameters: &'static [&'static str],
}

pub(crate) struct RetryableErrorTestCase {
    pub(crate) description: &'static str,
    pub(crate) error: fn() -> crate::loader::errors::LoaderError,
    pub(crate) expected_retryable: bool,
}

pub(crate) struct MergedFileTestCase {
    pub(crate) description: &'static str,
    /// The file's name, then its version, as a merge records them.
    pub(crate) file: (&'static str, &'static str),
    /// The name and version the ledger reads back.
    pub(crate) expected_file: (&'static str, &'static str),
}

pub(crate) struct ConstantsAddedTestCase {
    pub(crate) description: &'static str,
    /// The constants added: name, type and value as text.
    pub(crate) constants: &'static [(&'static str, ColumnType, &'static str)],
    /// Whether the batch comes back equal, and its columns as `name:Arrow type=values`; or the error.
    pub(crate) expected_outcome: Result<(bool, &'static str), &'static str>,
}
