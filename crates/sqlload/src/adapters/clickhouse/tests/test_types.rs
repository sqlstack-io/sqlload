pub(crate) struct LayoutTestCase {
    pub(crate) description: &'static str,
    pub(crate) layout: crate::loader::models::TableLayout,
    pub(crate) unique_key: &'static [&'static str],
    pub(crate) expected_clauses: &'static str,
}

pub(crate) struct PartitionStatementsTestCase {
    pub(crate) description: &'static str,
    /// The condition on the table's rows the delta replaces; `None` for an append.
    pub(crate) displaced: Option<&'static str>,
    /// The partitions of the delta's rows.
    pub(crate) new: &'static [&'static str],
    /// The table's partitions holding rows the delta replaces.
    pub(crate) replaced: &'static [&'static str],
    pub(crate) expected_statements: &'static [&'static str],
}

pub(crate) struct ArrowBatchTestCase {
    pub(crate) description: &'static str,
    /// The rows go to a build table, which leaves out fields outside `columns`.
    pub(crate) builds: bool,
    pub(crate) columns: Vec<crate::schema_plan::models::DesiredColumn>,
    pub(crate) batch: arrow_array::RecordBatch,
    /// Each column as `name: type = value|value`, or the error up to its first `: `.
    pub(crate) expected_batch: Result<&'static str, &'static str>,
}
