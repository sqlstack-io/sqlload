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

pub(crate) struct LayoutProblemsTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: crate::column_types::models::Warehouse,
    pub(crate) layout: crate::loader::models::TableLayout,
    /// Text the single problem holds; `None` when the layout is taken.
    pub(crate) expected_problem: Option<&'static str>,
}

pub(crate) struct PublishStepTestCase {
    pub(crate) description: &'static str,
    pub(crate) statement: &'static str,
    /// The step it names, `None` for plain SQL, or an error.
    pub(crate) expected_step: Result<Option<crate::adapters::clickhouse::models::PublishStep>, ()>,
}

pub(crate) struct PullInsertTestCase {
    pub(crate) description: &'static str,
    pub(crate) columns: Vec<crate::schema_plan::models::DesiredColumn>,
    /// Whether the read is signed (with a session token), or unsigned for a public bucket.
    pub(crate) signed: bool,
    /// The query parameters sent beside the statement.
    pub(crate) expected_parameters: &'static [(&'static str, &'static str)],
    pub(crate) expected_insert: &'static str,
}

pub(crate) struct ObjectUrlTestCase {
    pub(crate) description: &'static str,
    pub(crate) bucket: &'static str,
    pub(crate) region: Option<&'static str>,
    pub(crate) endpoint: Option<&'static str>,
    /// Whether requests to the endpoint are virtual-hosted, the bucket already in it.
    pub(crate) in_endpoint: bool,
    pub(crate) key: &'static str,
    pub(crate) expected_url: &'static str,
}
