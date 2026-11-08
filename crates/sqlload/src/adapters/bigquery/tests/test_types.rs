pub(crate) struct RenderedTestCase {
    pub(crate) description: &'static str,
    pub(crate) rendered: Vec<String>,
    pub(crate) expected_statements: &'static [&'static str],
}

pub(crate) struct WriteApiBatchTestCase {
    pub(crate) description: &'static str,
    /// The rows go to a build table, which leaves out fields outside `columns`.
    pub(crate) builds: bool,
    pub(crate) columns: Vec<crate::schema_plan::models::DesiredColumn>,
    pub(crate) batch: arrow_array::RecordBatch,
    /// Each column as `name: type = value|value`, or the error up to its first `: `.
    pub(crate) expected_batch: Result<&'static str, &'static str>,
}

pub(crate) struct AppendRequestsTestCase {
    pub(crate) description: &'static str,
    /// Ids 0 to `rows - 1`, each with a note of `note_bytes` characters.
    pub(crate) rows: i64,
    pub(crate) note_bytes: usize,
    /// The first rows of the batch that are cut, as a slice sharing its buffers.
    pub(crate) sliced_rows: usize,
    pub(crate) max_bytes: usize,
    /// Every request at most `max_bytes` once encoded.
    pub(crate) expected_within_limit: bool,
    /// More than one request.
    pub(crate) expected_split: bool,
    /// The ids the requests decode back to, in order, as `first..=last` and their count.
    pub(crate) expected_ids: String,
}
