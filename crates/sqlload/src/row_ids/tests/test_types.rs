use arrow_array::RecordBatch;

pub(crate) struct KeyRowIdsTestCase {
    pub(crate) description: &'static str,
    pub(crate) batch: fn() -> RecordBatch,
    /// Each key column as its name in the id and its index in the batch.
    pub(crate) key: &'static [(&'static str, usize)],
    pub(crate) expected_ids: &'static [&'static str],
}

pub(crate) struct RandomRowIdsTestCase {
    pub(crate) description: &'static str,
    pub(crate) rows: usize,
    pub(crate) expected_distinct: usize,
    pub(crate) expected_lengths: &'static [usize],
}
