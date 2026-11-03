pub(crate) struct BatcherTestCase {
    pub(crate) description: &'static str,
    pub(crate) rows: usize,
    pub(crate) row_bytes: usize,
    pub(crate) elapsed_secs: u64,
    pub(crate) other_streams_bytes: u64,
    pub(crate) expected_due: bool,
}

pub(crate) struct CompressChunksTestCase {
    pub(crate) description: &'static str,
    pub(crate) rows: usize,
    pub(crate) random_bytes: usize,
    pub(crate) limit: u64,
    pub(crate) expected_min_chunks: usize,
}

pub(crate) struct OversizedRowTestCase {
    pub(crate) description: &'static str,
    pub(crate) random_bytes: usize,
    pub(crate) limit: u64,
    pub(crate) expected_offset: i64,
}
