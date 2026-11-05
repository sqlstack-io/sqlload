pub(crate) struct WritePartTestCase {
    pub(crate) description: &'static str,
    /// Each batch's column name and row count, written in order before the writer finishes.
    pub(crate) batches: &'static [(&'static str, usize)],
    /// Each file's row count, in the order written.
    pub(crate) expected_files: &'static [usize],
}
