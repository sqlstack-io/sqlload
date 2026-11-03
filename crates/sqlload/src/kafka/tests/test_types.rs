pub(crate) struct PrefetchTestCase {
    pub(crate) description: &'static str,
    pub(crate) budget_bytes: u64,
    pub(crate) consumers: usize,
    pub(crate) expected_kbytes: u64,
}
