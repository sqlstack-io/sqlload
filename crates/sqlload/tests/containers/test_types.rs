pub(crate) struct DeliveryTestCase {
    pub(crate) description: &'static str,
    pub(crate) topic: &'static str,
    pub(crate) records: usize,
    pub(crate) oversized_records: usize,
    pub(crate) extra_config: &'static str,
    pub(crate) expected_table: &'static str,
    pub(crate) expected_rows: usize,
    pub(crate) expected_max_connections: u64,
}

pub(crate) struct RecoveryTestCase {
    pub(crate) description: &'static str,
    pub(crate) topic: &'static str,
    pub(crate) first_records: usize,
    pub(crate) second_records: usize,
    pub(crate) injected_failures: usize,
    pub(crate) injected_status: u16,
    pub(crate) expected_table: &'static str,
    pub(crate) expected_rows: usize,
}

pub(crate) struct DiscoveryTestCase {
    pub(crate) description: &'static str,
    pub(crate) first_topic: &'static str,
    pub(crate) later_topic: &'static str,
    pub(crate) records: usize,
    pub(crate) expected_table: &'static str,
}

pub(crate) struct ParallelTestCase {
    pub(crate) description: &'static str,
    pub(crate) topic: &'static str,
    pub(crate) records: usize,
    pub(crate) instances: usize,
    pub(crate) expected_table: &'static str,
}
