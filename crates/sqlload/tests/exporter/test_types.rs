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
    pub(crate) expected_rows: usize,
}

pub(crate) struct ParallelTestCase {
    pub(crate) description: &'static str,
    pub(crate) topic: &'static str,
    pub(crate) records: usize,
    pub(crate) instances: usize,
    pub(crate) expected_table: &'static str,
    pub(crate) expected_rows: usize,
}

pub(crate) struct S3LayoutTestCase {
    pub(crate) description: &'static str,
    pub(crate) staging: &'static str,
    pub(crate) s3: &'static str,
    pub(crate) records_per_topic: usize,
    /// Each key starts with one of these, then `{date}`.
    pub(crate) expected_folders: &'static [&'static str],
    /// Text between the date and the file name.
    pub(crate) expected_after_date: &'static str,
    pub(crate) expected_rows: usize,
}

pub(crate) struct S3RetryTestCase {
    pub(crate) description: &'static str,
    pub(crate) failures: usize,
    pub(crate) status: u16,
    pub(crate) records: usize,
    pub(crate) expected_rows: usize,
}
