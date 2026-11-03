use sqlload_testkit::fake_snowflake::models::LoadOutcome;

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
    /// The staged files' extension.
    pub(crate) expected_extension: &'static str,
    pub(crate) expected_rows: usize,
}

pub(crate) struct S3RetryTestCase {
    pub(crate) description: &'static str,
    pub(crate) failures: usize,
    pub(crate) status: u16,
    pub(crate) records: usize,
    pub(crate) expected_rows: usize,
}

pub(crate) struct S3LoadReportTestCase {
    pub(crate) description: &'static str,
    pub(crate) records: usize,
    /// How long reports are withheld after every row is loaded.
    pub(crate) withheld_for: std::time::Duration,
    pub(crate) expected_committed: i64,
}

pub(crate) struct S3LoadFailureTestCase {
    pub(crate) description: &'static str,
    /// Lines added to `[staging]`.
    pub(crate) staging: &'static str,
    pub(crate) script: &'static [LoadOutcome],
    pub(crate) expected_error: &'static str,
    pub(crate) expected_committed: std::ops::RangeInclusive<i64>,
}

pub(crate) struct S3SlowLoadTestCase {
    pub(crate) description: &'static str,
    pub(crate) staging: &'static str,
    pub(crate) script: &'static [LoadOutcome],
    pub(crate) withhold_reports: bool,
    /// How long the file is held before it is released.
    pub(crate) held_for: std::time::Duration,
    /// How often the first file was sent to `insertFiles` in all.
    pub(crate) expected_sends: usize,
}

pub(crate) struct S3ScanFailureTestCase {
    pub(crate) description: &'static str,
    pub(crate) status: u16,
    pub(crate) records: usize,
    /// Records committed once the first batch is: some, not all.
    pub(crate) expected_committed: std::ops::RangeInclusive<i64>,
}

pub(crate) struct S3SkipTestCase {
    pub(crate) description: &'static str,
    pub(crate) records_per_topic: usize,
    pub(crate) expected_metric: &'static str,
}

pub(crate) struct S3ShutdownTestCase {
    pub(crate) description: &'static str,
    pub(crate) records: usize,
    pub(crate) expected_committed: std::ops::RangeInclusive<i64>,
}

pub(crate) struct S3PauseTestCase {
    pub(crate) description: &'static str,
    pub(crate) records: usize,
    pub(crate) held_for: std::time::Duration,
    pub(crate) expected_uploads_while_held: usize,
}

pub(crate) struct ShardingTestCase {
    pub(crate) description: &'static str,
    pub(crate) topics: &'static [&'static str],
    pub(crate) records: usize,
    pub(crate) shards: u32,
    pub(crate) expected_streams: usize,
}
