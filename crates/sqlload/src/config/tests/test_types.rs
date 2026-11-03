use std::time::Duration;

pub(crate) struct ParseConfigTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    pub(crate) expected_columns: usize,
}

pub(crate) struct ParseConfigErrorTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    pub(crate) expected_error: &'static str,
}

pub(crate) struct StreamSettingsTestCase {
    pub(crate) description: &'static str,
    pub(crate) topic: &'static str,
    pub(crate) expected_max_rows: usize,
    pub(crate) expected_max_age: Duration,
}

pub(crate) struct DestinationNameTestCase {
    pub(crate) description: &'static str,
    pub(crate) schema: &'static str,
    /// The stored schema name, or text the error must contain.
    pub(crate) expected_outcome: &'static str,
}

pub(crate) struct StartupSettingsTestCase {
    pub(crate) description: &'static str,
    /// Lines added to `[source]`.
    pub(crate) source_lines: &'static str,
    pub(crate) expected_batch: usize,
    pub(crate) expected_interval: Duration,
    pub(crate) expected_budget: u64,
}
