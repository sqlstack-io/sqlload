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
