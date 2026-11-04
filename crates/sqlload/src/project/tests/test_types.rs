use crate::project::models::ProjectConfig;

pub(crate) struct ParseProjectTestCase {
    pub(crate) description: &'static str,
    /// Reads the value under test from the parsed project, as debug text.
    pub(crate) read: fn(&ProjectConfig) -> String,
    pub(crate) expected_value: &'static str,
}

pub(crate) struct ParseProjectErrorTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    /// Text the error message must contain.
    pub(crate) expected_message: &'static str,
}

pub(crate) struct IsProjectTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    pub(crate) expected_is_project: bool,
}
