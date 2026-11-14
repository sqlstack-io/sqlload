use rdkafka::Offset;

use crate::config::models::StartFrom;

pub(crate) struct ResumeOffsetTestCase {
    pub(crate) description: &'static str,
    /// What reading the group's committed offset gave: the offset, none for a group without one, or an error.
    pub(crate) found: Result<Option<Offset>, &'static str>,
    pub(crate) start_from: StartFrom,
    pub(crate) expected_offset: Result<Offset, &'static str>,
}
