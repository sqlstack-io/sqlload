use std::time::Duration;

use rdkafka::Offset;

use crate::config::models::StartFrom;
use crate::delivery::errors::DeliveryError;
use crate::exporter::_helpers::stream_task::AfterFailure;

pub(crate) struct ResumeOffsetTestCase {
    pub(crate) description: &'static str,
    /// What reading the group's committed offset gave: the offset, none for a group without one, or an error.
    pub(crate) found: Result<Option<Offset>, &'static str>,
    pub(crate) start_from: StartFrom,
    pub(crate) expected_offset: Result<Offset, &'static str>,
}

pub(crate) struct AfterFailureTestCase {
    pub(crate) description: &'static str,
    pub(crate) error: DeliveryError,
    /// Time since the batch's first delivery attempt.
    pub(crate) elapsed: Duration,
    pub(crate) attempt: u32,
    pub(crate) expected_step: AfterFailure,
}
