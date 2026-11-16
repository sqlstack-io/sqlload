//! Start a step before the plan.

use std::time::Instant;

use crate::load_events::_helpers::steps::{STEPS, Step};

/// Start step `doing` (such as `connecting` or `listing tables`) of `subject`, a source or target, ending any step running; its clock starts now. Nothing unless the command line listens for load events.
pub fn begin_step(subject: &str, doing: &str) {
    let Ok(mut steps) = STEPS.lock() else {
        return;
    };
    if !steps.tracking {
        return;
    }
    steps.next_id += 1;
    steps.current = Some(Step {
        id: steps.next_id,
        subject: subject.to_owned(),
        doing: doing.to_owned(),
        started: Instant::now(),
    });
}
