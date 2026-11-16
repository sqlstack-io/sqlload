//! Take the times the steps before the plan took.

use crate::load_events::_helpers::steps::STEPS;
use crate::load_events::models::PhaseTiming;

/// Every connection's and discovery's time, taken once the plan is done: steps are no longer tracked after, so a plan made again later, as `load --follow` makes, adds no phase lines, times or events.
pub fn take_timings() -> Vec<PhaseTiming> {
    let Ok(mut steps) = STEPS.lock() else {
        return Vec::new();
    };
    steps.tracking = false;
    steps.current = None;
    std::mem::take(&mut steps.timings)
}
