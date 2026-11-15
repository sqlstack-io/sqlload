//! Take the times the steps before the plan took.

use crate::load_events::_helpers::steps::STEPS;
use crate::load_events::models::PhaseTiming;

/// Every connection's and discovery's time since this was last asked, taken.
pub fn take_timings() -> Vec<PhaseTiming> {
    STEPS
        .lock()
        .map(|mut steps| std::mem::take(&mut steps.timings))
        .unwrap_or_default()
}
