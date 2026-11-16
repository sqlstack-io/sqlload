//! Say what the running step does now.

use crate::load_events::_helpers::steps::STEPS;

/// Say what the running step does now, such as `describing PUB.absence (2/5)`, keeping its clock.
pub fn step_doing(doing: &str) {
    let Ok(mut steps) = STEPS.lock() else {
        return;
    };
    if let Some(step) = steps.current.as_mut() {
        doing.clone_into(&mut step.doing);
    }
}
