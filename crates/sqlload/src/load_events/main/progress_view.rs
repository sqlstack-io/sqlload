//! What the plan's progress has to show now.

use crate::load_events::_helpers::steps::STEPS;
use crate::load_events::models::{ProgressView, StepView};

/// The phase lines finished since this was last asked, taken in the order the steps ended: only those of slow or failed steps, or every one when `verbose`; and the step running now.
pub fn progress_view(verbose: bool) -> ProgressView {
    let Ok(mut steps) = STEPS.lock() else {
        return ProgressView::default();
    };
    ProgressView {
        lines: std::mem::take(&mut steps.lines)
            .into_iter()
            .filter(|(_, quiet)| verbose || !quiet)
            .map(|(line, _)| line)
            .collect(),
        step: steps.current.as_ref().map(|step| StepView {
            id: step.id,
            subject: step.subject.clone(),
            doing: step.doing.clone(),
            elapsed: step.started.elapsed(),
        }),
    }
}
