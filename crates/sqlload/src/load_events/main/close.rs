//! Stop this process's load event stream.

use crate::load_events::_helpers::steps::STEPS;
use crate::load_events::_helpers::stream::STREAM;

/// Stop the process's load event stream: events sent after this go nowhere, and its receiver ends once it has the ones sent before; steps before a plan are no longer tracked.
pub fn close() {
    if let Ok(mut stream) = STREAM.lock() {
        *stream = None;
    }
    if let Ok(mut steps) = STEPS.lock() {
        steps.tracking = false;
        steps.current = None;
    }
}
