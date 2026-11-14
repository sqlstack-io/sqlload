//! Stop this process's load event stream.

use crate::load_events::_helpers::stream::STREAM;

/// Stop the process's load event stream: events sent after this go nowhere, and its receiver ends once it has the ones sent before.
pub fn close() {
    if let Ok(mut stream) = STREAM.lock() {
        *stream = None;
    }
}
