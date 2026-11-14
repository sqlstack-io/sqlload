//! Start this process's load event stream.

use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

use crate::load_events::_helpers::stream::{EventStream, STREAM};
use crate::load_events::models::LoadEvent;

/// Start the process's load event stream for run `run_id`, replacing any before it: every event [`emit`](super::emit::emit) sends from now on arrives on the receiver, in the order sent. Only the command line listens, for its one load; without a listener, emitting does nothing.
pub fn listen(run_id: &str) -> UnboundedReceiver<LoadEvent> {
    let (sender, receiver) = unbounded_channel();
    if let Ok(mut stream) = STREAM.lock() {
        *stream = Some(EventStream {
            run_id: run_id.to_owned(),
            sender,
        });
    }
    receiver
}
