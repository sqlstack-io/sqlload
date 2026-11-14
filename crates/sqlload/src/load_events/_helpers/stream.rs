//! The process's one load event stream.

use std::sync::Mutex;

use tokio::sync::mpsc::UnboundedSender;

use crate::load_events::models::LoadEvent;

/// Where load events go, and the run they belong to.
#[derive(Debug)]
pub(crate) struct EventStream {
    pub(crate) run_id: String,
    pub(crate) sender: UnboundedSender<LoadEvent>,
}

/// The stream the command line listens on, if it does.
pub(crate) static STREAM: Mutex<Option<EventStream>> = Mutex::new(None);
