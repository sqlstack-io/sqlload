//! Send a load event.

use crate::load_events::_helpers::stream::STREAM;
use crate::load_events::constants::EVENTS_SCHEMA_VERSION;
use crate::load_events::models::{EventKind, LoadEvent};

/// Send `kind`, about `table` when it concerns one, to the process's load event stream; nothing when no one listens (see [`listen`](super::listen::listen)).
pub fn emit(table: Option<&str>, kind: EventKind) {
    let Ok(stream) = STREAM.lock() else {
        return;
    };
    if let Some(stream) = stream.as_ref() {
        let event = LoadEvent {
            schema_version: EVENTS_SCHEMA_VERSION,
            run_id: stream.run_id.clone(),
            at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            table: table.map(str::to_owned),
            kind,
        };
        let _ = stream.sender.send(event);
    }
}
