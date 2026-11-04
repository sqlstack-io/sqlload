use crate::file_store::models::OpenFolder;
use crate::files_destination::constants::{PENDING_FOLDER, PENDING_REPLACING, PENDING_WRITING};
use crate::files_destination::models::PendingPhase;
use crate::store_paths::models::Location;

/// The folder of a table's pending markers, in the same store.
pub(crate) fn pending_folder(folder: &OpenFolder) -> OpenFolder {
    OpenFolder {
        store: folder.store.clone(),
        location: Location {
            store: folder.location.store.clone(),
            prefix: format!("{}{PENDING_FOLDER}", folder.location.prefix),
        },
    }
}

/// A marker's text: the phase, then one key per line.
pub(crate) fn marker_text(phase: PendingPhase, keys: &[String]) -> String {
    let first = match phase {
        PendingPhase::Writing => PENDING_WRITING,
        PendingPhase::Replacing => PENDING_REPLACING,
    };
    let mut lines = vec![first.to_owned()];
    lines.extend(keys.iter().cloned());
    lines.join("\n")
}

/// The phase and keys a marker's text records.
pub(crate) fn parse_marker(text: &str) -> Result<(PendingPhase, Vec<String>), String> {
    let mut lines = text.lines();
    let phase = match lines.next() {
        Some(PENDING_WRITING) => PendingPhase::Writing,
        Some(PENDING_REPLACING) => PendingPhase::Replacing,
        other => {
            return Err(format!(
                "a pending marker starts with {other:?}, not {PENDING_WRITING:?} or {PENDING_REPLACING:?}"
            ));
        }
    };
    let keys: Vec<String> = lines
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect();
    Ok((phase, keys))
}
