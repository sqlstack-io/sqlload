use crate::adapters::files::constants::{
    PENDING_FOLDER, PENDING_REPLACING, PENDING_RUN_PREFIX, PENDING_WRITING,
};
use crate::adapters::files::models::{Marker, PendingPhase, PendingRun};
use crate::file_store::models::OpenFolder;
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

/// A marker's text: the phase, then one key per line, then the run record waiting to be written, if any.
pub(crate) fn marker_text(
    phase: PendingPhase,
    keys: &[String],
    run: Option<&PendingRun>,
) -> String {
    let first = match phase {
        PendingPhase::Writing => PENDING_WRITING,
        PendingPhase::Replacing => PENDING_REPLACING,
    };
    let mut lines = vec![first.to_owned()];
    lines.extend(keys.iter().cloned());
    lines.extend(run.map(|run| {
        format!(
            "{PENDING_RUN_PREFIX}{}\t{}\t{}",
            run.prefix, run.name, run.json
        )
    }));
    lines.join("\n")
}

/// The phase, keys and waiting run record a marker's text records.
pub(crate) fn parse_marker(text: &str) -> Result<Marker, String> {
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
    let mut keys: Vec<String> = Vec::new();
    let mut run: Option<PendingRun> = None;
    for line in lines.filter(|line| !line.is_empty()) {
        match line
            .strip_prefix(PENDING_RUN_PREFIX)
            .map(|rest| rest.splitn(3, '\t').collect::<Vec<&str>>())
        {
            Some(parts) => match parts.as_slice() {
                [prefix, name, json] => {
                    run = Some(PendingRun {
                        prefix: (*prefix).to_owned(),
                        name: (*name).to_owned(),
                        json: (*json).to_owned(),
                    });
                }
                _ => {
                    return Err(format!(
                        "a pending marker's run line is malformed: {line:?}"
                    ));
                }
            },
            None => keys.push(line.to_owned()),
        }
    }
    Ok(Marker { phase, keys, run })
}
