use crate::adapters::files::_helpers::layout::loads::load_id_of;
use crate::adapters::files::constants::{
    ABORTED_KEPT_MS, PENDING_ABORTED, PENDING_FOLDER, PENDING_REPLACING, PENDING_RUN_PREFIX,
    PENDING_WRITING,
};
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::models::{Marker, PendingLoad, PendingPhase, PendingRun};
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::read_file::read_file;
use crate::file_store::models::{OpenFolder, StoredFile};
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
        PendingPhase::Aborted => PENDING_ABORTED,
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
        Some(PENDING_ABORTED) => PendingPhase::Aborted,
        other => {
            return Err(format!(
                "a pending marker starts with {other:?}, not {PENDING_WRITING:?}, {PENDING_REPLACING:?} or {PENDING_ABORTED:?}"
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

/// The loads the table's markers record as unfinished, with the files (of `files`, the table's) repairing each deletes; an `aborted` marker counts as one only while a file of its load is left, or once it expired.
pub(crate) async fn pending_loads(
    folder: &OpenFolder,
    files: &[StoredFile],
) -> Result<Vec<PendingLoad>, FilesDestinationError> {
    let markers = pending_folder(folder);
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut pending: Vec<PendingLoad> = Vec::new();
    for marker in list_files(&markers).await? {
        let text = read_file(&markers, &marker.relative_key).await?;
        let marker_read = parse_marker(&String::from_utf8_lossy(&text)).map_err(|reason| {
            FilesDestinationError::Write {
                key: format!("{}{}", markers.location, marker.relative_key),
                reason,
            }
        })?;
        let deletes = match marker_read.phase {
            PendingPhase::Writing | PendingPhase::Aborted => files_of(files, &marker.relative_key),
            PendingPhase::Replacing => marker_read.keys,
        };
        let expired = marker_read.phase == PendingPhase::Aborted
            && now_ms.saturating_sub(marker.modified_ms) > ABORTED_KEPT_MS;
        if marker_read.phase == PendingPhase::Aborted && deletes.is_empty() && !expired {
            continue;
        }
        pending.push(PendingLoad {
            load_id: marker.relative_key,
            phase: marker_read.phase,
            deletes,
            run: marker_read.run,
            expired,
        });
    }
    Ok(pending)
}

/// The keys of `load_id`'s files anywhere below the table folder.
fn files_of(files: &[StoredFile], load_id: &str) -> Vec<String> {
    files
        .iter()
        .filter(|file| load_id_of(&file.relative_key) == Some(load_id))
        .map(|file| file.relative_key.clone())
        .collect()
}
