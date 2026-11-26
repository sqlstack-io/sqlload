//! A `unique_key = "{primary_key}"` its settings alone refuse.

use crate::loader::constants::PRIMARY_KEY_PLACEHOLDER;
use crate::project::models::SourceKind;

/// Why table `qualified` of a `kind` source cannot take its `unique_key` (`set`) as written: `"{primary_key}"` listed with other columns, or a merge on it from a source that reports no keys; `merges` when it merges on a `unique_key`.
pub fn placeholder_problem(
    kind: SourceKind,
    qualified: &str,
    set: Option<&[String]>,
    merges: bool,
) -> Option<String> {
    let key = set?;
    if !key.iter().any(|column| column == PRIMARY_KEY_PLACEHOLDER) {
        return None;
    }
    if key.len() > 1 {
        return Some(format!(
            "{qualified}: unique_key {PRIMARY_KEY_PLACEHOLDER:?} stands for the whole primary key, so it can't be listed with other columns; set unique_key = {PRIMARY_KEY_PLACEHOLDER:?} or name every key column"
        ));
    }
    let reports_none = matches!(
        kind,
        SourceKind::Python | SourceKind::Files | SourceKind::GoogleSheets
    );
    (merges && reports_none).then(|| {
        format!(
            "{qualified} (kind = \"{}\"): unique_key = {PRIMARY_KEY_PLACEHOLDER:?} merges on the primary key the source reports, and a {} source reports none. Fix: name the key columns, such as unique_key = [\"id\"]",
            kind.name(),
            kind.name()
        )
    })
}
