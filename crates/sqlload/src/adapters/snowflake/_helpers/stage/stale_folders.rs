use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use crate::adapters::snowflake::constants::STAGE_FOLDER;

/// The load folders (`sqlload/<load id>/`) among `listed` (each file's name and `last_modified`, as `LIST @~/sqlload/` shows them) whose newest file is older than `before`; a file whose time cannot be read keeps its folder.
pub(crate) fn stale_folders(listed: &[(String, String)], before: DateTime<Utc>) -> Vec<String> {
    let mut newest: BTreeMap<String, Option<DateTime<Utc>>> = BTreeMap::new();
    for (name, modified) in listed {
        let mut parts = name.split('/');
        let (Some(STAGE_FOLDER), Some(load_id), Some(_)) =
            (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        let time = match DateTime::parse_from_rfc2822(modified) {
            Ok(time) => Some(time.with_timezone(&Utc)),
            Err(_) => None,
        };
        let entry = newest
            .entry(format!("{STAGE_FOLDER}/{load_id}/"))
            .or_insert(time);
        *entry = match (*entry, time) {
            (Some(seen), Some(time)) => Some(seen.max(time)),
            _ => None,
        };
    }
    newest
        .into_iter()
        .filter(|(_, time)| time.is_some_and(|time| time < before))
        .map(|(folder, _)| folder)
        .collect()
}
