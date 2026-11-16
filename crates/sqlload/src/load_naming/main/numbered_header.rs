//! Unique names for a header row that repeats a name.

use std::collections::BTreeSet;

/// Each of `names`, a header row in order, as a reader reads it, with the header it repeats: a name the row has once stays as it is; a repeat (matched ignoring case) gets `_1`, `_2`… after it, past every name the row has, so each read name is unique.
pub fn numbered_header(names: &[String]) -> Vec<(String, Option<String>)> {
    let mut taken: BTreeSet<String> = names.iter().map(|name| name.to_lowercase()).collect();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut read: Vec<(String, Option<String>)> = Vec::new();
    for name in names {
        if seen.insert(name.to_lowercase()) {
            read.push((name.clone(), None));
            continue;
        }
        let numbered = next_free(name, &taken);
        taken.insert(numbered.to_lowercase());
        read.push((numbered, Some(name.clone())));
    }
    read
}

/// `name` with the first of `_1`, `_2`… after it that `taken` doesn't hold, ignoring case.
fn next_free(name: &str, taken: &BTreeSet<String>) -> String {
    (1..)
        .map(|number| format!("{name}_{number}"))
        .find(|candidate| !taken.contains(&candidate.to_lowercase()))
        .unwrap_or_default()
}
