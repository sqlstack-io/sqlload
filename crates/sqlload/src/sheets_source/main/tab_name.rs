//! Table names for tabs.

/// The table a tab loads as when the source names no ranges: its title in lower case, with each run of other characters than letters and digits as one `_`.
pub fn tab_name(title: &str) -> String {
    let mut name = String::new();
    for character in title.trim().chars().flat_map(char::to_lowercase) {
        match character.is_alphanumeric() {
            true => name.push(character),
            false if !name.is_empty() && !name.ends_with('_') => name.push('_'),
            false => {}
        }
    }
    name.trim_end_matches('_').to_owned()
}
