//! Find files tables whose folders overlap.

use crate::store_paths::models::Location;

/// For each table (name and folder, `None` for a warehouse table), why its folder is unsafe: another table's folder is the same, inside it or around it, so replacing one would delete the other's files.
pub fn overlapping_folders(tables: &[Option<(&str, &Location)>]) -> Vec<Option<String>> {
    let mut problems: Vec<Option<String>> = Vec::with_capacity(tables.len());
    for (index, table) in tables.iter().enumerate() {
        let mut problem: Option<String> = None;
        for (other_index, other) in tables.iter().enumerate() {
            if let (Some((name, folder)), Some((other_name, other_folder))) = (table, other)
                && index != other_index
                && problem.is_none()
                && overlaps(folder, other_folder)
            {
                problem = Some(format!(
                    "{name} writes to {folder}, which overlaps {other_name}'s folder {other_folder}; give each table a folder of its own"
                ));
            }
        }
        problems.push(problem);
    }
    problems
}

fn overlaps(folder: &Location, other: &Location) -> bool {
    folder.store == other.store
        && (folder.prefix.starts_with(&other.prefix) || other.prefix.starts_with(&folder.prefix))
}
