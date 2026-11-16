//! The `Skipped` section: one line per reason.

use crate::loader::models::PlanItem;
use crate::plan_text::_helpers::tree::{branch, marks};
use crate::plan_text::constants::{NO_NEW_FILES, NOTHING_IN_RANGE, SKIPPED_WIDTH, UP_TO_DATE};
use crate::plan_text::models::SkippedGroup;

/// The `Skipped` section: one line per reason, in order of the reasons, as `no new files   customers, prices` or `up to date     estimates (3 files)`, the tables named without their schema unless two would read the same, a longer list wrapping under its first name; then a blank line. Nothing without skipped tables.
pub(crate) fn skipped_section(skipped: &[PlanItem]) -> Vec<String> {
    if skipped.is_empty() {
        return Vec::new();
    }
    let groups = skipped_groups(skipped);
    let width = groups
        .iter()
        .map(|group| group.reason.chars().count())
        .max()
        .unwrap_or(0)
        + 3;
    let mut lines: Vec<String> = vec![format!("Skipped ({})", skipped.len())];
    for (index, group) in groups.iter().enumerate() {
        let last = index + 1 == groups.len();
        let rows = wrapped(&group.tables, SKIPPED_WIDTH.saturating_sub(width + 4));
        let first = format!("{:<width$}{}", group.reason, rows[0]);
        lines.extend(branch(&first, &[], last, ""));
        let (_, below) = marks(last);
        for row in &rows[1..] {
            lines.push(format!("{below}{:width$}{row}", ""));
        }
    }
    lines.push(String::new());
    lines
}

/// The skipped tables grouped by reason, in order of the reasons: no new files, up to date (with how many files each has), nothing in range, or the reason as given.
fn skipped_groups(skipped: &[PlanItem]) -> Vec<SkippedGroup> {
    let short = |name: &str| {
        name.split_once('.')
            .map_or(name, |(_, table)| table)
            .to_owned()
    };
    let mut groups: Vec<SkippedGroup> = Vec::new();
    for item in skipped {
        let reason = item
            .details
            .first()
            .map_or(String::new(), |detail| detail.text.clone());
        let twins = skipped
            .iter()
            .filter(|other| short(&other.name) == short(&item.name))
            .count();
        let name = match twins {
            1 => short(&item.name),
            _ => item.name.clone(),
        };
        let (key, table) = grouped(&reason, name);
        match groups.iter_mut().find(|group| group.reason == key) {
            Some(group) => group.tables.push(table),
            None => groups.push(SkippedGroup {
                reason: key,
                tables: vec![table],
            }),
        }
    }
    groups.sort_by(|left, right| left.reason.cmp(&right.reason));
    groups
}

/// The group a skipped table's `reason` puts it in, and how the group names it: `up to date` names how many files it has.
fn grouped(reason: &str, name: String) -> (String, String) {
    let (up_to_date, up_to_date_key) = UP_TO_DATE;
    if let Some(rest) = reason.strip_prefix(up_to_date) {
        let count = rest.split(' ').next().unwrap_or_default().replace(',', "");
        let named = match count.parse::<u64>() {
            Ok(1) => format!("{name} (1 file)"),
            Ok(count) => format!("{name} ({count} files)"),
            Err(_) => name,
        };
        return (up_to_date_key.to_owned(), named);
    }
    for (start, key) in [NO_NEW_FILES, NOTHING_IN_RANGE] {
        if reason.starts_with(start) {
            return (key.to_owned(), name);
        }
    }
    (reason.to_owned(), name)
}

/// `names` joined with commas into rows of at most `width` characters, a name longer than that alone on its row.
fn wrapped(names: &[String], width: usize) -> Vec<String> {
    let mut rows: Vec<String> = vec![String::new()];
    for (index, name) in names.iter().enumerate() {
        let comma = match index + 1 == names.len() {
            true => "",
            false => ",",
        };
        let Some(row) = rows.last_mut() else {
            continue;
        };
        if row.is_empty() {
            row.push_str(&format!("{name}{comma}"));
        } else if row.chars().count() + name.chars().count() + 2 > width {
            rows.push(format!("{name}{comma}"));
        } else {
            row.push_str(&format!(" {name}{comma}"));
        }
    }
    rows
}
