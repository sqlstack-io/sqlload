//! What `plan` prints that `plan --json` doesn't: tables named as they land, the details most of a project's tables share left out, an incremental table's `reads` and `commits`, and errors with a headline, wrapped, a source that could not be read naming what it holds up.

use std::collections::HashMap;

use crate::adapters::contract::models::WriteMode;
use crate::loader::_helpers::render::batch_line;
use crate::loader::_helpers::resolve::display_name;
use crate::loader::constants::{LABEL_WIDTH, PLAN_ERROR_WIDTH, PRIMARY_KEY_PLACEHOLDER};
use crate::loader::models::{
    CommitRule, PlanDetail, PlanItem, ProjectPlan, TablePlan, UnreadSource, WholeReason,
};
use crate::project::models::BatchStep;

/// Details the project view leaves out where a table has the text most of its tables have.
const SHARED_LABELS: [&str; 4] = ["route", "why", "batch", "schema"];

/// For each of `SHARED_LABELS`, the text most of `items` have, when at least two share it; ties go to the text first in order.
pub(crate) fn usual_details(items: &[PlanItem]) -> HashMap<String, String> {
    let mut counts: Vec<(&str, &str, usize)> = Vec::new();
    for detail in items.iter().flat_map(|item| &item.details) {
        if !SHARED_LABELS.contains(&detail.label.as_str()) {
            continue;
        }
        match counts
            .iter_mut()
            .find(|(label, text, _)| *label == detail.label && *text == detail.text)
        {
            Some((_, _, count)) => *count += 1,
            None => counts.push((&detail.label, &detail.text, 1)),
        }
    }
    let mut usual: HashMap<String, (String, usize)> = HashMap::new();
    for (label, text, count) in counts {
        let best = usual.get(label).map_or(1, |(_, best)| *best);
        if count > best {
            usual.insert(label.to_owned(), (text.to_owned(), count));
        }
    }
    usual
        .into_iter()
        .map(|(label, (text, _))| (label, text))
        .collect()
}

/// `item`, `load_item`'s view of `table`, as `plan` prints it: named as it lands, and, for an incremental table, `reads` and `commits` in place of `cursor`, `state` and `batch`. With `one` (one table selected) every detail stays, with times to the millisecond; otherwise a detail with its `usual` text is left out, and `commits` only shows when it says more than the usual.
pub(crate) fn text_item(
    table: &TablePlan,
    item: PlanItem,
    one: bool,
    usual: &HashMap<String, String>,
) -> PlanItem {
    let incremental = table.window.is_some() || table.logged.is_some();
    let mut details: Vec<PlanDetail> = item
        .details
        .into_iter()
        .filter(|detail| match detail.label.as_str() {
            "cursor" | "state" | "batch" if incremental => false,
            label => one || usual.get(label) != Some(&detail.text),
        })
        .collect();
    let after_source = details
        .iter()
        .position(|detail| detail.label == "source")
        .map_or(0, |index| index + 1);
    let mut added: Vec<PlanDetail> = Vec::new();
    if one {
        added.extend(key_line(table).map(|text| detail("key", text)));
    }
    if incremental {
        added.extend(reads_line(table, one).map(|text| detail("reads", text)));
    }
    details.splice(after_source..after_source, added);
    if let Some(text) = incremental.then(|| commits_line(table, one)).flatten() {
        let at = details
            .iter()
            .position(|detail| detail.label == "route")
            .unwrap_or(details.len());
        details.insert(at, detail("commits", text));
    }
    PlanItem {
        name: display_name(table.spec.warehouse, &table.spec.destination),
        action: item.action,
        details,
    }
}

fn detail(label: &str, text: String) -> PlanDetail {
    PlanDetail {
        label: label.to_owned(),
        text,
    }
}

/// Where a merge's key comes from: `unique_key`, or the primary key the source reports.
fn key_line(table: &TablePlan) -> Option<String> {
    let WriteMode::Merge { unique_key, .. } = &table.mode else {
        return None;
    };
    Some(match table.spec.key_from_primary_key {
        true => format!(
            "{}, the primary key the source reports (unique_key = \"{PRIMARY_KEY_PLACEHOLDER}\")",
            unique_key.join(", ")
        ),
        false => "from unique_key".to_owned(),
    })
}

/// Where the read starts: `updated_at >= 2026-11-14 07:49:41 (warehouse maximum minus lookback 5s)`, or the logged state; times to the second, or to the millisecond when `precise`.
fn reads_line(table: &TablePlan, precise: bool) -> Option<String> {
    if let Some(logged) = &table.logged {
        return Some(match &logged.current {
            Some(record) => format!(
                "logged state {} (run {}, {})",
                record.state.as_deref().unwrap_or("not set"),
                record.run_id.chars().take(8).collect::<String>(),
                clock_time(&record.recorded_at)
            ),
            None => "everything: no run is logged yet".to_owned(),
        });
    }
    let window = table.window.as_ref()?;
    let note = table.cursor_note.as_deref().unwrap_or_default();
    let origin = note
        .rsplit_once(" (")
        .map(|(_, origin)| origin.trim_end_matches(')'))
        .unwrap_or_default()
        .replace("destination maximum", "warehouse maximum");
    let origin = match origin.is_empty() {
        true => String::new(),
        false => format!(" ({origin})"),
    };
    let Some(from) = &window.from else {
        return Some(format!(
            "{} everything{}",
            window.column,
            match window.to.as_deref() {
                Some(to) => format!(", up to {}", trimmed_time(to, precise)),
                None => String::new(),
            }
        ));
    };
    let bound = match window.inclusive {
        true => ">=",
        false => ">",
    };
    Some(format!(
        "{} {bound} {}{origin}",
        window.column,
        trimmed_time(from, precise)
    ))
}

/// How the read commits: once at its end, or after each window; with `precise`, any other rule too, as `batch` says it.
fn commits_line(table: &TablePlan, precise: bool) -> Option<String> {
    match &table.commits {
        CommitRule::Whole(WholeReason::Unordered | WholeReason::Blocked) if !precise => {
            Some(format!(
                "once, at the end of the read ({} isn't read in order)",
                table.window.as_ref()?.column
            ))
        }
        CommitRule::Whole(_) => Some(batch_line(table).replacen(
            "one commit for the whole read",
            "once, at the end of the read",
            1,
        )),
        CommitRule::Windows(_) => {
            let step = match table.spec.batch {
                Some(BatchStep::Duration(step)) => window_length(step),
                Some(BatchStep::Integer(step)) => format!("{step}-wide"),
                Some(BatchStep::Whole) | None => return Some(batch_line(table)),
            };
            Some(match precise {
                true => format!("after each {step} window: {}", batch_line(table)),
                false => format!("after each {step} window"),
            })
        }
        CommitRule::Ordered { .. } | CommitRule::Size | CommitRule::FileGroups(_) => {
            precise.then(|| batch_line(table))
        }
    }
}

/// A window's length as people say it: `30-day`, `6-hour`, or as written when it isn't whole days, hours or minutes.
fn window_length(step: std::time::Duration) -> String {
    let seconds = step.as_secs();
    let units = [(86_400, "day"), (3_600, "hour"), (60, "minute")];
    units
        .iter()
        .find(|(size, _)| seconds.is_multiple_of(*size) && seconds >= *size)
        .map_or_else(
            || humantime::format_duration(step).to_string(),
            |(size, unit)| format!("{}-{unit}", seconds / size),
        )
}

/// `value` with a timestamp's fraction of a second cut to the second, or to the millisecond when `precise`, and an offset of whole hours shortened (`+00:00` to `+00`); anything else as it is.
fn trimmed_time(value: &str, precise: bool) -> String {
    let bytes = value.as_bytes();
    let is_timestamp = bytes.len() >= 19
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && matches!(bytes[10], b' ' | b'T')
        && bytes[13] == b':'
        && bytes[16] == b':';
    if !is_timestamp {
        return value.to_owned();
    }
    let (seconds, rest) = value.split_at(19);
    let digits = match rest.strip_prefix('.') {
        Some(fraction) => fraction.bytes().take_while(u8::is_ascii_digit).count(),
        None => 0,
    };
    let (fraction, offset) = match digits {
        0 => ("", rest),
        _ => rest.split_at(digits + 1),
    };
    let fraction = match precise {
        true => fraction.get(..4).unwrap_or(fraction),
        false => "",
    };
    let offset = offset
        .strip_suffix(":00")
        .filter(|hours| hours.len() == 3)
        .unwrap_or(offset);
    format!("{seconds}{fraction}{offset}")
}

/// A recorded time's clock part: `2026-11-14T07:54:11.123456Z` as `07:54:11Z`.
fn clock_time(recorded_at: &str) -> String {
    let time = recorded_at
        .split_once('T')
        .map_or(recorded_at, |(_, time)| time);
    let clock = time.get(..8).unwrap_or(time);
    match time.ends_with('Z') {
        true => format!("{clock}Z"),
        false => clock.to_owned(),
    }
}

/// What an error's reason says, and the headline `plan` gives it.
const HEADLINES: [(&str, &str); 2] = [
    ("the source reports no primary key", "no key to merge on"),
    ("no write_strategy is set", "no write_strategy"),
];

/// `item`, an `Errors` item for `problem` (a project problem, or a blocked table's reason), as `plan` prints it: a source that could not be read named by the source, with a headline, its driver's message, a fix and what it holds up (`blocks`); a table's problem named as its table lands, with a headline when its reason has a known one. Each detail is wrapped to the plan's error width.
pub(crate) fn text_error(plan: &ProjectPlan, problem: &str, item: PlanItem) -> PlanItem {
    let source = item
        .name
        .strip_prefix("[sources.")
        .and_then(|name| name.strip_suffix(']'));
    let unread = source.and_then(|source| Some((source, plan.unread_sources.get(source)?)));
    let item = match unread {
        Some((source, unread)) => {
            let reason = item
                .details
                .iter()
                .find(|detail| detail.label == "reason")
                .map_or_else(String::new, |detail| driver_message(&detail.text));
            let action = match [
                "connect",
                "login",
                "password",
                "authentication",
                "timed out",
            ]
            .iter()
            .any(|word| reason.to_lowercase().contains(word))
            {
                true => "can't connect",
                false => "can't read",
            };
            let mut details = vec![
                detail("reason", reason),
                detail(
                    "fix",
                    format!(
                        "check connection {}'s settings and credentials; sqlload debug tests every connection",
                        unread.connection
                    ),
                ),
            ];
            details.extend(blocks_line(plan, source, unread).map(|text| detail("blocks", text)));
            PlanItem {
                name: source.to_owned(),
                action: action.to_owned(),
                details,
            }
        }
        None => PlanItem {
            name: plan
                .problem_names
                .iter()
                .find(|(named, _)| named == problem)
                .map_or(item.name, |(_, display)| display.clone()),
            action: HEADLINES
                .iter()
                .find(|(says, _)| problem.contains(says))
                .map_or(item.action, |(_, headline)| (*headline).to_owned()),
            details: item.details,
        },
    };
    PlanItem {
        details: item
            .details
            .into_iter()
            .map(|detail| PlanDetail {
                text: wrapped(&detail.text),
                label: detail.label,
            })
            .collect(),
        ..item
    }
}

/// Whether `warning` is one `text_error` folds into an unread source's `blocks`.
pub(crate) fn folded_warning(plan: &ProjectPlan, warning: &str) -> bool {
    plan.unread_sharing.iter().any(|sharing| {
        sharing.warning == warning
            && sharing
                .sources
                .iter()
                .all(|source| plan.unread_sources.contains_key(source))
    })
}

/// What `source`, which could not be read, holds up: its tables, any its include patterns match, and the shared-table check on other tables.
fn blocks_line(plan: &ProjectPlan, source: &str, unread: &UnreadSource) -> Option<String> {
    let mut parts: Vec<String> = unread.tables.clone();
    if unread.patterns {
        parts.push("any table its include patterns match".to_owned());
    }
    parts.extend(
        plan.unread_sharing
            .iter()
            .filter(|sharing| sharing.sources.iter().any(|named| named == source))
            .map(|sharing| format!("the shared-table check on {}", listed(&sharing.tables))),
    );
    match parts.as_slice() {
        [] => None,
        [one] => Some(one.clone()),
        [first, second] => Some(format!("{first} and {second}")),
        [rest @ .., last] => Some(format!("{}, and {last}", rest.join(", "))),
    }
}

/// `names` as `a`, `a and b`, or `a, b and c`.
fn listed(names: &[String]) -> String {
    match names {
        [] => String::new(),
        [only] => only.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

/// An ODBC error as its driver's message with its state and native error, `Login failed for user 'sa' (ODBC state 28000, native error 18456)`; other text as it is.
fn driver_message(text: &str) -> String {
    let field = |name: &str| -> Option<&str> {
        let (_, after) = text.split_once(name)?;
        Some(after.split([',', '\n']).next().unwrap_or(after).trim())
    };
    let message = text.split_once("Message: ").map(|(_, message)| {
        message
            .rsplit(']')
            .next()
            .unwrap_or(message)
            .trim()
            .trim_end_matches('.')
    });
    match (message, field("State: "), field("Native error: ")) {
        (Some(message), Some(state), Some(native)) => {
            format!("{message} (ODBC state {state}, native error {native})")
        }
        _ => text.to_owned(),
    }
}

/// `text` wrapped at word boundaries so each line fits the plan's error width after a branch, its indent and a label, its lines joined by `\n`, which `tree` lines up under the first; a line already laid out in columns (holding two spaces in a row) is left as it is.
fn wrapped(text: &str) -> String {
    let width = PLAN_ERROR_WIDTH - 8 - LABEL_WIDTH;
    let mut lines: Vec<String> = Vec::new();
    for paragraph in text.split('\n') {
        if paragraph.contains("  ") {
            lines.push(paragraph.to_owned());
            continue;
        }
        let mut line = String::new();
        for word in paragraph.split(' ') {
            if !line.is_empty() && line.chars().count() + 1 + word.chars().count() > width {
                lines.push(std::mem::take(&mut line));
            }
            if !line.is_empty() {
                line.push(' ');
            }
            line.push_str(word);
        }
        lines.push(line);
    }
    lines.join("\n")
}
