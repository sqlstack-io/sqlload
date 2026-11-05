//! The tally `apply` and `load` end with.

use std::time::Duration;

use crate::loader::_helpers::render::thousands;
use crate::loader::models::{TableOutcome, TallyLine};

/// `created  name  detail`, as `apply` and a Python table's load report a change, as a tally line.
fn change_line(line: &str) -> TallyLine {
    let mut parts = line.splitn(3, "  ");
    let verb = match parts.next().unwrap_or_default() {
        "created" => "created",
        "recreated" => "recreated",
        _ => "altered",
    };
    TallyLine {
        verb,
        name: parts.next().unwrap_or_default().to_owned(),
        detail: parts.next().unwrap_or_default().to_owned(),
    }
}

/// One aligned line per object `applied` created or altered and per table loaded or failed, then a final line counting them; and whether nothing failed.
pub fn render_tally(
    applied: &[String],
    outcomes: &[TableOutcome],
    elapsed: Duration,
) -> (Vec<String>, bool) {
    let mut lines: Vec<TallyLine> = applied.iter().map(|line| change_line(line)).collect();
    let mut rows: u64 = 0;
    for outcome in outcomes {
        let seconds = format!("{:.1}s", outcome.elapsed.as_secs_f64());
        let (verb, detail) = match &outcome.loaded {
            Ok(loaded) => {
                lines.extend(loaded.changes.iter().map(|line| change_line(line)));
                rows += loaded.rows;
                let note = loaded
                    .note
                    .as_ref()
                    .map(|note| format!(" {note}"))
                    .unwrap_or_default();
                (
                    "loaded",
                    format!("{} rows{note}  {seconds}", thousands(loaded.rows)),
                )
            }
            Err(reason) => ("failed", reason.clone()),
        };
        lines.push(TallyLine {
            verb,
            name: outcome.table.clone(),
            detail,
        });
    }
    let name_width = lines.iter().map(|line| line.name.len()).max().unwrap_or(0);
    let verb_width = lines
        .iter()
        .map(|line| line.verb.len())
        .chain(["created".len()])
        .max()
        .unwrap_or(0);
    let continued = format!("\n{:width$}", "", width = verb_width + name_width + 4);
    let mut out: Vec<String> = lines
        .iter()
        .map(|line| {
            format!(
                "{:<verb_width$}  {:<name_width$}  {}",
                line.verb,
                line.name,
                line.detail.replace('\n', &continued)
            )
            .trim_end()
            .to_owned()
        })
        .collect();
    let count = |verb: &str| lines.iter().filter(|line| line.verb == verb).count();
    let (loaded, failed) = (count("loaded"), count("failed"));
    let mut clauses: Vec<String> = Vec::new();
    clauses.extend((loaded > 0).then(|| format!("{loaded} loaded")));
    clauses.extend((failed > 0).then(|| format!("{failed} failed")));
    clauses.extend((loaded > 0).then(|| format!("{} rows", thousands(rows))));
    clauses.push(format!("{:.1}s", elapsed.as_secs_f64()));
    if !out.is_empty() {
        out.push(String::new());
    }
    out.push(match failed {
        0 => format!("✓ Completed  {}", clauses.join(", ")),
        _ => format!("✗ Completed with failures  {}", clauses.join(", ")),
    });
    (out, failed == 0)
}
