//! The tally `apply` and `load` end with.

use std::time::Duration;

use crate::loader::_helpers::render::thousands;
use crate::loader::models::TallyLine;

/// One aligned line per object created, altered, loaded or failed, then a final line counting them; and whether nothing failed.
pub fn render_tally(lines: &[TallyLine], (rows, elapsed): (u64, Duration)) -> (Vec<String>, bool) {
    let name_width = lines.iter().map(|line| line.name.len()).max().unwrap_or(0);
    let mut out: Vec<String> = lines
        .iter()
        .map(|line| {
            format!(
                "{:<8} {:<name_width$}  {}",
                line.verb, line.name, line.detail
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
