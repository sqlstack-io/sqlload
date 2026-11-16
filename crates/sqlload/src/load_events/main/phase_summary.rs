//! The lines the steps before the plan collapse into once they are done.

use crate::load_events::_helpers::steps::STEPS;

/// `✓ Connected   warehouse DuckDB · erp PostgreSQL   0.05s` and `✓ Discovered  17 tables in 15 sources   0.04s`: the connections that opened and the discoveries that ended well within [`SLOW_STEP`](crate::load_events::constants::SLOW_STEP) (a slower step has its own line), with the time they took together (steps run one at a time); either line left out when no such step ran. Taken, so they print once.
pub fn phase_summary() -> Vec<String> {
    let Ok(mut steps) = STEPS.lock() else {
        return Vec::new();
    };
    let connected = std::mem::take(&mut steps.connected);
    let (tables, sources, discover_ms) = std::mem::take(&mut steps.discovered);
    let connect_ms = std::mem::take(&mut steps.connect_ms);
    let seconds = |millis: u64| format!("{:.2}s", millis as f64 / 1000.0);
    let mut lines: Vec<String> = Vec::new();
    if !connected.is_empty() {
        lines.push(format!(
            "\u{2713} Connected   {}   {}",
            connected.join(" \u{b7} "),
            seconds(connect_ms)
        ));
    }
    if sources > 0 {
        let tables = match tables {
            1 => "1 table".to_owned(),
            count => format!("{count} tables"),
        };
        let sources = match sources {
            1 => "1 source".to_owned(),
            count => format!("{count} sources"),
        };
        lines.push(format!(
            "\u{2713} Discovered  {tables} in {sources}   {}",
            seconds(discover_ms)
        ));
    }
    lines
}
