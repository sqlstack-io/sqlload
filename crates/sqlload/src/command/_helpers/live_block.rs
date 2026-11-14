//! The block `load` redraws in place on a terminal while tables load: how many are done, each running table and what it is doing, what waits, and the rate limits in use. Drawn from the load events alone.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::load_events::models::{EventKind, LoadEvent};
use crate::loader::_helpers::render::thousands;

/// Spinner frames, one per redraw.
const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

/// How many queued tables the block names before `…`.
const QUEUED_NAMED: usize = 3;

/// The width of a running table's name column.
const NAME_COLUMN: usize = 28;

/// The width of the table-count bar's surroundings, as the mock lays them out: the terminal's width less this is the bar's.
const HEADER_TEXT: usize = 46;

/// The widest the block draws, however wide the terminal.
const MAX_WIDTH: usize = 112;

/// How long after a wait a connection's rate limit still shows as in use.
const RATE_RECENT: Duration = Duration::from_millis(1500);

/// A table loading now.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Running {
    table: String,
    strategy: String,
    rows: u64,
    /// What it is doing: `batch 3`, `committing`, or empty before its first progress.
    doing: String,
    committing: bool,
}

/// What the block shows, as the events left it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LiveState {
    /// The tables to load, in the order they may start.
    waiting: Vec<String>,
    total: usize,
    done: usize,
    /// Rows of the tables that finished.
    finished_rows: u64,
    running: Vec<Running>,
    fail_fast: bool,
    /// The table whose failure stopped new tables starting under `--fail-fast`.
    stopped_after: Option<String>,
    /// Each connection with a `rate_limit`: its limit and when a request last waited, as time since the load started.
    rates: BTreeMap<String, (String, Option<Duration>)>,
}

impl LiveState {
    /// The state before any event: `tables` to load, in plan order, and whether the load stops starting tables at the first failure.
    pub(crate) fn new(tables: Vec<String>, fail_fast: bool) -> Self {
        Self {
            total: tables.len(),
            waiting: tables,
            fail_fast,
            ..Self::default()
        }
    }

    /// `event`, which happened `at` into the load, taken in.
    pub(crate) fn apply(&mut self, event: &LoadEvent, at: Duration) {
        let table = event.table.clone().unwrap_or_default();
        match &event.kind {
            EventKind::TableStarted { strategy } => {
                self.waiting.retain(|waiting| *waiting != table);
                self.running.push(Running {
                    table,
                    strategy: strategy.clone(),
                    rows: 0,
                    doing: String::new(),
                    committing: false,
                });
            }
            EventKind::TableProgress { unit, count, rows } => {
                if let Some(running) = self.running_mut(&table) {
                    running.rows = *rows;
                    running.doing = format!("{unit} {count}");
                }
            }
            EventKind::TableCommitting { rows } => {
                if let Some(running) = self.running_mut(&table) {
                    running.rows = *rows;
                    running.doing = "committing".to_owned();
                    running.committing = true;
                }
            }
            EventKind::RateLimited { connection, rate, .. } => {
                self.rates
                    .insert(connection.clone(), (rate.clone(), Some(at)));
            }
            EventKind::TableDone { rows, .. } => {
                self.running.retain(|running| running.table != table);
                self.done += 1;
                self.finished_rows += rows;
            }
            EventKind::TableFailed { .. } => {
                self.running.retain(|running| running.table != table);
                if self.fail_fast && self.stopped_after.is_none() {
                    self.stopped_after = Some(table);
                }
            }
            EventKind::TableNotStarted { .. } => {
                self.waiting.retain(|waiting| *waiting != table);
            }
            EventKind::RunStarted { .. } | EventKind::RunFinished { .. } => {}
        }
    }

    /// The running table named `table`.
    fn running_mut(&mut self, table: &str) -> Option<&mut Running> {
        self.running
            .iter_mut()
            .find(|running| running.table == table)
    }
}

/// The block's lines for `state`, `elapsed` into the load, at redraw `frame`, for a terminal `width` columns wide. Plain text: colour is the caller's.
pub(crate) fn live_lines(
    state: &LiveState,
    (elapsed, frame): (Duration, usize),
    width: usize,
) -> Vec<String> {
    let width = width.min(MAX_WIDTH);
    let spinner = SPINNER[frame % SPINNER.len()];
    let rows = state.finished_rows + state.running.iter().map(|running| running.rows).sum::<u64>();
    let bar_width = width.saturating_sub(HEADER_TEXT).max(10);
    let filled = match state.total {
        0 => bar_width,
        total => bar_width * state.done / total,
    };
    let seconds = elapsed.as_secs();
    let mut lines = vec![
        format!(
            " LOADING  {}{}  {}/{} tables · {:>5} rows · {:02}:{:02}",
            "━".repeat(filled),
            "━".repeat(bar_width - filled),
            state.done,
            state.total,
            thousands(rows),
            seconds / 60,
            seconds % 60
        ),
        String::new(),
    ];
    for running in &state.running {
        lines.push(format!(
            "  {spinner} {:<NAME_COLUMN$} {:<8} {:>6} {}",
            running.table,
            running.strategy,
            thousands(running.rows),
            running.doing
        ));
    }
    if let Some(failed) = &state.stopped_after {
        lines.push(format!(
            "  ⊘ fail-fast: {} tables will not start after {failed} failed; running tables finish",
            state.waiting.len()
        ));
    } else if !state.waiting.is_empty() {
        let named: Vec<&str> = state
            .waiting
            .iter()
            .take(QUEUED_NAMED)
            .map(String::as_str)
            .collect();
        let more = match state.waiting.len() > QUEUED_NAMED {
            true => ", …",
            false => "",
        };
        lines.push(format!(
            "  · {} queued: {}{more}",
            state.waiting.len(),
            named.join(", ")
        ));
    }
    lines.push(String::new());
    if !state.rates.is_empty() {
        let meters: Vec<String> = state
            .rates
            .iter()
            .map(|(connection, (rate, waited))| {
                let busy = waited.is_some_and(|at| elapsed.saturating_sub(at) < RATE_RECENT);
                match busy {
                    true => format!("{connection} ▮▮▮▮▮▮▮▮ at {rate}"),
                    false => format!("{connection} ▯▯▯▯▯▯▯▯ under {rate}"),
                }
            })
            .collect();
        lines.push(format!("  rate  {}", meters.join("   ")));
    }
    lines.push(
        "  ctrl-c finishes running tables and stops · a second ctrl-c stops at once".to_owned(),
    );
    lines
}
