//! What `load` writes as it runs, rendered from its load events: a line per table as it finishes, `--events` as JSON lines, the summary it ends with, and on a terminal the block it redraws in place while tables load: how many are done, each running table and what it is doing, what waits, and the rate limits in use.

use std::collections::BTreeMap;
use std::io::Write;
use std::time::Duration;

use tokio::sync::mpsc::UnboundedReceiver;

use crate::command::_helpers::output::print_lines;
use crate::command::constants::PLAN_TREE_HINT;
use crate::load_events::main::event_line::event_line;
use crate::load_events::models::{EventKind, LoadEvent};
use crate::loader::constants::{BOLD, CYAN, DIM, INVERSE, RED, RESET, YELLOW};
use crate::loader::models::ProjectPlan;
use crate::number_text::main::thousands::thousands;

/// The width of a tally line's verb column: `loaded` or `failed` and two spaces.
const VERB_COLUMN: usize = 8;

/// The width of a summary branch's label column.
const SUMMARY_LABEL: usize = 13;

/// How often the live block redraws.
const LIVE_REDRAW: std::time::Duration = std::time::Duration::from_millis(100);

/// The most redraws a second the live block's draw target allows.
const LIVE_HZ: u8 = 10;

/// The width the live block assumes when the terminal's is unknown.
const MAX_LIVE_WIDTH: usize = 112;

/// How many tables a summary names before `+N more`.
const NAMED_TABLES: usize = 3;

/// Spinner frames, one per redraw.
const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

/// How many queued tables the block names before `…`.
const QUEUED_NAMED: usize = 3;

/// The width of a running table's name column.
const NAME_COLUMN: usize = 28;

/// The widest the block draws, however wide the terminal.
const MAX_WIDTH: usize = 112;

/// How long after a wait a connection's rate limit still shows as in use.
const RATE_RECENT: Duration = Duration::from_millis(1500);

/// The columns of the live block's bar line besides the bar and its counts: the badge, a space before the bar, two spaces before the counts, and one short of the edge so the line never wraps.
const BAR_LINE_GAPS: usize = " LOADING ".len() + 3 + 1;

/// Where a load's lines go: printed as they come (`print`), or kept to return with the rest.
#[derive(Debug)]
pub(crate) struct LoadOutput {
    /// The width of the table-name column: the widest selected table's name and two spaces.
    pub(crate) name_width: usize,
    /// The width of the rows column: the widest row count the plan expects, with separators, and at least 5.
    pub(crate) rows_width: usize,
    pub(crate) print: bool,
    pub(crate) no_color: bool,
    /// Where `--events` writes each event as a JSON line, if anywhere.
    pub(crate) events: Option<EventsTarget>,
    /// The block drawn on stderr while tables load, when it is drawn.
    pub(crate) live: Option<LiveState>,
}

/// Where `--events` writes: stderr, or the `--events-file`.
#[derive(Debug)]
pub(crate) enum EventsTarget {
    Stderr,
    File(std::fs::File),
}

impl EventsTarget {
    /// Write `line` and a newline, and flush it.
    fn write_line(&mut self, line: &str) {
        let _ = match self {
            Self::Stderr => {
                let mut stderr = std::io::stderr().lock();
                writeln!(stderr, "{line}").and_then(|()| stderr.flush())
            }
            Self::File(file) => writeln!(file, "{line}").and_then(|()| file.flush()),
        };
    }
}

/// What the events said happened, for the summary.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Tally {
    pub(crate) loaded: usize,
    pub(crate) failed: usize,
    pub(crate) not_started: Vec<String>,
    pub(crate) rows: u64,
    /// The table that took longest to load, and how long.
    pub(crate) slowest: Option<(String, u64)>,
}

impl Tally {
    /// `event` counted in.
    pub(crate) fn add(&mut self, event: &LoadEvent) {
        let table = event.table.clone().unwrap_or_default();
        match &event.kind {
            EventKind::TableDone {
                rows, elapsed_ms, ..
            } => {
                self.loaded += 1;
                self.rows += rows;
                if self
                    .slowest
                    .as_ref()
                    .is_none_or(|(_, slowest)| elapsed_ms > slowest)
                {
                    self.slowest = Some((table, *elapsed_ms));
                }
            }
            EventKind::TableFailed { .. } => self.failed += 1,
            EventKind::TableNotStarted { .. } => self.not_started.push(table),
            _ => {}
        }
    }
}

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
            EventKind::RateLimited {
                connection, rate, ..
            } => {
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

/// The tally lines `event` prints: `loaded` with its rows, time and new position (after the `created` and `altered` lines of a table that made its columns as it loaded), or `failed` with why. Names are padded to `name_width` and row counts to `rows_width`, so lines printed one at a time still line up.
pub(crate) fn event_lines(
    event: &LoadEvent,
    (name_width, rows_width): (usize, usize),
) -> Vec<String> {
    let name = event.table.as_deref().unwrap_or_default();
    let (verb, detail) = match &event.kind {
        EventKind::TableDone {
            rows,
            elapsed_ms,
            position,
            changes,
        } => {
            let word = match rows {
                1 => "row",
                _ => "rows",
            };
            let seconds = format!("{:.1}s", *elapsed_ms as f64 / 1000.0);
            let detail = format!(
                "{:>rows_width$} {word:<5}{seconds:<6}{}",
                thousands(*rows),
                position.as_deref().unwrap_or_default()
            );
            let mut lines: Vec<String> = changes
                .iter()
                .map(|change| change_tally_line(change, name_width))
                .collect();
            lines.push(tally_line(("loaded", name), &detail, name_width));
            return lines;
        }
        EventKind::TableFailed { reason, .. } => ("failed", reason.clone()),
        _ => return Vec::new(),
    };
    vec![tally_line((verb, name), &detail, name_width)]
}

/// `verb  name  detail` in the tally's columns, a detail's further lines under its first; a verb longer than `loaded` (`created`, `altered`), or a name wider than the column (a child table made as its parent loads), keeps two spaces after it.
fn tally_line((verb, name): (&str, &str), detail: &str, name_width: usize) -> String {
    let verb_width = VERB_COLUMN.max(verb.len() + 2);
    let name_width = name_width.max(name.len() + 2);
    let continued = format!("\n{:width$}", "", width = verb_width + name_width);
    format!(
        "{verb:<verb_width$}{name:<name_width$}{}",
        detail.replace('\n', &continued)
    )
    .trim_end()
    .to_owned()
}

/// A `created` or `altered` line `apply` reported, in the tally's columns.
pub(crate) fn change_tally_line(line: &str, name_width: usize) -> String {
    let mut parts = line.splitn(2, char::is_whitespace);
    let verb = parts.next().unwrap_or_default();
    let rest = parts.next().unwrap_or_default().trim_start();
    let (name, detail) = rest.split_once("  ").unwrap_or((rest, ""));
    tally_line((verb, name), detail.trim_start(), name_width)
}

/// What `load` prints of its plan before it starts: the closing line, pointing a complete plan to `sqlload plan`, and the Errors and Warnings sections when there are any; the Objects, Loads and Skipped trees and the `Plan ready` line are `plan`'s (and Objects `apply`'s), and the `created` and `altered` lines follow.
pub(crate) fn start_lines(plan_lines: Vec<String>) -> Vec<String> {
    let dropped = ["Plan ready", "Objects (", "Loads (", "Skipped ("];
    let mut kept: Vec<String> = Vec::new();
    for section in plan_lines.split(String::is_empty) {
        let first = section.first().map_or("", String::as_str);
        if section.is_empty() || dropped.iter().any(|heading| first.starts_with(heading)) {
            continue;
        }
        kept.extend(section.iter().cloned());
        kept.push(String::new());
    }
    kept.pop();
    if let Some(closing) = kept.last_mut()
        && closing.starts_with('✓')
    {
        closing.push_str(&format!("   {PLAN_TREE_HINT}"));
    }
    kept
}

/// Write each event from `receiver` as it comes: to `--events`, and as its tally line, printed or kept; until every sender is gone. The live block, when drawn, redraws about ten times a second under the printed lines, and is cleared at the end. Returns the kept lines and the tally.
pub(crate) async fn write_events(
    mut output: LoadOutput,
    mut receiver: UnboundedReceiver<LoadEvent>,
) -> (Vec<String>, Tally) {
    let mut kept: Vec<String> = Vec::new();
    let mut tally = Tally::default();
    let started = std::time::Instant::now();
    let block = output.live.as_ref().map(|_| live_bar());
    let width = console::Term::stderr()
        .size_checked()
        .map_or(MAX_LIVE_WIDTH, |(_, columns)| usize::from(columns));
    let mut ticks = tokio::time::interval(LIVE_REDRAW);
    let mut frame: usize = 0;
    let coloured = !output.no_color && std::env::var_os("NO_COLOR").is_none();
    loop {
        let event = tokio::select! {
            event = receiver.recv() => event,
            _ = ticks.tick(), if block.is_some() => {
                frame += 1;
                if let (Some(bar), Some(state)) = (&block, &output.live) {
                    bar.set_message(live_lines(state, (started.elapsed(), frame), (width, coloured)).join("\n"));
                }
                continue;
            }
        };
        let Some(event) = event else {
            break;
        };
        if let Some(events) = output.events.as_mut() {
            events.write_line(&event_line(&event));
        }
        tally.add(&event);
        if let Some(state) = output.live.as_mut() {
            state.apply(&event, started.elapsed());
        }
        let lines = event_lines(&event, (output.name_width, output.rows_width));
        match (output.print, &block) {
            (true, _) if lines.is_empty() => {}
            (true, Some(bar)) => bar.suspend(|| print_lines(lines, output.no_color)),
            (true, None) => print_lines(lines, output.no_color),
            (false, _) => kept.extend(lines),
        }
    }
    if let Some(bar) = block {
        bar.finish_and_clear();
    }
    (kept, tally)
}

/// The live block's one bar on stderr: a message of several lines, redrawn in place.
fn live_bar() -> indicatif::ProgressBar {
    let bar = indicatif::ProgressBar::with_draw_target(
        None,
        indicatif::ProgressDrawTarget::stderr_with_hz(LIVE_HZ),
    );
    if let Ok(style) = indicatif::ProgressStyle::with_template("{msg}") {
        bar.set_style(style);
    }
    bar
}

/// The line a load ends with, and the branches under it: what never started, the slowest table, and after failures whether running it again helps and what it would do.
pub(crate) fn summary_lines(tally: &Tally, elapsed_ms: u64, exit_code: u8) -> Vec<String> {
    let mut clauses: Vec<String> = Vec::new();
    clauses.extend((tally.loaded > 0).then(|| format!("{} loaded", tally.loaded)));
    clauses.extend((tally.failed > 0).then(|| format!("{} failed", tally.failed)));
    clauses.extend(
        (!tally.not_started.is_empty()).then(|| format!("{} not started", tally.not_started.len())),
    );
    clauses.extend((tally.loaded > 0).then(|| match tally.rows {
        1 => "1 row".to_owned(),
        rows => format!("{} rows", thousands(rows)),
    }));
    clauses.push(format!("{:.1}s", elapsed_ms as f64 / 1000.0));
    let failures = tally.failed > 0 || !tally.not_started.is_empty();
    let mut branches: Vec<(&str, String)> = Vec::new();
    if !tally.not_started.is_empty() {
        let named: Vec<&str> = tally
            .not_started
            .iter()
            .take(NAMED_TABLES)
            .map(String::as_str)
            .collect();
        let more = match tally.not_started.len().saturating_sub(NAMED_TABLES) {
            0 => String::new(),
            more => format!(", +{more} more"),
        };
        branches.push((
            "not started",
            format!("{}{more} (--fail-fast)", named.join(", ")),
        ));
    }
    if let Some((table, elapsed_ms)) = &tally.slowest {
        branches.push((
            "slowest",
            format!("{table} {:.1}s", *elapsed_ms as f64 / 1000.0),
        ));
    }
    if failures && exit_code == crate::command::constants::RETRYABLE_EXIT {
        branches.push((
            "retry",
            format!("every failure is retryable, so exit {exit_code}: run the same command again"),
        ));
    }
    if failures {
        let unchanged = match tally.failed + tally.not_started.len() {
            1 => "the 1 unchanged table loads".to_owned(),
            count => format!("the {count} unchanged tables load"),
        };
        branches.push((
            "next",
            match tally.loaded {
                0 => unchanged,
                1 => format!("{unchanged}; the 1 that committed carries on from its position"),
                count => {
                    format!("{unchanged}; the {count} that committed carry on from their positions")
                }
            },
        ));
    }
    let mut lines = vec![match tally.failed {
        0 => format!("✓ Completed  {}", clauses.join(", ")),
        _ => format!("✗ Completed with failures  {}", clauses.join(", ")),
    }];
    for (index, (label, text)) in branches.iter().enumerate() {
        let branch = match index + 1 == branches.len() {
            true => "└── ",
            false => "├── ",
        };
        lines.push(format!("{branch}{label:<SUMMARY_LABEL$}{text}"));
    }
    lines
}

/// `work`, a plan of a project with `sources` sources, with one line on stderr while it runs when `draws`: a spinner, `Planning` and how many sources are done, redrawn by a thread of its own (planning may hold up the async runtime) and cleared before anything else prints.
pub(crate) async fn planning<T>(
    work: impl std::future::Future<Output = T>,
    sources: usize,
    (draws, no_color): (bool, bool),
) -> T {
    if !draws {
        return work.await;
    }
    let bar = indicatif::ProgressBar::with_draw_target(
        None,
        indicatif::ProgressDrawTarget::stderr_with_hz(LIVE_HZ),
    );
    let template = match no_color {
        true => "{spinner} Planning  {msg}",
        false => "{spinner:.cyan} Planning  {msg:.dim}",
    };
    if let Ok(style) = indicatif::ProgressStyle::with_template(template) {
        bar.set_style(style.tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "));
    }
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let drawer = {
        let (bar, done) = (bar.clone(), done.clone());
        std::thread::spawn(move || {
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                bar.set_message(format!(
                    "checking sources, {} of {sources} done",
                    ProjectPlan::planned_sources()
                ));
                bar.tick();
                std::thread::sleep(LIVE_REDRAW);
            }
        })
    };
    let output = work.await;
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    if drawer.join().is_err() {
        bar.abandon();
    }
    bar.finish_and_clear();
    output
}

/// The block's lines, after a blank one that parts it from the finished tables' lines, for `state`, `elapsed` into the load, at redraw `frame`, for a terminal `width` columns wide; `coloured` as the mock has it: cyan for what runs, red only for a failure, yellow for waits and fail-fast, dim for notes.
pub(crate) fn live_lines(
    state: &LiveState,
    (elapsed, frame): (Duration, usize),
    (width, coloured): (usize, bool),
) -> Vec<String> {
    let paint = |code: &str, text: String| match coloured && !text.is_empty() {
        true => format!("{code}{text}{RESET}"),
        false => text,
    };
    let width = width.min(MAX_WIDTH);
    let spinner = paint(CYAN, SPINNER[frame % SPINNER.len()].to_string());
    let rows = state.finished_rows
        + state
            .running
            .iter()
            .map(|running| running.rows)
            .sum::<u64>();
    let seconds = elapsed.as_secs();
    let counts = format!(
        "{}/{} tables · {:>5} rows · {:02}:{:02}",
        state.done,
        state.total,
        thousands(rows),
        seconds / 60,
        seconds % 60
    );
    let bar_width = width
        .saturating_sub(BAR_LINE_GAPS + counts.chars().count())
        .max(10);
    let filled = match state.total {
        0 => bar_width,
        total => bar_width * state.done / total,
    };
    let mut lines = vec![
        String::new(),
        format!(
            "{} {}{}  {counts}",
            paint(&format!("{BOLD}{INVERSE}{CYAN}"), " LOADING ".to_owned()),
            paint(
                match state.stopped_after {
                    Some(_) => RED,
                    None => CYAN,
                },
                "━".repeat(filled)
            ),
            paint(DIM, "━".repeat(bar_width - filled)),
        ),
        String::new(),
    ];
    for running in &state.running {
        lines.push(format!(
            "  {spinner} {:<NAME_COLUMN$} {:<8} {:>6} {}",
            running.table,
            running.strategy,
            thousands(running.rows),
            paint(
                match running.committing {
                    true => CYAN,
                    false => DIM,
                },
                running.doing.clone()
            )
        ));
    }
    if let Some(failed) = &state.stopped_after {
        lines.push(paint(
            YELLOW,
            format!(
                "  ⊘ fail-fast: {} will not start after {failed} failed; running tables finish",
                match state.waiting.len() {
                    1 => "1 table".to_owned(),
                    count => format!("{count} tables"),
                }
            ),
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
        lines.push(paint(
            DIM,
            format!(
                "  · {} queued: {}{more}",
                state.waiting.len(),
                named.join(", ")
            ),
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
                    true => format!(
                        "{connection} {}",
                        paint(YELLOW, format!("▮▮▮▮▮▮▮▮ at {rate}"))
                    ),
                    false => format!(
                        "{connection} {}",
                        paint(DIM, format!("▯▯▯▯▯▯▯▯ under {rate}"))
                    ),
                }
            })
            .collect();
        lines.push(format!(
            "  {}{}",
            paint(DIM, "rate  ".to_owned()),
            meters.join("   ")
        ));
    }
    lines.push(paint(
        DIM,
        "  ctrl-c finishes running tables and stops · a second ctrl-c stops at once".to_owned(),
    ));
    lines
}
