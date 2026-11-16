//! The steps before a plan: connecting to targets and sources and finding the sources' tables.

use std::sync::Mutex;
use std::time::Instant;

use crate::load_events::constants::SYSTEM_NAMES;
use crate::load_events::models::{PhaseTiming, Role};

/// A step running now.
#[derive(Debug)]
pub(crate) struct Step {
    pub(crate) id: u64,
    pub(crate) subject: String,
    pub(crate) doing: String,
    pub(crate) started: Instant,
}

/// The steps' state: whether they are tracked (only while the command line listens for load events), the one running, the phase lines not shown yet (`lines` of steps that failed or took [`SLOW_STEP`](crate::load_events::constants::SLOW_STEP) or longer, `quiet` of the others), every phase's time, and what the summary lines say: each connection opened, as `erp PostgreSQL`, and the time they took, and the tables, sources and time of the discoveries that ended well.
#[derive(Debug, Default)]
pub(crate) struct Steps {
    pub(crate) tracking: bool,
    pub(crate) current: Option<Step>,
    pub(crate) lines: Vec<String>,
    pub(crate) quiet: Vec<String>,
    pub(crate) timings: Vec<PhaseTiming>,
    pub(crate) next_id: u64,
    pub(crate) connected: Vec<String>,
    pub(crate) connect_ms: u64,
    pub(crate) discovered: (usize, usize, u64),
}

/// The process's steps.
pub(crate) static STEPS: Mutex<Steps> = Mutex::new(Steps {
    tracking: false,
    current: None,
    lines: Vec::new(),
    quiet: Vec::new(),
    timings: Vec::new(),
    next_id: 0,
    connected: Vec::new(),
    connect_ms: 0,
    discovered: (0, 0, 0),
});

/// The line a phase ends with: `✓ Source connected  erp  OPENEDGE through ODBC  (1.84s)`, `✓ Discovered  erp  2 tables  (21.30s)`, or the failure with its time.
pub(crate) fn phase_line(
    (role, phase, ok): (Role, &str, bool),
    (subject, detail): (&str, &str),
    seconds: f64,
) -> String {
    let who = match role {
        Role::Source => "Source",
        Role::Target => "Target",
    };
    let (label, time) = match (phase, ok) {
        ("connect", true) => (format!("{who} connected"), format!("({seconds:.2}s)")),
        ("connect", false) => (
            format!("{who} connection failed"),
            format!("(after {seconds:.2}s)"),
        ),
        (_, true) => ("Discovered".to_owned(), format!("({seconds:.2}s)")),
        (_, false) => (
            "Discovery failed".to_owned(),
            format!("(after {seconds:.2}s)"),
        ),
    };
    let mark = match ok {
        true => '\u{2713}',
        false => '\u{2717}',
    };
    match detail.is_empty() {
        true => format!("{mark} {label}  {subject}  {time}"),
        false => format!("{mark} {label}  {subject}  {detail}  {time}"),
    }
}

/// The system a connection's `label` names, as people write it: `POSTGRESQL through ODBC` is `PostgreSQL`, `MICROSOFT SQL SERVER through ODBC` `SQL Server`; a name [`SYSTEM_NAMES`] doesn't know stays as the label gives it.
pub(crate) fn system_name(label: &str) -> String {
    let system = label.strip_suffix(" through ODBC").unwrap_or(label);
    SYSTEM_NAMES
        .iter()
        .find(|(upper, _)| *upper == system)
        .map_or_else(|| system.to_owned(), |(_, name)| (*name).to_owned())
}
