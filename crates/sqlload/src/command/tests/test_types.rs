pub(crate) struct LoaderCommandTestCase {
    pub(crate) description: &'static str,
    pub(crate) project: &'static str,
    /// Text the error must contain, or `""` when the command succeeds.
    pub(crate) expected_error: &'static str,
}

pub(crate) struct BlockingThreadsTestCase {
    pub(crate) description: &'static str,
    pub(crate) cli_threads: Option<u16>,
    pub(crate) config: &'static str,
    pub(crate) expected_threads: Option<usize>,
}

pub(crate) struct LoadOptionsTestCase {
    pub(crate) description: &'static str,
    pub(crate) argv: &'static [&'static str],
    /// `select | exclude | start | end`, or text the parse error must contain.
    pub(crate) expected_options: Result<&'static str, &'static str>,
}

pub(crate) struct PlanFailureTestCase {
    pub(crate) description: &'static str,
    pub(crate) project: &'static str,
    pub(crate) argv: &'static [&'static str],
    /// How the output starts.
    pub(crate) expected_output: &'static str,
    /// The last line: `exit 0` or `exit 1`.
    pub(crate) expected_exit: &'static str,
}

pub(crate) struct PaintTestCase {
    pub(crate) description: &'static str,
    pub(crate) line: &'static str,
    pub(crate) expected_line: &'static str,
}

pub(crate) struct FolderOverlapTestCase {
    pub(crate) description: &'static str,
    /// The files target's `path` template, below the root the source reads `data/orders/` from.
    pub(crate) target_path: &'static str,
    /// Whether the target writes one file per table.
    pub(crate) single_file: bool,
    /// Text the plan must contain; empty for none.
    pub(crate) expected_problem: &'static str,
    /// The last line: `exit 0` or `exit 1`.
    pub(crate) expected_exit: &'static str,
}

pub(crate) struct ExitCodeTestCase {
    pub(crate) description: &'static str,
    pub(crate) project: &'static str,
    pub(crate) argv: &'static [&'static str],
    /// Text the output holds.
    pub(crate) expected_output: &'static str,
    /// The last line: `exit 1` or `exit 75`.
    pub(crate) expected_exit: &'static str,
}

/// Load events in, the lines `load` prints for them out.
pub(crate) struct LoadOutputTestCase {
    pub(crate) description: &'static str,
    /// Each event's table and kind, in order.
    pub(crate) events: Vec<(Option<&'static str>, crate::load_events::models::EventKind)>,
    pub(crate) name_width: usize,
    pub(crate) elapsed_ms: u64,
    pub(crate) exit_code: u8,
    pub(crate) expected_lines: &'static str,
}
