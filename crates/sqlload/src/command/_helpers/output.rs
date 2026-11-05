use std::io::{IsTerminal, Write};

use crate::loader::constants::{BOLD, DIM, GREEN, RED, RESET, YELLOW};

/// The colour a line of `plan`, `apply` or `load` output is printed in, if any: bold headings, red errors and failures, yellow warnings, green completions.
fn colour(line: &str) -> Option<&'static str> {
    let starts = |prefixes: &[&str]| prefixes.iter().any(|prefix| line.starts_with(prefix));
    match line.chars().next() {
        Some('✓') => Some(GREEN),
        Some('✗') => Some(RED),
        _ if starts(&["Errors (", "failed "]) => Some(RED),
        _ if starts(&["Warnings ("]) => Some(YELLOW),
        _ if starts(&["Plan ready", "Objects (", "Loads (", "Skipped ("]) => Some(BOLD),
        _ => None,
    }
}

/// `line` coloured, with a heading's command note dimmed in place.
fn paint(line: String) -> String {
    let Some(code) = colour(&line) else {
        return line;
    };
    let note = line.trim_end().rsplit("  ").next().unwrap_or_default();
    match line.starts_with("Objects (") || line.starts_with("Loads (") {
        true => {
            let left = &line[..line.len() - note.len()];
            format!(
                "{code}{}{RESET}{}{DIM}{note}{RESET}",
                left.trim_end(),
                &left[left.trim_end().len()..]
            )
        }
        false => format!("{code}{line}{RESET}"),
    }
}

/// Write `lines` to stdout, in colour on a terminal unless `no_color` or `NO_COLOR` says otherwise.
pub(crate) fn print_lines(lines: Vec<String>, no_color: bool) {
    let coloured =
        !no_color && std::env::var_os("NO_COLOR").is_none() && std::io::stdout().is_terminal();
    let mut stdout = std::io::stdout().lock();
    for line in lines {
        let line = match coloured {
            true => paint(line),
            false => line,
        };
        let _ = writeln!(stdout, "{line}");
    }
    let _ = stdout.flush();
}
