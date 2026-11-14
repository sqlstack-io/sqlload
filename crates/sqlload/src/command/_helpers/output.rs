use std::io::{BufRead, IsTerminal, Write};

use crate::command::constants::{MIN_WRAP_WIDTH, PLAN_TREE_HINT, RELOAD_YES, WRAP_ROOM};
use crate::loader::constants::{BOLD, DIM, GREEN, RED, RESET, YELLOW};

/// The colour a line of `plan`, `apply` or `load` output is printed in, if any: bold headings, red errors and failures, yellow warnings, green completions. A `loaded` line has only its first word green, and an outcome line only its label (see [`paint`]).
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
pub(crate) fn paint(line: String) -> String {
    if let Some(rest) = line.strip_prefix("loaded ") {
        return format!("{GREEN}loaded{RESET} {rest}");
    }
    let Some(code) = colour(&line) else {
        return line;
    };
    if line.starts_with(['✓', '✗']) {
        let (label, counts) = line.split_once("  ").unwrap_or((&line, ""));
        let gap = match counts.is_empty() {
            true => "",
            false => "  ",
        };
        let counts = match counts.strip_suffix(PLAN_TREE_HINT) {
            Some(before) => format!("{before}{DIM}{PLAN_TREE_HINT}{RESET}"),
            None => counts.to_owned(),
        };
        return format!("{code}{BOLD}{label}{RESET}{gap}{counts}");
    }
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

/// Write `lines` to stdout; on a terminal, each wrapped to its width (see [`wrapped`]) and in colour unless `no_color` or `NO_COLOR` says otherwise.
pub(crate) fn print_lines(lines: Vec<String>, no_color: bool) {
    let terminal = std::io::stdout().is_terminal();
    let coloured = terminal && !no_color && std::env::var_os("NO_COLOR").is_none();
    let width = console::Term::stdout()
        .size_checked()
        .filter(|_| terminal)
        .map(|(_, columns)| usize::from(columns));
    let mut stdout = std::io::stdout().lock();
    for line in lines {
        let failed = line.starts_with("failed ");
        let parts = match width {
            Some(width) => wrapped(&line, width),
            None => vec![line],
        };
        for (index, part) in parts.into_iter().enumerate() {
            let part = match (coloured, index, failed) {
                (true, 0, _) => paint(part),
                (true, _, true) => format!("{RED}{part}{RESET}"),
                _ => part,
            };
            let _ = writeln!(stdout, "{part}");
        }
    }
    let _ = stdout.flush();
}

/// `line` cut at spaces into lines of at most `width` columns. Each further line starts under the field the cut falls in (the text after the last two spaces or tree branch before it that leaves `WRAP_ROOM` columns), with the tree's `│` carried down; a word longer than the room is cut where it must be.
pub(crate) fn wrapped(line: &str, width: usize) -> Vec<String> {
    let chars: Vec<char> = line.chars().collect();
    if chars.len() <= width || width < MIN_WRAP_WIDTH {
        return vec![line.to_owned()];
    }
    let indent = (1..width.saturating_sub(WRAP_ROOM))
        .rev()
        .find(|&at| {
            chars[at] != ' '
                && (chars[..at].ends_with(&[' ', ' ']) || chars[..at].ends_with(&['─', ' ']))
        })
        .unwrap_or(0);
    let prefix: String = chars[..indent]
        .iter()
        .map(|branch| match branch {
            '│' | '├' => '│',
            _ => ' ',
        })
        .collect();
    let room = width - indent;
    let mut lines: Vec<String> = Vec::new();
    let mut rest: &[char] = &chars;
    let mut first = true;
    while !rest.is_empty() {
        let (start, available) = match first {
            true => (indent, width),
            false => (0, room),
        };
        let text = rest;
        if text.len() <= available {
            lines.push(text.iter().collect::<String>());
            break;
        }
        let cut = (start + 1..=available)
            .rev()
            .find(|&at| text[at] == ' ')
            .unwrap_or(available);
        lines.push(text[..cut].iter().collect::<String>().trim_end().to_owned());
        let mut next = cut;
        while text.get(next) == Some(&' ') {
            next += 1;
        }
        rest = &text[next..];
        first = false;
    }
    lines
        .into_iter()
        .enumerate()
        .map(|(index, text)| match index {
            0 => text,
            _ => format!("{prefix}{text}"),
        })
        .collect()
}

/// Whether to go on with a `--reload` that rebuilds `tables`: on a terminal, the plan `lines` are printed and the answer read; otherwise it is refused with the fix, before anything changes.
pub(crate) fn confirm_reload(
    lines: &[String],
    flag: &str,
    tables: &[&str],
    no_color: bool,
) -> Result<bool, String> {
    let names = tables.join(", ");
    if !std::io::stdin().is_terminal() {
        return Err(format!(
            "{flag} rebuilds {names} from the whole source, replacing what they hold; confirm it in a terminal, or add --yes"
        ));
    }
    print_lines(lines.to_vec(), no_color);
    let mut stderr = std::io::stderr();
    let _ = write!(stderr, "Rebuild {names} from the whole source? [y/N] ");
    let _ = stderr.flush();
    let mut answer = String::new();
    std::io::stdin()
        .lock()
        .read_line(&mut answer)
        .map_err(|error| format!("cannot read the answer: {error}"))?;
    Ok(RELOAD_YES.contains(&answer.trim()))
}

/// The configuration file's text; `sqlload copy` runs without one, as if it were empty.
pub(crate) fn config_text(path: &std::path::Path, copies: bool) -> Result<String, String> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(text),
        Err(error) if copies && error.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(format!("cannot read {}: {error}", path.display())),
    }
}
