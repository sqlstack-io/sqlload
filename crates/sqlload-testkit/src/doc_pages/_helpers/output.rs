//! Compare a page's output blocks with a run's.

use regex::Regex;

use crate::doc_pages::constants::{ELISION, TALLY_VERBS, VOLATILE};

/// What the page should show of `actual`: all of it, or around a `…` line the page's line counts.
pub(crate) fn shown_output(expected: &[String], actual: &[String]) -> Vec<String> {
    let Some(elided) = expected.iter().position(|line| line == ELISION) else {
        return actual.to_vec();
    };
    let tail = expected.len() - elided - 1;
    if actual.len() <= elided + tail {
        return actual.to_vec();
    }
    let mut shown = actual[..elided].to_vec();
    shown.push(ELISION.to_owned());
    shown.extend_from_slice(&actual[actual.len() - tail..]);
    shown
}

/// `lines` without durations, memory, server versions and the spaces that align them, each run of a load's tally lines in a fixed order (see [`tally_runs_sorted`]).
pub(crate) fn normalized(lines: &[String]) -> Result<Vec<String>, String> {
    let mut patterns: Vec<(Regex, &str)> = Vec::new();
    for (pattern, replacement) in VOLATILE {
        let pattern = Regex::new(pattern).map_err(|error| error.to_string())?;
        patterns.push((pattern, *replacement));
    }
    let mut normalized: Vec<String> = Vec::new();
    for line in lines {
        let mut text = line.clone();
        for (pattern, replacement) in &patterns {
            text = pattern.replace_all(&text, *replacement).into_owned();
        }
        normalized.push(text.trim().to_owned());
    }
    Ok(tally_runs_sorted(normalized))
}

/// `lines` with each run of adjacent tally lines (`loaded`, `failed`, `skipped`) sorted: `load` prints a table's line as the table finishes, so tables loading at once print in no fixed order; which tables, and what each line says, still have to match.
pub(crate) fn tally_runs_sorted(lines: Vec<String>) -> Vec<String> {
    let tally = |line: &String| TALLY_VERBS.iter().any(|verb| line.starts_with(verb));
    let mut sorted: Vec<String> = Vec::with_capacity(lines.len());
    let mut run: Vec<String> = Vec::new();
    for line in lines {
        if tally(&line) {
            run.push(line);
            continue;
        }
        run.sort();
        sorted.append(&mut run);
        sorted.push(line);
    }
    run.sort();
    sorted.append(&mut run);
    sorted
}

/// A difference between the page's lines and a run's, for a failure message.
pub(crate) fn difference(what: &str, page: &[String], run: &[String]) -> String {
    format!(
        "`{what}`\n--- page\n{}\n--- run\n{}",
        page.join("\n"),
        run.join("\n")
    )
}
