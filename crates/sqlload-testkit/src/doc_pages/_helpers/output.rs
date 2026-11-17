//! Compare a page's output blocks with a run's.

use regex::Regex;

use crate::doc_pages::constants::{ELISION, VOLATILE};

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

/// `lines` without durations, memory, server versions and the spaces that align them.
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
    Ok(normalized)
}

/// A difference between the page's lines and a run's, for a failure message.
pub(crate) fn difference(what: &str, page: &[String], run: &[String]) -> String {
    format!(
        "`{what}`\n--- page\n{}\n--- run\n{}",
        page.join("\n"),
        run.join("\n")
    )
}
