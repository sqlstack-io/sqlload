//! Run a page's `sqlload` commands.

use std::io::Read;
use std::path::Path;

/// Run one `sqlload …` command with `binary` in `dir` with `env`; what it prints to stdout and stderr, in order.
pub(crate) fn run_command(
    binary: &Path,
    dir: &Path,
    env: &[(String, String)],
    command: &str,
) -> Result<Vec<String>, String> {
    let mut words = command.split_whitespace();
    if words.next() != Some("sqlload") {
        return Err(format!(
            "the page runs `{command}`, which is not sqlload or cd"
        ));
    }
    let (mut reader, writer) = std::io::pipe().map_err(|error| error.to_string())?;
    let mut process = std::process::Command::new(binary);
    process
        .args(words)
        .current_dir(dir)
        .envs(env.iter().map(|(name, value)| (name, value)))
        .stdout(writer.try_clone().map_err(|error| error.to_string())?)
        .stderr(writer);
    let mut child = process.spawn().map_err(|error| error.to_string())?;
    drop(process);
    let mut text = String::new();
    reader
        .read_to_string(&mut text)
        .map_err(|error| error.to_string())?;
    let status = child.wait().map_err(|error| error.to_string())?;
    if !status.success() {
        return Err(format!("`{command}` failed ({status}):\n{text}"));
    }
    let mut lines: Vec<String> = text
        .lines()
        .map(|line| line.trim_end().to_owned())
        .collect();
    while lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    Ok(lines)
}
