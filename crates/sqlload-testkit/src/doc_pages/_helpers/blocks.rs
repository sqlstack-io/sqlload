//! A page's sections and fenced blocks.

use crate::doc_pages::constants::{FENCE, HEADING};
use crate::doc_pages::models::Block;

/// The fenced blocks of the section under `## heading`, up to the next `## ` heading.
pub(crate) fn section_blocks(lines: &[String], heading: &str) -> Result<Vec<Block>, String> {
    let title = format!("{HEADING}{heading}");
    let start = lines
        .iter()
        .position(|line| *line == title)
        .ok_or_else(|| format!("the page has no `{title}`"))?;
    let mut blocks: Vec<Block> = Vec::new();
    let mut index = start + 1;
    while index < lines.len() && !lines[index].starts_with(HEADING) {
        if let Some(language) = lines[index].strip_prefix(FENCE) {
            let first = index + 1;
            let end = first
                + lines[first..]
                    .iter()
                    .position(|line| line == FENCE)
                    .ok_or_else(|| format!("an unclosed block at line {}", index + 1))?;
            blocks.push(Block {
                language: language.to_owned(),
                first,
                end,
                lines: lines[first..end].to_vec(),
            });
            index = end;
        }
        index += 1;
    }
    Ok(blocks)
}

/// The SQL a `psql … -c "…"` line runs.
pub(crate) fn psql_statement(line: &str) -> Option<&str> {
    let start = line.find(" -c \"")? + 5;
    let end = line.rfind('"')?;
    (end > start).then(|| &line[start..end])
}

/// Each `$ ` command of a console block, and the lines the page shows it printing.
pub(crate) fn commands(lines: &[String]) -> Result<Vec<(String, Vec<String>)>, String> {
    let mut commands: Vec<(String, Vec<String>)> = Vec::new();
    for line in lines {
        match (line.strip_prefix("$ "), commands.last_mut()) {
            (Some(command), _) => commands.push((command.to_owned(), Vec::new())),
            (None, Some((_, output))) => output.push(line.clone()),
            (None, None) => {
                return Err(format!(
                    "a console block starts with `{line}`, not a `$ ` command, so it can't be checked"
                ));
            }
        }
    }
    match commands.is_empty() {
        true => Err("a console block holds no `$ ` command, so it can't be checked".to_owned()),
        false => Ok(commands),
    }
}
