//! Run each kind of fenced block as a reader would.

use std::path::Path;

use crate::doc_pages::_helpers::blocks::{commands, psql_statement};
use crate::doc_pages::_helpers::commands::run_command;
use crate::doc_pages::_helpers::output::{difference, normalized, shown_output};
use crate::doc_pages::constants::{PAGE_HOST_LINE, PROJECT_FILE};
use crate::doc_pages::models::{Block, PageRun, PageServer};

/// `run` after `block`: SQL kept, shell setup run, the project written or compared, commands run.
pub(crate) fn run_block(
    run: PageRun,
    block: &Block,
    server: Option<PageServer<'_>>,
    binary: &Path,
) -> Result<PageRun, String> {
    match block.language.split_whitespace().next().unwrap_or_default() {
        "sql" => Ok(PageRun {
            seed: block.lines.join("\n"),
            ..run
        }),
        "bash" => run_shell(run, block, server),
        "toml" => project_file(run, block, server),
        "console" => run_console(run, block, binary),
        other => Err(format!(
            "the block at line {} is `{other}`, which the page run can't check; use sql, bash, toml or console",
            block.first
        )),
    }
}

/// Run `statement` on the page's server.
fn run_sql(server: Option<PageServer<'_>>, statement: &str) -> Result<(), String> {
    let server = server.ok_or("the page runs SQL, but the test started no server")?;
    server
        .runtime
        .block_on(server.postgres.client.batch_execute(statement))
        .map_err(|error| error.to_string())
}

/// `run` after a `bash` block's `export`s and the SQL its `psql` lines run; `docker run` is the test's.
fn run_shell(
    mut run: PageRun,
    block: &Block,
    server: Option<PageServer<'_>>,
) -> Result<PageRun, String> {
    for line in &block.lines {
        if let Some(assignment) = line.strip_prefix("export ")
            && let Some((name, value)) = assignment.split_once('=')
        {
            run.env.push((name.to_owned(), value.to_owned()));
        }
        if line.contains("< seed.sql") {
            run_sql(server, &run.seed)?;
        }
        if let Some(statement) = psql_statement(line) {
            run_sql(server, statement)?;
        }
    }
    Ok(run)
}

/// `run` after writing a `toml` block as the project file, or comparing it with the one a command wrote.
fn project_file(
    mut run: PageRun,
    block: &Block,
    server: Option<PageServer<'_>>,
) -> Result<PageRun, String> {
    let path = run.dir.join(PROJECT_FILE);
    if !path.exists() {
        let mut text = block.lines.join("\n") + "\n";
        if let Some(server) = server {
            let host = format!("host = \"127.0.0.1\"\nport = {}", server.postgres.port);
            text = text.replace(PAGE_HOST_LINE, &host);
        }
        std::fs::write(&path, text).map_err(|error| format!("{}: {error}", path.display()))?;
        return Ok(run);
    }
    let written =
        std::fs::read_to_string(&path).map_err(|error| format!("{}: {error}", path.display()))?;
    let shown: Vec<String> = written.lines().map(str::to_owned).collect();
    if shown != block.lines {
        run.differences
            .push(difference(PROJECT_FILE, &block.lines, &shown));
    }
    run.replacements.push((block.first, block.end, shown));
    Ok(run)
}

/// `run` after a `console` block's commands, each compared with what the page shows it printing.
fn run_console(mut run: PageRun, block: &Block, binary: &Path) -> Result<PageRun, String> {
    let mut shown_block: Vec<String> = Vec::new();
    for (command, expected) in commands(&block.lines)? {
        let actual = match command.strip_prefix("cd ") {
            Some(folder) => {
                run.dir = run.dir.join(folder);
                Vec::new()
            }
            None => run_command(binary, &run.dir, &run.env, &command)?,
        };
        let shown = shown_output(&expected, &actual);
        if normalized(&shown)? != normalized(&expected)? {
            run.differences
                .push(difference(&command, &expected, &shown));
        }
        shown_block.push(format!("$ {command}"));
        shown_block.extend(shown);
    }
    run.replacements.push((block.first, block.end, shown_block));
    Ok(run)
}
