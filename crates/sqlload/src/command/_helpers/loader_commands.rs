use std::path::Path;
use std::sync::Arc;

use std::collections::BTreeMap;

use crate::adapters::dispatch::models::Destination;
use crate::command::models::{Command, LoaderArgs, LoaderRequest, StateAction};
use crate::loader::_helpers::plan_view::failed_lines;
use crate::loader::_helpers::render::thousands;
use crate::loader::constants::NO_PYTHON_HOST;
use crate::loader::main::apply_project::apply_project;
use crate::loader::main::connect_targets::connect_targets;
use crate::loader::main::debug_project::debug_project;
use crate::loader::main::load_tables::load_tables;
use crate::loader::main::plan_json::plan_json;
use crate::loader::main::plan_project::plan_project;
use crate::loader::main::render_plan::render_plan;
use crate::loader::main::render_tally::render_tally;
use crate::loader::main::state_command::state_command;
use crate::loader::models::{LoadRun, PlanOptions, StateKind, StateRequest, TallyLine};
use crate::project::main::parse_project::parse_project;
use crate::project::main::parse_python_project::parse_python_project;
use crate::project::models::ProjectConfig;
use crate::python_sources::types::PythonHost;

fn environment_variable(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent | std::env::VarError::NotUnicode(_)) => None,
    }
}

/// The plan options `args` give.
pub(crate) fn options(args: LoaderArgs) -> Result<PlanOptions, String> {
    let start = cursor_flag(
        "--start-cursor-ts",
        args.start_cursor_ts,
        args.start_cursor_int,
    )?;
    let end = cursor_flag("--end-cursor-ts", args.end_cursor_ts, args.end_cursor_int)?;
    Ok(PlanOptions {
        select: [args.positional_select, args.select].concat(),
        exclude: args.exclude,
        rebuild_allowed: args.allow_type_change_rebuild,
        reload: args.reload,
        start_cursor: args.start_cursor.or(start),
        end_cursor: args.end_cursor.or(end),
        concurrency: args.concurrency.map(usize::from),
        batch_bytes: args
            .batch_bytes
            .map(|bytes| usize::try_from(bytes).unwrap_or(usize::MAX)),
    })
}

/// A cursor flag's value as text, checking that a timestamp parses.
fn cursor_flag(
    flag: &str,
    timestamp: Option<String>,
    integer: Option<i64>,
) -> Result<Option<String>, String> {
    match (timestamp, integer) {
        (Some(text), _) => match timestamp_problem(&text) {
            None => Ok(Some(text)),
            Some(problem) => Err(format!(
                "{flag} {text:?} is not an RFC 3339 timestamp or a date (YYYY-MM-DD): {problem}"
            )),
        },
        (None, integer) => Ok(integer.map(|value| value.to_string())),
    }
}

/// Why `text` is not a timestamp or date, if it is not.
fn timestamp_problem(text: &str) -> Option<String> {
    let with_zone = chrono::DateTime::parse_from_rfc3339(text);
    let without_zone = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f");
    let date = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d");
    match (with_zone, without_zone, date) {
        (Err(error), Err(_), Err(_)) => Some(error.to_string()),
        _ => None,
    }
}

/// The project in `request.text`, with its Python sources discovered through the host when it has `python_paths`.
async fn load_project_config(request: &LoaderRequest<'_>) -> Result<ProjectConfig, String> {
    let project =
        parse_project(request.text, &environment_variable).map_err(|error| error.to_string())?;
    if project.python_paths.is_empty() {
        return Ok(project);
    }
    let Some(host) = request.host.clone() else {
        return Err(NO_PYTHON_HOST.to_owned());
    };
    let root = request
        .config
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_default();
    let paths = project.python_paths.clone();
    let found = tokio::task::spawn_blocking(move || host.discover(&root, &paths))
        .await
        .map_err(|error| format!("Python discovery failed: {error}"))??;
    parse_python_project(request.text, &environment_variable, &found)
        .map_err(|error| error.to_string())
}

/// Run a loader command against the project in `request.text`: the lines to print, and whether it succeeded.
pub(crate) async fn run_loader(request: LoaderRequest<'_>) -> Result<(Vec<String>, bool), String> {
    let project = match load_project_config(&request).await {
        Ok(project) => project,
        Err(error) => return config_failure(&request.command, error),
    };
    let outcome = run_command(&project, request.command, request.host.clone()).await;
    if let Some(host) = request.host {
        let _ = tokio::task::spawn_blocking(move || host.finish()).await;
    }
    outcome
}

/// A project that does not parse: a failed plan's `Errors` for `plan`, `apply` and `load` (as JSON for `plan --json`), else the error.
fn config_failure(command: &Command, error: String) -> Result<(Vec<String>, bool), String> {
    let (Command::Plan(args) | Command::Apply(args) | Command::Load(args)) = command else {
        return Err(error);
    };
    let problems: Vec<String> = match error.strip_prefix("invalid project:\n  - ") {
        Some(listed) => listed.split("\n  - ").map(str::to_owned).collect(),
        None => vec![error.clone()],
    };
    match args.json {
        true => Ok((
            vec![
                serde_json::json!({ "error": { "code": "config", "message": error, "help": null } })
                    .to_string(),
            ],
            false,
        )),
        false => Ok((failed_lines(&problems), false)),
    }
}

/// `created  name  detail` as a tally line; a line without a detail keeps its name whole.
fn tally_line(line: &str) -> TallyLine {
    let mut parts = line.splitn(3, "  ");
    let verb = match parts.next().unwrap_or_default() {
        "created" => "created",
        "recreated" => "recreated",
        _ => "altered",
    };
    TallyLine {
        verb,
        name: parts.next().unwrap_or_default().to_owned(),
        detail: parts.next().unwrap_or_default().to_owned(),
    }
}

async fn run_command(
    project: &ProjectConfig,
    command: Command,
    host: Option<Arc<dyn PythonHost>>,
) -> Result<(Vec<String>, bool), String> {
    if let Command::Debug { no_connection } = command {
        let (lines, ok) = debug_project(project, !no_connection).await;
        return match ok {
            true => Ok((lines, true)),
            false => Err(lines.join("\n")),
        };
    }
    let destinations = connect_targets(project)
        .await
        .map_err(|error| error.to_string())?;
    if let Command::State { action } = command {
        return run_state(project, &destinations, action)
            .await
            .map(|lines| (lines, true));
    }
    let (args, applies, loads) = match command {
        Command::Plan(args) => (args, false, false),
        Command::Apply(args) => (args, true, false),
        Command::Load(args) => (args, true, true),
        _ => {
            return Err(
                "this command runs on exporter configurations; this file is a loader project"
                    .to_owned(),
            );
        }
    };
    if args.json && applies {
        return Err("--json applies to plan only".to_owned());
    }
    let json = args.json;
    let started = std::time::Instant::now();
    let options = options(args)?;
    let plan = plan_project(project, &destinations, &options)
        .await
        .map_err(|error| error.to_string())?;
    if json {
        let (text, ok) = plan_json(&plan);
        return Ok((vec![text], ok));
    }
    let (mut lines, planned) = render_plan(&plan);
    if !applies {
        return Ok((lines, planned));
    }
    let mut tally: Vec<TallyLine> = apply_project(&plan, &destinations)
        .await
        .map_err(|error| error.to_string())?
        .iter()
        .map(|line| tally_line(line))
        .collect();
    let mut rows: u64 = 0;
    if loads {
        let run = LoadRun {
            project,
            destinations: &destinations,
            host,
            options: &options,
        };
        for outcome in load_tables(&run, &plan).await {
            let seconds = format!("{:.1}s", outcome.elapsed.as_secs_f64());
            match outcome.loaded {
                Ok(loaded) => {
                    tally.extend(loaded.changes.iter().map(|line| tally_line(line)));
                    rows += loaded.rows;
                    let note = loaded
                        .note
                        .map(|note| format!(" {note}"))
                        .unwrap_or_default();
                    tally.push(TallyLine {
                        verb: "loaded",
                        name: outcome.table,
                        detail: format!("{} rows{note}  {seconds}", thousands(loaded.rows)),
                    });
                }
                Err(reason) => tally.push(TallyLine {
                    verb: "failed",
                    name: outcome.table,
                    detail: reason,
                }),
            }
        }
    }
    let (tallied, completed) = render_tally(&tally, (rows, started.elapsed()));
    lines.extend(tallied);
    Ok((lines, planned && completed))
}

/// `sqlload state`: plan the selected tables (which reads their run logs), then show, verify, reconcile or reset them.
async fn run_state(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    action: StateAction,
) -> Result<Vec<String>, String> {
    let (kind, args, to) = match action {
        StateAction::Show(args) => (StateKind::Show, args, None),
        StateAction::Verify(args) => (StateKind::Verify, args, None),
        StateAction::Reconcile(args) => (StateKind::Reconcile, args, None),
        StateAction::Reset(reset) => (StateKind::Reset, reset.tables, reset.to),
    };
    if kind == StateKind::Reset && args.select.is_empty() {
        return Err(
            "state reset changes where tables read from next; name the sources or tables to reset"
                .to_owned(),
        );
    }
    let options = PlanOptions {
        select: args.select,
        ..PlanOptions::default()
    };
    let plan = plan_project(project, destinations, &options)
        .await
        .map_err(|error| error.to_string())?;
    let request = StateRequest {
        kind,
        append: args.yes,
        to,
    };
    let (lines, ok) = state_command(&plan, destinations, &request)
        .await
        .map_err(|error| error.to_string())?;
    match ok {
        true => Ok(lines),
        false => Err(lines.join("\n")),
    }
}
