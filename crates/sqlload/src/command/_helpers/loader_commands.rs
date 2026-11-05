use std::path::Path;
use std::sync::Arc;

use std::collections::BTreeMap;

use crate::adapters::dispatch::models::Destination;
use crate::command::models::{Command, LoaderArgs, LoaderRequest, StateAction};
use crate::loader::constants::NO_PYTHON_HOST;
use crate::loader::main::apply_project::apply_project;
use crate::loader::main::connect_targets::connect_targets;
use crate::loader::main::debug_project::debug_project;
use crate::loader::main::load_tables::load_tables;
use crate::loader::main::plan_project::plan_project;
use crate::loader::main::render_plan::render_plan;
use crate::loader::main::state_command::state_command;
use crate::loader::models::{LoadRun, PlanOptions, StateKind, StateRequest};
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
    Ok(PlanOptions {
        select: [args.positional_select, args.select].concat(),
        exclude: args.exclude,
        rebuild_allowed: args.allow_type_change_rebuild,
        reload: args.reload,
        start_cursor: args.start_cursor,
        end_cursor: args.end_cursor,
        concurrency: args.concurrency.map(usize::from),
        batch_bytes: args
            .batch_bytes
            .map(|bytes| usize::try_from(bytes).unwrap_or(usize::MAX)),
    })
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

/// Run a loader command against the project in `request.text`.
pub(crate) async fn run_loader(request: LoaderRequest<'_>) -> Result<Vec<String>, String> {
    let project = load_project_config(&request).await?;
    let outcome = run_command(&project, request.command, request.host.clone()).await;
    if let Some(host) = request.host {
        let _ = tokio::task::spawn_blocking(move || host.finish()).await;
    }
    outcome
}

async fn run_command(
    project: &ProjectConfig,
    command: Command,
    host: Option<Arc<dyn PythonHost>>,
) -> Result<Vec<String>, String> {
    if let Command::Debug { no_connection } = command {
        let (lines, ok) = debug_project(project, !no_connection).await;
        return match ok {
            true => Ok(lines),
            false => Err(lines.join("\n")),
        };
    }
    let destinations = connect_targets(project)
        .await
        .map_err(|error| error.to_string())?;
    if let Command::State { action } = command {
        return run_state(project, &destinations, action).await;
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
    let options = options(args)?;
    let plan = plan_project(project, &destinations, &options)
        .await
        .map_err(|error| error.to_string())?;
    let (mut lines, mut ok) = render_plan(&plan);
    if !applies {
        return match ok {
            true => Ok(lines),
            false => Err(lines.join("\n")),
        };
    }
    lines.extend(
        apply_project(&plan, &destinations)
            .await
            .map_err(|error| error.to_string())?,
    );
    if loads {
        let run = LoadRun {
            project,
            destinations: &destinations,
            host,
            options: &options,
        };
        for outcome in load_tables(&run, &plan).await {
            match outcome.loaded {
                Ok(loaded) => {
                    lines.extend(loaded.changes);
                    let note = loaded
                        .note
                        .map(|note| format!(" {note}"))
                        .unwrap_or_default();
                    lines.push(format!(
                        "loaded  {}  {} rows{note}",
                        outcome.table, loaded.rows
                    ));
                }
                Err(reason) => {
                    ok = false;
                    lines.push(format!("failed  {}  {reason}", outcome.table));
                }
            }
        }
    }
    match ok {
        true => Ok(lines),
        false => Err(format!("{}\nsome tables were not loaded", lines.join("\n"))),
    }
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
