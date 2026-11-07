use std::path::Path;
use std::sync::Arc;

use std::collections::BTreeMap;

use crate::adapters::dispatch::models::Destination;
use crate::command::_helpers::output::confirm_reload;
use crate::command::_helpers::project_file::replace_file;
use crate::command::constants::RELOAD_DECLINED;
use crate::command::models::{Command, ContractAction, LoaderArgs, LoaderRequest, StateAction};
use crate::contract::main::compare_contracts::compare_contracts;
use crate::contract::main::render_contract::render_contract;
use crate::contract::main::write_contracts::write_contracts;
use crate::contract::models::{ContractReport, ContractRequest};
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
use crate::loader::models::{LoadRun, PlanOptions, ProjectPlan, StateKind, StateRequest};
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

/// `name`'s value for `${ENV:NAME}`: from `variables`, else the process environment.
fn variable(variables: &BTreeMap<String, String>, name: &str) -> Option<String> {
    variables
        .get(name)
        .cloned()
        .or_else(|| environment_variable(name))
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
    let lookup = |name: &str| variable(&request.variables, name);
    let project = parse_project(request.text, &lookup).map_err(|error| error.to_string())?;
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
    parse_python_project(request.text, &lookup, &found).map_err(|error| error.to_string())
}

/// Run a loader command against the project in `request.text`: the lines to print, and whether it succeeded. Under `plan --json` every failure is a JSON error document.
pub(crate) async fn run_loader(request: LoaderRequest<'_>) -> Result<(Vec<String>, bool), String> {
    let json = match &request.command {
        Command::Plan(args) => args.json,
        Command::Apply(args) | Command::Load(args) if args.json => {
            return Err("--json applies to plan only".to_owned());
        }
        _ => false,
    };
    let project = match load_project_config(&request).await {
        Ok(project) => project,
        Err(error) => return config_failure(&request.command, error),
    };
    let outcome = match request.command {
        Command::Contract { action } => {
            let file = (request.config, request.text);
            run_contract(&project, action, file, &request.variables).await
        }
        command => run_command(&project, command, request.host.clone(), request.no_color).await,
    };
    if let Some(host) = request.host {
        let _ = tokio::task::spawn_blocking(move || host.finish()).await;
    }
    match (outcome, json) {
        (Err(error), true) => Ok((vec![json_error("plan", &error)], false)),
        (outcome, _) => outcome,
    }
}

/// Each problem in `error`: one per line of an `invalid project` list, else the whole error.
fn problems(error: &str) -> Vec<String> {
    match error.strip_prefix("invalid project:\n  - ") {
        Some(listed) => listed.split("\n  - ").map(str::to_owned).collect(),
        None => vec![error.to_owned()],
    }
}

/// `error` in sqlbuild's error shape, `{"error": {"code", "message", "help"}}`: each problem's reason in `message` and its fix (the text after `Fix:`) in `help`, `null` when none has one.
pub(crate) fn json_error(code: &str, error: &str) -> String {
    let mut messages: Vec<String> = Vec::new();
    let mut helps: Vec<String> = Vec::new();
    for problem in problems(error) {
        match problem.split_once(" Fix: ") {
            Some((reason, fix)) => {
                messages.push(reason.trim_end_matches('.').to_owned());
                helps.push(fix.to_owned());
            }
            None => messages.push(problem),
        }
    }
    let help = (!helps.is_empty()).then(|| helps.join("\n"));
    serde_json::json!({ "error": { "code": code, "message": messages.join("\n"), "help": help } })
        .to_string()
}

/// A project that does not parse: a failed plan's `Errors` for `plan`, `apply` and `load` (as JSON for `plan --json`), else the error.
fn config_failure(command: &Command, error: String) -> Result<(Vec<String>, bool), String> {
    let (Command::Plan(args) | Command::Apply(args) | Command::Load(args)) = command else {
        return Err(error);
    };
    match args.json {
        true => Ok((vec![json_error("config", &error)], false)),
        false => Ok(render_plan(&ProjectPlan {
            problems: problems(&error),
            ..ProjectPlan::default()
        })),
    }
}

async fn run_command(
    project: &ProjectConfig,
    command: Command,
    host: Option<Arc<dyn PythonHost>>,
    no_color: bool,
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
    if let Command::LoggedState { action } = command {
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
    let (json, yes) = (args.json, args.yes);
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
    let rebuilt: Vec<&str> = plan
        .tables
        .iter()
        .filter(|table| loads && options.reload && !yes && table.schema.blocked.is_none())
        .map(|table| table.spec.destination.table.as_str())
        .collect();
    if !rebuilt.is_empty() {
        match confirm_reload(&lines, &rebuilt, no_color)? {
            true => lines.clear(),
            false => return Ok((vec![RELOAD_DECLINED.to_owned()], false)),
        }
    }
    let applied = apply_project(&plan, &destinations)
        .await
        .map_err(|error| error.to_string())?;
    let outcomes = match loads {
        true => {
            let run = LoadRun {
                project,
                destinations: &destinations,
                host,
                options: &options,
            };
            load_tables(&run, &plan).await
        }
        false => Vec::new(),
    };
    let (tallied, completed) = render_tally(&applied, &outcomes, started.elapsed());
    lines.extend(tallied);
    Ok((lines, planned && completed))
}

/// `sqlload logged-state`: plan the selected tables (which reads their run logs), then show, verify, reconcile or reset them.
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
            "logged-state reset changes where tables read from next; name the sources or tables to reset"
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

/// `sqlload contract diff` and `generate`: the report, and whether every selected table agrees; `generate --write` first writes the adoptions into the project `file` (its path and text).
async fn run_contract(
    project: &ProjectConfig,
    action: ContractAction,
    file: (&Path, &str),
    variables: &BTreeMap<String, String>,
) -> Result<(Vec<String>, bool), String> {
    let destinations = connect_targets(project)
        .await
        .map_err(|error| error.to_string())?;
    let (args, generate, write) = match action {
        ContractAction::Diff(args) => (args, None, false),
        ContractAction::Generate(chosen) => (chosen.tables, Some(chosen.overwrite), chosen.write),
    };
    let options = PlanOptions {
        select: [args.positional_select, args.select].concat(),
        exclude: args.exclude,
        ..PlanOptions::default()
    };
    let request = ContractRequest {
        from: args.from,
        generate,
        all_files: args.all_files,
        source: args.source,
    };
    let report = compare_contracts(project, &destinations, &options, &request)
        .await
        .map_err(|error| error.to_string())?;
    let report = match write {
        true => write_adoptions(report, file, variables)?,
        false => report,
    };
    Ok(render_contract(&report, args.json))
}

/// `report` after writing its adoptions into the project file, which is left as it was when the edited project would no longer parse.
fn write_adoptions(
    report: ContractReport,
    (config, text): (&Path, &str),
    variables: &BTreeMap<String, String>,
) -> Result<ContractReport, String> {
    let (updated, mut report) = write_contracts(text, report)?;
    let Some(updated) = updated else {
        return Ok(report);
    };
    parse_project(&updated, &|name| variable(variables, name)).map_err(|error| {
        format!(
            "contract generate --write left {} unchanged: the edited project would not parse: {error}",
            config.display()
        )
    })?;
    replace_file(config, (text, &updated))?;
    report.written_paths.push(config.file_name().map_or_else(
        || config.display().to_string(),
        |name| name.to_string_lossy().into_owned(),
    ));
    Ok(report)
}
