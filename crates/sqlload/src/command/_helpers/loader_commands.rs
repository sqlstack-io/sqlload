use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::Path;
use std::sync::Arc;

use crate::adapters::dispatch::models::Destination;
use crate::auth::main::allow_sign_in::allow_sign_in;
use crate::command::_helpers::load_output::{
    EventsTarget, LiveState, LoadOutput, change_tally_line, planning, start_lines, summary_lines,
    write_events,
};
use crate::command::_helpers::output::{confirm_reload, print_lines};
use crate::command::_helpers::project_file::replace_file;
use crate::command::_helpers::signals::wait_for_signal;
use crate::command::constants::{FORCED_STOP_EXIT, RELOAD_DECLINED, RETRYABLE_EXIT};
use crate::command::errors::CommandError;
use crate::command::models::{
    Command, ContractAction, EventsFormat, LoaderArgs, LoaderRequest, Progress, StateAction,
};
use crate::contract::main::compare_contracts::compare_contracts;
use crate::contract::main::render_contract::render_contract;
use crate::contract::main::write_contracts::write_contracts;
use crate::contract::models::{ContractReport, ContractRequest};
use crate::load_events::main::close::close;
use crate::load_events::main::emit::emit;
use crate::load_events::main::listen::listen;
use crate::load_events::models::EventKind;
use crate::loader::constants::NO_PYTHON_HOST;
use crate::loader::main::apply_project::apply_project;
use crate::loader::main::connect_targets::connect_targets;
use crate::loader::main::debug_project::debug_project;
use crate::loader::main::json_report::json_report;
use crate::loader::main::load_tables::load_tables;
use crate::loader::main::plan_project::plan_project;
use crate::loader::main::render_plan::render_plan;
use crate::loader::main::render_tally::render_tally;
use crate::loader::main::state_command::state_command;
use crate::loader::models::{
    LoadRun, PlanOptions, ProjectPlan, RunReport, StateKind, StateRequest, TableOutcome, TablePlan,
};
use crate::number_text::main::thousands::thousands;
use crate::project::main::config_lines::config_lines;
use crate::project::main::parse_project::parse_project;
use crate::project::main::parse_python_project::parse_python_project;
use crate::project::models::{ProjectConfig, SourceKind};
use crate::python_sources::types::PythonHost;
use tokio_util::sync::CancellationToken;

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
        resnapshot: args.resnapshot,
        fail_fast: args.fail_fast,
        batch_bytes: args
            .batch_bytes
            .map(|bytes| usize::try_from(bytes).unwrap_or(usize::MAX)),
    })
}

/// The project in `request.text`, with where its settings are written in its file; a project `sqlload copy` wrote has neither those lines nor the variables its connections read.
async fn load_project_config(request: &LoaderRequest<'_>) -> Result<ProjectConfig, String> {
    let mut project = parsed_project(request).await?;
    if request.generated {
        project.connection_variables.clear();
        return Ok(project);
    }
    let file = request.config.file_name().map_or_else(
        || request.config.display().to_string(),
        |name| name.to_string_lossy().into_owned(),
    );
    project.config_lines = config_lines(&file, request.text);
    Ok(project)
}

/// The project in `request.text`, with its Python sources discovered through the host when it has `python_paths`.
async fn parsed_project(request: &LoaderRequest<'_>) -> Result<ProjectConfig, String> {
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

/// The exit code of a command that succeeded (`ok`), or failed only in `retryable` ways, or failed otherwise.
pub(crate) fn exit_code(ok: bool, retryable: bool) -> u8 {
    match (ok, retryable) {
        (true, _) => 0,
        (false, true) => RETRYABLE_EXIT,
        (false, false) => 1,
    }
}

/// Run a loader command against the project in `request.text`: the lines to print, and its exit code. Under `--json` every failure is a JSON error document.
pub(crate) async fn run_loader(
    request: LoaderRequest<'_>,
) -> Result<(Vec<String>, u8), CommandError> {
    let name = match &request.command {
        Command::Apply(_) => "apply",
        Command::Load(_) => "load",
        _ => "plan",
    };
    let json = match &request.command {
        Command::Plan(args) | Command::Apply(args) if args.follow => {
            let error = "--follow and --max-duration apply to load only";
            return match args.json {
                true => Ok((vec![json_error(name, error, false)], 1)),
                false => Err(error.to_owned().into()),
            };
        }
        Command::Plan(args) | Command::Apply(args) | Command::Load(args) => args.json,
        _ => false,
    };
    let contract_json = match &request.command {
        Command::Contract {
            action: ContractAction::Diff(args),
        } => args.json,
        Command::Contract {
            action: ContractAction::Generate(chosen),
        } => chosen.tables.json,
        _ => false,
    };
    allow_sign_in(!(json || contract_json));
    let project = match load_project_config(&request).await {
        Ok(project) => project,
        Err(error) => return config_failure(&request.command, error).map_err(Into::into),
    };
    let outcome = match request.command {
        Command::Contract { action } => {
            let file = (request.config, request.text);
            run_contract(&project, action, file, &request.variables)
                .await
                .map(|(lines, agrees)| (lines, exit_code(agrees, false)))
                .map_err(Into::into)
        }
        command => {
            let shown = (request.no_color, request.streams, request.log_json);
            run_command(&project, command, request.host.clone(), shown).await
        }
    };
    if let Some(host) = request.host {
        let _ = tokio::task::spawn_blocking(move || host.finish()).await;
    }
    match (outcome, json) {
        (Err(error), true) => Ok((
            vec![json_error(name, &error.message, error.retryable)],
            exit_code(false, error.retryable),
        )),
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

/// `error` in sqlbuild's error shape, `{"error": {"code", "message", "help", "retryable"}}`: each problem's reason in `message` and its fix (the text after `Fix:`) in `help`, `null` when none has one; `retryable` when running the command again unchanged may succeed.
pub(crate) fn json_error(code: &str, error: &str, retryable: bool) -> String {
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
    serde_json::json!({ "error": { "code": code, "message": messages.join("\n"), "help": help, "retryable": retryable } })
        .to_string()
}

/// A project that does not parse: a failed plan's `Errors` for `plan`, `apply` and `load` (as JSON under `--json`), else the error.
fn config_failure(command: &Command, error: String) -> Result<(Vec<String>, u8), String> {
    let (Command::Plan(args) | Command::Apply(args) | Command::Load(args)) = command else {
        return Err(error);
    };
    match args.json {
        true => Ok((vec![json_error("config", &error, false)], 1)),
        false => {
            let (lines, ok) = render_plan(&ProjectPlan {
                problems: problems(&error),
                ..ProjectPlan::default()
            });
            Ok((lines, exit_code(ok, false)))
        }
    }
}

async fn run_command(
    project: &ProjectConfig,
    command: Command,
    host: Option<Arc<dyn PythonHost>>,
    (no_color, streams, log_json): (bool, bool, bool),
) -> Result<(Vec<String>, u8), CommandError> {
    if let Command::Debug { no_connection } = command {
        let (lines, ok) = debug_project(project, !no_connection).await;
        return match ok {
            true => Ok((lines, 0)),
            false => Err(lines.join("\n").into()),
        };
    }
    let destinations = connect_targets(project).await;
    if let Command::LoggedState { action } = command {
        let destinations = destinations.map_err(|error| error.to_string())?;
        return run_state(project, &destinations, action)
            .await
            .map(|lines| (lines, 0))
            .map_err(Into::into);
    }
    let destinations = destinations?;
    let (args, applies, loads) = match command {
        Command::Plan(args) => (args, false, false),
        Command::Apply(args) => (args, true, false),
        Command::Load(args) => (args, true, true),
        _ => {
            return Err(
                "this command runs on exporter configurations; this file is a loader project"
                    .to_owned()
                    .into(),
            );
        }
    };
    let (json, yes, follow) = (
        args.json,
        args.yes,
        args.follow.then_some(args.max_duration),
    );
    let started = (chrono::Utc::now(), std::time::Instant::now());
    if !loads && (args.events.is_some() || args.events_file.is_some()) {
        return Err("--events and --events-file apply to load only"
            .to_owned()
            .into());
    }
    let events = match (args.events, &args.events_file) {
        (Some(EventsFormat::Jsonl), _) => Some(EventsTarget::Stderr),
        (None, Some(path)) => Some(EventsTarget::File(std::fs::File::create(path).map_err(
            |error| format!("cannot write --events-file {}: {error}", path.display()),
        )?)),
        (None, None) => None,
    };
    let draws = match args.progress {
        Progress::Never => false,
        Progress::Always => streams,
        Progress::Auto => streams && !json && !log_json && std::io::stderr().is_terminal(),
    };
    let options = options(args)?;
    let plan = planning(
        plan_project(project, &destinations, &options),
        project.sources.len(),
        (draws, no_color),
    )
    .await?;
    if json && !applies {
        let (text, ok, retryable) = json_report(&plan, None);
        return Ok((vec![text], exit_code(ok, retryable)));
    }
    let (mut lines, planned) = match json {
        true => (Vec::new(), true),
        false => render_plan(&plan),
    };
    let plan_retryable = plan.only_retryable_problems();
    if !applies {
        return Ok((lines, exit_code(planned, plan_retryable)));
    }
    if follow.is_some() && !render_plan(&plan).1 {
        return Ok(match json {
            true => {
                let (text, _, retryable) = json_report(&plan, None);
                (vec![text], exit_code(false, retryable))
            }
            false => {
                lines.extend([String::new(), follow_refused(project, &plan)]);
                (lines, exit_code(false, plan_retryable))
            }
        });
    }
    if let Some(problem) = follow.and_then(|_| follow_problem(&plan)) {
        return Err(problem.into());
    }
    let flag = match (options.reload, options.resnapshot) {
        (true, true) => "--reload and --resnapshot",
        (true, false) => "--reload",
        (false, _) => "--resnapshot",
    };
    let rebuilt: Vec<&str> = plan
        .tables
        .iter()
        .filter(|table| {
            loads
                && !yes
                && table.schema.blocked.is_none()
                && (options.reload || (options.resnapshot && table.spec.cdc.is_some()))
        })
        .map(|table| table.spec.destination.table.as_str())
        .collect();
    if !rebuilt.is_empty() && json {
        return Err(format!(
            "{flag} rebuilds {}, replacing what they hold, and --json cannot ask to confirm it; add --yes",
            rebuilt.join(", ")
        )
        .into());
    }
    if !rebuilt.is_empty() {
        match confirm_reload(&lines, flag, &rebuilt, no_color)? {
            true => lines.clear(),
            false => return Ok((vec![RELOAD_DECLINED.to_owned()], 1)),
        }
    }
    let applied = apply_project(&plan, &destinations).await?;
    let run = LoadRun {
        project,
        destinations: &destinations,
        host,
        options: &options,
    };
    if loads && follow.is_none() {
        let streamed = StreamedLoad {
            run: &run,
            plan: &plan,
            applied: &applied,
            output: LoadOutput {
                name_width: name_width(&plan, &applied),
                rows_width: rows_width(&plan),
                print: streams && !json,
                no_color,
                events,
                live: draws.then(|| {
                    let tables: Vec<String> = plan
                        .tables
                        .iter()
                        .filter(|table| table.schema.blocked.is_none())
                        .map(TablePlan::outcome_name)
                        .collect();
                    LiveState::new(tables, options.fail_fast)
                }),
            },
            started,
            planned: (planned, plan_retryable),
        };
        lines = start_lines(lines);
        if streamed.output.print {
            print_lines(std::mem::take(&mut lines), no_color);
        }
        let (outcomes, kept, code) = Box::pin(streamed_load(streamed)).await;
        if json {
            let run = RunReport {
                command: "load",
                applied: &applied,
                outcomes: &outcomes,
                started: started.0,
                elapsed: started.1.elapsed(),
            };
            let (text, ok, retryable) = json_report(&plan, Some(&run));
            return Ok((vec![text], exit_code(ok, retryable)));
        }
        lines.extend(kept);
        return Ok((lines, code));
    }
    let stop = follow.map(follow_stop);
    let outcomes = match loads {
        true => load_tables(&run, &plan, stop.as_ref()).await,
        false => Vec::new(),
    };
    if json {
        let run = RunReport {
            command: if loads { "load" } else { "apply" },
            applied: &applied,
            outcomes: &outcomes,
            started: started.0,
            elapsed: started.1.elapsed(),
        };
        let (text, ok, retryable) = json_report(&plan, Some(&run));
        return Ok((vec![text], exit_code(ok, retryable)));
    }
    let (tallied, completed) = render_tally(&applied, &outcomes, started.1.elapsed());
    lines.extend(tallied);
    let retryable = outcomes.iter().all(TableOutcome::passes_on_rerun);
    Ok((
        lines,
        exit_code(planned && completed, plan_retryable && retryable),
    ))
}

/// What a load streams its lines and events from: its run and plan, what `apply` changed before it, where its lines go, when it started, and whether the plan was ready and failed only in ways a rerun may get past.
struct StreamedLoad<'a> {
    run: &'a LoadRun<'a>,
    plan: &'a ProjectPlan,
    applied: &'a [String],
    output: LoadOutput,
    started: (chrono::DateTime<chrono::Utc>, std::time::Instant),
    planned: (bool, bool),
}

/// Load `load.plan`, rendering its events as they come: each table's outcome, the lines not printed yet (the `created`/`altered` lines `apply` reported, each table's line as it finished, and the summary, which a load of no table leaves to the plan's closing line), and the exit code, the same as a load without events.
async fn streamed_load(load: StreamedLoad<'_>) -> (Vec<TableOutcome>, Vec<String>, u8) {
    let receiver = listen(&uuid::Uuid::new_v4().to_string());
    let width = load.output.name_width;
    let selected = load.plan.tables.len();
    let to_load = load
        .plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
        .count();
    let mut first: Vec<String> = Vec::new();
    if to_load > 0 || !load.applied.is_empty() {
        first.push(String::new());
        first.extend(
            load.applied
                .iter()
                .map(|line| change_tally_line(line, width)),
        );
    }
    let print = load.output.print;
    let no_color = load.output.no_color;
    let mut kept: Vec<String> = Vec::new();
    match print {
        true => print_lines(first, no_color),
        false => kept.extend(first),
    }
    let writer = tokio::spawn(write_events(load.output, receiver));
    emit(None, EventKind::RunStarted { selected, to_load });
    let outcomes = load_tables(load.run, load.plan, None).await;
    let (mut loaded, mut failed, mut not_started, mut rows) = (0, 0, 0, 0);
    for outcome in &outcomes {
        match (&outcome.loaded, outcome.skipped) {
            (Ok(table), false) => (loaded, rows) = (loaded + 1, rows + table.rows),
            (Ok(table), true) => rows += table.rows,
            (Err(_), false) => failed += 1,
            (Err(_), true) => not_started += 1,
        }
    }
    let completed = failed + not_started == 0;
    let retryable = outcomes.iter().all(TableOutcome::passes_on_rerun);
    let code = exit_code(load.planned.0 && completed, load.planned.1 && retryable);
    let elapsed_ms = u64::try_from(load.started.1.elapsed().as_millis()).unwrap_or(u64::MAX);
    emit(
        None,
        EventKind::RunFinished {
            loaded,
            failed,
            not_started,
            rows,
            elapsed_ms,
            exit_code: code,
        },
    );
    close();
    let (lines, tally) = writer.await.unwrap_or_default();
    kept.extend(lines);
    if to_load > 0 {
        kept.push(String::new());
        kept.extend(summary_lines(&tally, elapsed_ms, code));
    }
    (outcomes, kept, code)
}

/// The width of the tally's name column: the widest name of a table the plan loads, or of an object `applied` changed, and two spaces.
fn name_width(plan: &ProjectPlan, applied: &[String]) -> usize {
    let tables = plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
        .map(|table| table.outcome_name().len());
    let objects = applied
        .iter()
        .map(|line| line.split_whitespace().nth(1).map_or(0, str::len));
    tables.chain(objects).max().unwrap_or(0) + 2
}

/// The width of the tally's rows column: the widest row estimate, with separators, of a table the plan loads, and at least 5; a count wider still pushes only its own line.
fn rows_width(plan: &ProjectPlan) -> usize {
    plan.tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
        .map(|table| thousands(table.spec.source_table.estimated_rows.max(0) as u64).len())
        .fold(5, usize::max)
}

/// Why `load --follow` did not start on a plan with errors: following past a source or table that did not plan would move the replication slot beyond its changes for good. Names how to leave each failing source out to follow the rest: `--exclude` for a source it does not follow, and the source's own `exclude` for the tables of a `postgres_cdc` source, which share its one slot.
fn follow_refused(project: &ProjectConfig, plan: &ProjectPlan) -> String {
    let (captured, others): (Vec<String>, Vec<String>) =
        plan.failing_sources().into_iter().partition(|name| {
            project
                .sources
                .get(name)
                .is_some_and(|source| source.kind == SourceKind::PostgresCdc)
        });
    let mut ways: Vec<String> = Vec::new();
    if !others.is_empty() {
        ways.push(format!("--exclude {}", others.join(" ")));
    }
    ways.extend(captured.iter().map(|name| {
        format!("the failing tables of {name} in exclude under [sources.{name}], as its tables share one slot")
    }));
    let leave_out = match ways.is_empty() {
        true => String::new(),
        false => format!(
            ", or leave them out to follow the rest: {}",
            ways.join("; ")
        ),
    };
    format!(
        "✗ load --follow did not start: it follows only when every selected source and table plans, since the replication slot would move past the changes of one left out; fix the errors above{leave_out}"
    )
}

/// Why `load --follow` cannot follow `plan`: it follows exactly one `postgres_cdc` source, and nothing else, since it never finishes to load other tables after; when it selects none, the plan's problems say why.
fn follow_problem(plan: &ProjectPlan) -> Option<String> {
    let others: Vec<&str> = plan
        .tables
        .iter()
        .filter(|table| table.spec.cdc.is_none())
        .map(|table| table.spec.destination.table.as_str())
        .collect();
    if !others.is_empty() {
        return Some(format!(
            "load --follow follows one postgres_cdc source and nothing else; {} {} not captured, so load them without --follow, or leave them out: --exclude {}",
            others.join(", "),
            match others.len() {
                1 => "is",
                _ => "are",
            },
            others.join(" ")
        ));
    }
    let sources: std::collections::BTreeSet<&str> = plan
        .tables
        .iter()
        .map(|table| table.spec.source_name.as_str())
        .collect();
    match sources.len() {
        0 => Some(
            std::iter::once("load --follow needs a postgres_cdc source, and selects none")
                .chain(plan.problems.iter().map(String::as_str))
                .collect::<Vec<&str>>()
                .join("; "),
        ),
        1 => None,
        _ => Some(format!(
            "load --follow follows one postgres_cdc source per process; select one of {}",
            sources.into_iter().collect::<Vec<&str>>().join(", ")
        )),
    }
}

/// What stops a follow: `max_duration` passing, or Ctrl-C/SIGTERM; a second signal exits at once with 130, confirming nothing more (a restart skips what was written).
fn follow_stop(max_duration: Option<std::time::Duration>) -> CancellationToken {
    let stop = CancellationToken::new();
    if let Some(limit) = max_duration {
        let timer = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(limit).await;
            tracing::info!(
                "--max-duration passed; stopping once the batch or copy under way is written"
            );
            timer.cancel();
        });
    }
    let signalled = stop.clone();
    tokio::spawn(async move {
        wait_for_signal().await;
        tracing::info!(
            "stopping once the batch or copy under way is written; press Ctrl-C again to stop now"
        );
        signalled.cancel();
        wait_for_signal().await;
        std::process::exit(FORCED_STOP_EXIT);
    });
    stop
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
