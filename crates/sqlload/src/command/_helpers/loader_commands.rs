use crate::command::models::{Command, LoaderArgs};
use crate::loader::main::apply_project::apply_project;
use crate::loader::main::connect_targets::connect_targets;
use crate::loader::main::load_project::load_project;
use crate::loader::main::plan_project::plan_project;
use crate::loader::main::render_plan::render_plan;
use crate::loader::models::PlanOptions;
use crate::project::main::parse_project::parse_project;

fn environment_variable(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent | std::env::VarError::NotUnicode(_)) => None,
    }
}

fn options(args: LoaderArgs) -> Result<PlanOptions, String> {
    let start = cursor_flag(
        "--start-cursor-ts",
        args.start_cursor_ts,
        args.start_cursor_int,
    )?;
    let end = cursor_flag("--end-cursor-ts", args.end_cursor_ts, args.end_cursor_int)?;
    Ok(PlanOptions {
        select: args.select,
        rebuild_allowed: args.allow_type_change_rebuild,
        reload: args.reload,
        start_cursor: start,
        end_cursor: end,
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

/// Run a loader command against the project in `text`.
pub(crate) async fn run_loader(text: &str, command: Command) -> Result<Vec<String>, String> {
    let project = parse_project(text, &environment_variable).map_err(|error| error.to_string())?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
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
    let plan = plan_project(&project, &destinations, &options)
        .await
        .map_err(|error| error.to_string())?;
    let (mut lines, ok) = render_plan(&plan);
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
        let results = load_project(&project, &plan, &destinations)
            .await
            .map_err(|error| error.to_string())?;
        lines.extend(
            results
                .iter()
                .map(|result| format!("loaded  {}  {} rows", result.table, result.rows)),
        );
    }
    match ok {
        true => Ok(lines),
        false => Err(format!("{}\nsome tables were not loaded", lines.join("\n"))),
    }
}
