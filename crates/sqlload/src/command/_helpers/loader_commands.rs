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

fn options(args: LoaderArgs) -> PlanOptions {
    PlanOptions {
        select: args.select,
        rebuild_allowed: args.allow_type_change_rebuild,
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
    let plan = plan_project(&project, &destinations, &options(args))
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
