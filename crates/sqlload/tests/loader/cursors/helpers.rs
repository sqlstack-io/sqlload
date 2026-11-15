use std::collections::BTreeMap;

use futures_util::future::OptionFuture;
use sqlload::adapters::dispatch::main::session::single_text::single_text;
use sqlload::adapters::dispatch::models::Destination;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::main::render_plan::render_plan;
use sqlload::loader::models::{PlanOptions, ProjectPlan, TypedCursors};
use sqlload::project::models::ProjectConfig;
use tokio_postgres::Client;

use crate::helpers::{described_project, duckdb_project, load_with_options, words};
use crate::test_types::TypedCursorTestCase;

/// Run a typed cursor case on fresh source tables and a fresh DuckDB: each run's outcome, `ok` or `refused` then ` => ` and the check query's result; each run's plan must contain its expected text.
pub(crate) async fn run_typed_cursors(
    client: &Client,
    port: u16,
    test_case: &TypedCursorTestCase,
) -> Result<Vec<String>, String> {
    client
        .batch_execute("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;")
        .await
        .map_err(|error| error.to_string())?;
    client
        .batch_execute(test_case.setup)
        .await
        .map_err(|error| error.to_string())?;
    let project = described_project(&duckdb_project(port, test_case.settings)).await?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let Some(duckdb @ Destination::DuckDb(_)) = destinations.get("dev") else {
        return Err("no DuckDB destination".to_owned());
    };
    let mut outcomes: Vec<String> = Vec::new();
    for (index, run) in test_case.runs.iter().enumerate() {
        client
            .batch_execute(run.source_change)
            .await
            .map_err(|error| error.to_string())?;
        let [start_ts, end_ts, start_int, end_int] =
            run.typed.map(|value| value.map(str::to_owned));
        let options = PlanOptions {
            typed_cursors: TypedCursors {
                start_ts,
                end_ts,
                start_int,
                end_int,
            },
            select: run.select.iter().map(|name| (*name).to_owned()).collect(),
            ..PlanOptions::default()
        };
        let plan = plan_project(&project, &destinations, &options)
            .await
            .map_err(|error| error.to_string())?;
        let (lines, ok) = render_plan(&plan);
        let shown = words(&lines.join("\n"));
        let missing: Vec<&&str> = run
            .expected_plan
            .iter()
            .filter(|expected| !shown.contains(&words(expected)))
            .collect();
        assert!(
            missing.is_empty(),
            "{} run {}: plan lacks {missing:?}:\n{}",
            test_case.description,
            index + 1,
            lines.join("\n")
        );
        let outcome = OptionFuture::from(
            ok.then(|| apply_and_load(&project, &plan, &destinations, &options)),
        )
        .await
        .transpose()?
        .unwrap_or_else(|| "refused".to_owned());
        let checked = (!test_case.check_query.is_empty())
            .then(|| single_text(duckdb, test_case.check_query.to_owned()));
        let result = OptionFuture::from(checked)
            .await
            .transpose()
            .map_err(|error| error.to_string())?
            .flatten()
            .unwrap_or_default();
        outcomes.push(format!("{outcome} => {result}"));
    }
    Ok(outcomes)
}

/// Apply `plan` and load it: `ok`, or the load error.
async fn apply_and_load(
    project: &ProjectConfig,
    plan: &ProjectPlan,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
) -> Result<String, String> {
    apply_project(plan, destinations)
        .await
        .map_err(|error| error.to_string())?;
    load_with_options(project, plan, destinations, options).await?;
    Ok("ok".to_owned())
}
