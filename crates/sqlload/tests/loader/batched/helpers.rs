use std::collections::BTreeMap;

use sqlload::adapters::dispatch::main::session::execute::execute;
use sqlload::adapters::dispatch::main::session::single_text::single_text;
use sqlload::adapters::dispatch::models::Destination;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::main::render_plan::render_plan;
use sqlload::loader::models::{PlanOptions, ProjectPlan};
use sqlload::project::models::ProjectConfig;
use tokio_postgres::Client;

use crate::helpers::{described_project, duckdb_project, load_with_options};
use crate::test_types::BatchedScenarioTestCase;

/// Run a batched scenario on fresh source tables and a fresh DuckDB; returns each run's outcome as `<load error, ok, or plan: <reason>> => <check query result>`, the error cut to its first two parts (`table: what failed`), which stay the same from run to run.
pub(crate) async fn run_batched(
    client: &Client,
    port: u16,
    test_case: &BatchedScenarioTestCase,
) -> Result<Vec<String>, String> {
    client
        .batch_execute("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;")
        .await
        .map_err(|error| error.to_string())?;
    client
        .batch_execute(test_case.setup)
        .await
        .map_err(|error| error.to_string())?;
    let text = duckdb_project(port, test_case.settings);
    let project = described_project(&text).await?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let Some(duckdb @ Destination::DuckDb(_)) = destinations.get("dev") else {
        return Err("no DuckDB destination".to_owned());
    };
    let mut outcomes: Vec<String> = Vec::new();
    for run in test_case.runs {
        client
            .batch_execute(run.source_change)
            .await
            .map_err(|error| error.to_string())?;
        let hand_edits: Vec<String> = run
            .destination_change
            .split_terminator(';')
            .map(str::to_owned)
            .collect();
        execute(duckdb, hand_edits)
            .await
            .map_err(|error| error.to_string())?;
        let options = PlanOptions {
            start_cursor: run.start_cursor.map(str::to_owned),
            end_cursor: run.end_cursor.map(str::to_owned),
            reload: run.reload,
            batch_bytes: test_case.batch_bytes,
            concurrency: test_case.concurrency,
            ..PlanOptions::default()
        };
        let plan = plan_project(&project, &destinations, &options)
            .await
            .map_err(|error| error.to_string())?;
        let (lines, ok) = render_plan(&plan);
        let refused = lines.iter().filter(|_| !ok).find_map(|line| {
            line.split_once("reason  ")
                .map(|(_, reason)| format!("plan: {reason}"))
        });
        let loaded = futures_util::future::OptionFuture::from(
            refused
                .is_none()
                .then(|| apply_and_load(&project, &plan, &destinations, &options)),
        )
        .await
        .transpose()?;
        let error = refused.or(loaded).unwrap_or_default();
        let result = single_text(duckdb, run.check_query.to_owned())
            .await
            .map_err(|error| error.to_string())?
            .unwrap_or_default();
        let shown = lines
            .iter()
            .filter(|_| !run.show.is_empty())
            .filter_map(|line| line.split_once(run.show))
            .map(|(_, rest)| format!(" | {}{rest}", run.show))
            .collect::<String>();
        outcomes.push(format!("{error} => {result}{shown}"));
    }
    Ok(outcomes)
}

/// Apply `plan` and load it, failing when the plan has errors; the load error cut to its first two parts (`table: what failed`), or `ok`.
async fn apply_and_load(
    project: &ProjectConfig,
    plan: &ProjectPlan,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
) -> Result<String, String> {
    let (lines, ok) = render_plan(plan);
    ok.then_some(()).ok_or_else(|| lines.join("\n"))?;
    apply_project(plan, destinations)
        .await
        .map_err(|error| error.to_string())?;
    Ok(load_with_options(project, plan, destinations, options)
        .await
        .err()
        .map_or_else(
            || "ok".to_owned(),
            |error| error.splitn(3, ": ").take(2).collect::<Vec<_>>().join(": "),
        ))
}
