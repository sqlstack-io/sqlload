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
use crate::test_types::{BatchedScenarioTestCase, GatedWindowsTestCase};

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
    let text = format!(
        "{}{}",
        test_case.project,
        duckdb_project(port, test_case.settings)
    );
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
        let (lines, ok) = render_plan(&plan, true);
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
    let (lines, ok) = render_plan(plan, true);
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

/// Load `GATED` with the test case's settings into a fresh DuckDB while `locker` holds the advisory lock the first window's rows wait on: once a read waits on it, run the case's change in Postgres, then release it. Returns `<load error, or ok> => <check query result>`.
pub(crate) async fn run_gated(
    (client, locker): (&Client, &Client),
    port: u16,
    test_case: &GatedWindowsTestCase,
) -> Result<String, String> {
    client
        .batch_execute(GATED)
        .await
        .map_err(|error| error.to_string())?;
    let text = format!(
        "microbatch_concurrency = true\n{}",
        duckdb_project(port, test_case.settings)
    );
    let project = described_project(&text).await?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let Some(duckdb @ Destination::DuckDb(_)) = destinations.get("dev") else {
        return Err("no DuckDB destination".to_owned());
    };
    let options = PlanOptions {
        end_cursor: Some("2026-11-05".to_owned()),
        ..PlanOptions::default()
    };
    let plan = plan_project(&project, &destinations, &options)
        .await
        .map_err(|error| error.to_string())?;
    locker
        .batch_execute("SELECT pg_advisory_lock(1)")
        .await
        .map_err(|error| error.to_string())?;
    let gate = async {
        let changed = locker
            .batch_execute(&format!(
                "{WAIT_FOR_A_WAITING_READ}{}",
                test_case.mid_load_change
            ))
            .await;
        let released = locker.batch_execute("SELECT pg_advisory_unlock(1)").await;
        changed.and(released).map_err(|error| error.to_string())
    };
    let (loaded, gated) = futures_util::join!(
        apply_and_load(&project, &plan, &destinations, &options),
        gate
    );
    gated?;
    let result = single_text(duckdb, test_case.check_query.to_owned())
        .await
        .map_err(|error| error.to_string())?
        .unwrap_or_default();
    Ok(format!("{} => {result}", loaded?))
}

/// One row per day from 2026-11-01 to 11-04; reading a row of 11-01 waits for the advisory lock 1.
const GATED: &str = "
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
CREATE TABLE public.events_raw (id int8, at date NOT NULL);
CREATE VIEW public.events AS SELECT id, at, CASE WHEN at = '2026-11-01' THEN length(pg_advisory_xact_lock_shared(1)::text) ELSE 0 END AS g FROM public.events_raw;
INSERT INTO public.events_raw VALUES (1, '2026-11-01'), (2, '2026-11-02'), (3, '2026-11-03'), (4, '2026-11-04');
";

/// Wait, up to 30 seconds, until a read waits on an advisory lock.
const WAIT_FOR_A_WAITING_READ: &str = "
DO $$
BEGIN
    FOR attempt IN 1..1500 LOOP
        EXIT WHEN EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted);
        PERFORM pg_sleep(0.02);
    END LOOP;
    IF NOT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted) THEN
        RAISE EXCEPTION 'no read waited on the gate';
    END IF;
END $$;
";
