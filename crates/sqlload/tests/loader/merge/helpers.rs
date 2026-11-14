//! What the merge loader tests share: a Postgres table with repeated keys loaded into any target.

use std::collections::BTreeMap;

use sqlload::adapters::dispatch::main::session::execute::execute;
use sqlload::adapters::dispatch::main::session::single_text::single_text;
use sqlload::adapters::dispatch::models::Destination;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::main::render_plan::render_plan;
use sqlload::loader::models::PlanOptions;
use sqlload_testkit::postgres::constants::{PASSWORD, USER};

use crate::helpers::{described_project, load_selected, load_with_options};
use crate::test_types::{DedupeRule, DedupeTargetTestCase, FilesMergeTestCase};

/// `public.readings`: `s1` and `s2` repeat with their `seen_at` order unlike their `reading_id` order, `s2`'s last row has no `seen_at`, and two rows have no sensor.
pub(crate) const READINGS_SETUP: &str = "
CREATE TABLE public.readings (reading_id int8 PRIMARY KEY, sensor text, amount int8, seen_at timestamptz, meta jsonb);
CREATE TABLE public.visits (visit_id int8 PRIMARY KEY, sensor text);
INSERT INTO public.readings VALUES
  (1, 's1', 10, '2026-11-01 00:00:00+00'),
  (2, 's1', 30, '2026-11-03 00:00:00+00'),
  (3, 's1', 20, '2026-11-02 00:00:00+00'),
  (4, 's2', 5, '2026-11-01 00:00:00+00'),
  (5, 's2', 1, NULL),
  (6, NULL, 7, NULL),
  (7, NULL, 8, NULL),
  (8, 's3', 4, '2026-11-01 00:00:00+00');
";

/// What a merge keeps of `public.readings` by each repeated-key rule, the same on every target.
const RULES: [DedupeRule; 4] = [
    DedupeRule {
        description: "by default the last row read of each repeated key wins, and rows without a key all stay",
        settings: "",
        expected_error: "",
        expected_plan: "merge (unique_key: sensor)",
    },
    DedupeRule {
        description: "dedup_sort desc keeps the latest seen_at, a NULL seen_at losing, and plan warns that a later commit still overwrites",
        settings: "dedup_sort = { seen_at = \"desc\" }",
        expected_error: "",
        expected_plan: "dedup_sort ranks repeated keys within one commit, and this table commits about every 1 GB: a later commit still overwrites a key an earlier one wrote, whatever its seen_at value",
    },
    DedupeRule {
        description: "dedup_sort asc keeps the earliest seen_at, a NULL seen_at still losing",
        settings: "dedup_sort = { seen_at = \"asc\" }",
        expected_error: "",
        expected_plan: "repeated keys: lowest seen_at wins",
    },
    DedupeRule {
        description: "on_duplicate_keys = \"fail\" refuses the load, naming the repeated keys, and merges nothing",
        settings: "on_duplicate_keys = \"fail\"",
        expected_error: "the loaded rows repeat its unique_key: 2 value(s) of (sensor) appear in more than one row",
        expected_plan: "",
    },
];

/// Load `public.readings` from Postgres on `postgres_port` into the `target` connection (its TOML lines) under `schema` as `test_case` says, after the `reset` statements on the target: the table's rows as `check_query` renders them, once the error and plan matched.
pub(crate) async fn run_dedupe_case(
    (target, schema): (&str, &str),
    postgres_port: u16,
    (reset, check_query): (&[&str], &str),
    test_case: &DedupeRule,
) -> Result<Option<String>, String> {
    let text = format!(
        "[connections.app_pg]\nkind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {postgres_port}\nuser = \"{USER}\"\npassword = \"{PASSWORD}\"\ndatabase = \"app\"\n\n[connections.target]\n{target}\n\n[targets.dev]\nconnection = \"target\"\nloader_schema = \"{schema}\"\n\n[sources.app]\nkind = \"postgres\"\nconnection = \"app_pg\"\ninclude = [\"public.readings\"]\n\n[sources.app.tables.\"public.readings\"]\nwrite_strategy = \"merge\"\nunique_key = [\"sensor\"]\ncursor_column = \"reading_id\"\n{}\n",
        test_case.settings
    );
    let project = described_project(&text).await?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let destination = destinations
        .get("dev")
        .ok_or_else(|| "no dev destination".to_owned())?;
    execute(
        destination,
        reset.iter().map(|sql| (*sql).to_owned()).collect(),
    )
    .await
    .map_err(|error| error.to_string())?;
    let outcome = load_selected(&project, &destinations, &[]).await;
    let (plan, error) = (
        outcome.clone().unwrap_or_default(),
        outcome.err().unwrap_or_default(),
    );
    assert!(
        error.contains(test_case.expected_error)
            && error.is_empty() == test_case.expected_error.is_empty(),
        "{}: {error}",
        test_case.description
    );
    assert!(
        plan.contains(test_case.expected_plan),
        "{}: {plan}",
        test_case.description
    );
    single_text(destination, check_query.to_owned())
        .await
        .map_err(|error| error.to_string())
}

/// The rows `target.check_query` returns after each rule of `RULES`, loaded from Postgres on `postgres_port` into `target`, each from a fresh table; `""` where it returns none.
pub(crate) async fn run_dedupe_rules(
    target: &DedupeTargetTestCase,
    postgres_port: u16,
) -> Result<Vec<String>, String> {
    let mut rows = Vec::new();
    for rule in &RULES {
        let found = run_dedupe_case(
            (&target.connection, target.schema),
            postgres_port,
            (&[target.reset], target.check_query),
            rule,
        )
        .await?;
        rows.push(found.unwrap_or_default());
    }
    Ok(rows)
}

/// Run `test_case` against the target connection `target` (its TOML body), with a `loader_schema` of `schema`: CSV files written to a fresh folder with their modified times, loaded step by step; the check query's result.
pub(crate) async fn run_merge_case(
    (target, schema): (&str, &str),
    test_case: &FilesMergeTestCase,
) -> Result<Option<String>, String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let root = directory.path().display().to_string();
    let text = format!(
        "[connections.files]\nkind = \"filesystem\"\nroot = \"{root}\"\n\n[connections.target]\n{target}\n\n[targets.dev]\nconnection = \"target\"\nloader_schema = \"{schema}\"\n\n[sources.scores]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"files\"\npath = \"drop/\"\nformat = \"csv\"\n{}\n",
        test_case.settings
    );
    let project = described_project(&text).await?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let destination = destinations
        .get("dev")
        .ok_or_else(|| "no dev destination".to_owned())?;
    std::fs::create_dir_all(directory.path().join("drop")).map_err(|error| error.to_string())?;
    for (index, step) in test_case.steps.iter().enumerate() {
        execute(
            destination,
            step.before.iter().map(|sql| (*sql).to_owned()).collect(),
        )
        .await
        .map_err(|error| error.to_string())?;
        for file in step.files {
            let path = directory.path().join("drop").join(file.key);
            std::fs::write(&path, file.contents).map_err(|error| error.to_string())?;
            let modified =
                std::time::UNIX_EPOCH + std::time::Duration::from_secs(file.modified_secs);
            std::fs::File::options()
                .write(true)
                .open(&path)
                .and_then(|opened| opened.set_modified(modified))
                .map_err(|error| error.to_string())?;
        }
        let outcome = run_step(&text, &destinations, (step.loads, step.reload)).await;
        let (plan, error) = (
            outcome.clone().unwrap_or_default(),
            outcome.err().unwrap_or_default(),
        );
        assert!(
            error.contains(step.expected_error)
                && error.is_empty() == step.expected_error.is_empty(),
            "{} step {}: {error}",
            test_case.description,
            index + 1
        );
        assert!(
            plan.contains(step.expected_plan),
            "{} step {}: {plan}",
            test_case.description,
            index + 1
        );
    }
    single_text(destination, test_case.check_query.to_owned())
        .await
        .map_err(|error| error.to_string())
}

/// Plan and apply once (with `--reload` when `reload`), then load only when `loads` (otherwise as `sqlload apply`): the rendered plan, or the error text.
async fn run_step(
    project_text: &str,
    destinations: &BTreeMap<String, Destination>,
    (loads, reload): (bool, bool),
) -> Result<String, String> {
    let project = described_project(project_text).await?;
    let options = PlanOptions {
        reload,
        ..PlanOptions::default()
    };
    let mut plan = plan_project(&project, destinations, &options)
        .await
        .map_err(|error| error.to_string())?;
    let (lines, ok) = render_plan(&plan);
    ok.then_some(()).ok_or_else(|| lines.join("\n"))?;
    apply_project(&plan, destinations)
        .await
        .map_err(|error| error.to_string())?;
    plan.tables.retain(|_| loads);
    load_with_options(&project, &plan, destinations, &options).await?;
    Ok(lines.join("\n"))
}
