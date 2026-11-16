//! What the merge loader tests share: a Postgres table with repeated keys loaded into any target.

use std::collections::BTreeMap;

use sqlload::adapters::dispatch::main::session::execute::execute;
use sqlload::adapters::dispatch::main::session::single_text::single_text;
use sqlload::adapters::dispatch::models::Destination;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::main::load_tables::load_tables;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::main::render_plan::render_plan;
use sqlload::loader::models::{LoadRun, PlanOptions};
use sqlload::project::models::ProjectConfig;
use sqlload_testkit::postgres::constants::{PASSWORD, USER};

use crate::helpers::{described_project, load_selected, load_with_options, words};
use crate::test_types::{
    DedupeRule, DedupeTargetTestCase, FilesMergeTestCase, NullKeyTargetTestCase,
    ReplaceKeyTargetTestCase, SyncDeletesTestCase,
};

/// `public.readings`: `s1` and `s2` repeat with their `seen_at` order unlike their `reading_id` order, `s2`'s last row has no `seen_at`, and two rows have no sensor (nor `seen_at`), a repeated NULL key.
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
        description: "by default the last row read of each repeated key wins, rows without a sensor included, and plan says NULLs in the key match",
        settings: "",
        expected_error: "",
        expected_plan: "merge (unique_key: sensor; NULLs in keys match)",
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
        expected_error: "the loaded rows repeat its unique_key: 3 values of (sensor) appear in more than one row",
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

/// `public.stock`, keyed by `(company, item, site)` with no constraint: one key whole, one without a site, one without an item or a site, and one whose item and site are empty, which must not match the NULLs.
const STOCK_SETUP: &str = "
CREATE TABLE public.stock (company text, item text, site text, qty int8);
INSERT INTO public.stock VALUES ('acme', 'bolt', 's1', 1), ('acme', 'bolt', NULL, 2), ('acme', NULL, NULL, 3), ('acme', '', '', 4);
";

/// The change made to `public.stock` after each load: every row's `qty` grows by 10.
const STOCK_CHANGE: &str = "UPDATE public.stock SET qty = qty + 10";

/// Merge `public.stock` from Postgres on `postgres_port` (reached by `source`) into `target` on `(company, item, site)` three times, the source's rows changed after each load: the rows `target.check_query` returns after each load, or the first load's error.
pub(crate) async fn run_null_key_loads(
    target: &NullKeyTargetTestCase,
    (source, postgres_port): (&tokio_postgres::Client, u16),
) -> Result<Vec<String>, String> {
    let text = format!(
        "[connections.app_pg]\nkind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {postgres_port}\nuser = \"{USER}\"\npassword = \"{PASSWORD}\"\ndatabase = \"app\"\n\n[connections.target]\n{}\n\n[targets.dev]\nconnection = \"target\"\nloader_schema = \"{}\"\n\n[sources.app]\nkind = \"postgres\"\nconnection = \"app_pg\"\ninclude = [\"public.stock\"]\n\n[sources.app.tables.\"public.stock\"]\nwrite_strategy = \"merge\"\nunique_key = [\"company\", \"item\", \"site\"]\n",
        target.connection, target.schema
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
        target.reset.iter().map(|sql| (*sql).to_owned()).collect(),
    )
    .await
    .map_err(|error| error.to_string())?;
    source
        .batch_execute(STOCK_SETUP)
        .await
        .map_err(|error| error.to_string())?;
    let rows = loaded_rows(&project, &destinations, (target.check_query, source)).await;
    source
        .batch_execute("DROP TABLE public.stock")
        .await
        .map_err(|error| error.to_string())?;
    rows
}

/// Load `project` into its `dev` target three times, `public.stock` changed on `source` after each: what `check_query` returns after each load, or the first load's error.
async fn loaded_rows(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    (check_query, source): (&str, &tokio_postgres::Client),
) -> Result<Vec<String>, String> {
    let destination = destinations
        .get("dev")
        .ok_or_else(|| "no dev destination".to_owned())?;
    let mut rows = Vec::new();
    for _ in 0..3 {
        load_selected(project, destinations, &[]).await?;
        rows.push(
            single_text(destination, check_query.to_owned())
                .await
                .map_err(|error| error.to_string())?
                .unwrap_or_default(),
        );
        source
            .batch_execute(STOCK_CHANGE)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(rows)
}

/// `public.estimates`: version P00 read long ago, P01's lines A, B and C, a line without a version, then P02, the newest `updated_at`.
const ESTIMATES_SETUP: &str = "
CREATE TABLE public.estimates (le_version text, line text, amount int8, updated_at timestamptz NOT NULL);
INSERT INTO public.estimates VALUES
  ('LE-FY27-P00', 'Z', 0, '2026-01-01'),
  ('LE-FY27-P01', 'A', 1, '2026-02-01'), ('LE-FY27-P01', 'B', 2, '2026-02-01'), ('LE-FY27-P01', 'C', 3, '2026-02-01'),
  (NULL, 'N', 7, '2026-02-15'),
  ('LE-FY27-P02', 'X', 10, '2026-03-01');
";

/// P01 sent again after the first load, without line C and with new amounts, and the line without a version replaced by another.
const ESTIMATES_CHANGE: &str = "
DELETE FROM public.estimates WHERE line IN ('C', 'N');
UPDATE public.estimates SET amount = amount * 100, updated_at = '2026-04-01' WHERE le_version = 'LE-FY27-P01';
INSERT INTO public.estimates VALUES (NULL, 'M', 9, '2026-04-01');
";

/// A project loading `public.estimates` from Postgres on `postgres_port` into the target `connection` (schema `schema`) by `delete_insert` with `replace_key = ["le_version"]` and `settings` added.
pub(crate) fn estimates_project(
    (connection, schema): (&str, &str),
    postgres_port: u16,
    settings: &str,
) -> String {
    format!(
        "[connections.app_pg]\nkind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {postgres_port}\nuser = \"{USER}\"\npassword = \"{PASSWORD}\"\ndatabase = \"app\"\n\n[connections.target]\n{connection}\n\n[targets.dev]\nconnection = \"target\"\nloader_schema = \"{schema}\"\n\n[sources.app]\nkind = \"postgres\"\nconnection = \"app_pg\"\ninclude = [\"public.estimates\"]\n\n[sources.app.tables.\"public.estimates\"]\nwrite_strategy = \"delete_insert\"\nreplace_key = [\"le_version\"]\n{settings}\n"
    )
}

/// Load `public.estimates` from Postgres on `postgres_port` (reached by `source`) into `target` by `delete_insert` with `replace_key = ["le_version"]` and no cursor, so each load reads the whole table, then again after [`ESTIMATES_CHANGE`]: what `target.check_query` returns after each load, or the first error; each load's plan must show `target.expected_plan`.
pub(crate) async fn run_replace_key_loads(
    target: &ReplaceKeyTargetTestCase,
    (source, postgres_port): (&tokio_postgres::Client, u16),
) -> Result<Vec<String>, String> {
    let text = estimates_project((&target.connection, target.schema), postgres_port, "");
    let project = described_project(&text).await?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let destination = destinations
        .get("dev")
        .ok_or_else(|| "no dev destination".to_owned())?;
    execute(
        destination,
        target.reset.iter().map(|sql| (*sql).to_owned()).collect(),
    )
    .await
    .map_err(|error| error.to_string())?;
    source
        .batch_execute(ESTIMATES_SETUP)
        .await
        .map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    for change in [ESTIMATES_CHANGE, ""] {
        let plan = load_selected(&project, &destinations, &[]).await?;
        assert!(
            words(&plan).contains(target.expected_plan),
            "{}: {plan}",
            target.description
        );
        rows.push(
            single_text(destination, target.check_query.to_owned())
                .await
                .map_err(|error| error.to_string())?
                .unwrap_or_default(),
        );
        source
            .batch_execute(change)
            .await
            .map_err(|error| error.to_string())?;
    }
    source
        .batch_execute("DROP TABLE public.estimates")
        .await
        .map_err(|error| error.to_string())?;
    Ok(rows)
}

/// Plan and load `public.estimates` from Postgres on `postgres_port` (reached by `source`) into an in-memory DuckDB with `settings` added to its `delete_insert` by `replace_key`: the load's error, empty when it loads, and how many tables named `app__estimates` the target holds afterwards.
pub(crate) async fn replace_key_outcome(
    (source, postgres_port): (&tokio_postgres::Client, u16),
    settings: &str,
) -> Result<(String, Option<String>), String> {
    let text = estimates_project(
        ("kind = \"duckdb\"\ndatabase = \":memory:\"", "raw"),
        postgres_port,
        settings,
    );
    source
        .batch_execute(ESTIMATES_SETUP)
        .await
        .map_err(|error| error.to_string())?;
    let project = described_project(&text).await?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let destination = destinations
        .get("dev")
        .ok_or_else(|| "no dev destination".to_owned())?;
    let error = load_selected(&project, &destinations, &[])
        .await
        .err()
        .unwrap_or_default();
    let tables = single_text(
        destination,
        "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = 'app__estimates'".to_owned(),
    )
    .await
    .map_err(|error| error.to_string())?;
    source
        .batch_execute("DROP TABLE public.estimates")
        .await
        .map_err(|error| error.to_string())?;
    Ok((error, tables))
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
        for key in step.removed {
            std::fs::remove_file(directory.path().join("drop").join(key))
                .map_err(|error| error.to_string())?;
        }
        for file in step.files {
            let path = directory.path().join("drop").join(file.key);
            std::fs::create_dir_all(path.parent().unwrap_or(directory.path()))
                .map_err(|error| error.to_string())?;
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
            words(&error).contains(&words(step.expected_error))
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

/// `public.items`, five items, each updated at its own time.
const ITEMS_SETUP: &str = "
DROP TABLE IF EXISTS public.items;
CREATE TABLE public.items (id int8 PRIMARY KEY, qty int8, updated_at timestamptz NOT NULL);
INSERT INTO public.items SELECT id, id * 10, TIMESTAMPTZ '2026-11-01 00:00:00+00' + id * INTERVAL '1 hour' FROM generate_series(1, 5) AS id;
";

/// The key scan steps, each the source's change, the target's (`{schema}` its schema), and whether the load runs with `--sync-deletes`: a first load; item 2 deleted at the source and scanned; a row a later load wrote (its `_sqlload_loaded_at` after the scan's read started), whose key the source never held, left alone by the next scan; item 2 back at the source, merged by a load without a scan; every item deleted at the source, more than the scan may mark, so it marks none; and three items back, so it marks the other two.
const SYNC_STEPS: [(&str, &str, bool); 6] = [
    ("", "", false),
    ("DELETE FROM public.items WHERE id = 2", "", true),
    (
        "",
        "INSERT INTO {schema}.app__items (id, qty, updated_at, _sqlload_load_id, _sqlload_loaded_at) VALUES (9, 90, TIMESTAMPTZ '2026-11-01 09:00:00+00', 'later', TIMESTAMPTZ '2100-01-01 00:00:00+00')",
        true,
    ),
    (
        "INSERT INTO public.items VALUES (2, 21, '2026-11-02 00:00:00+00')",
        "DELETE FROM {schema}.app__items WHERE id = 9",
        false,
    ),
    ("DELETE FROM public.items", "", true),
    (
        "INSERT INTO public.items VALUES (1, 11, '2026-11-03 00:00:00+00'), (2, 22, '2026-11-03 00:00:00+00'), (3, 33, '2026-11-03 00:00:00+00')",
        "",
        true,
    ),
];

/// Run [`SYNC_STEPS`] on `target`, merging `public.items` from Postgres on `postgres_port` (reached by `source`) by `updated_at` with `target.deletes`, the scan marking at most half the table: after each step, `target.check_query`'s rows, then ` | ` and the scan's note, or the load's error.
pub(crate) async fn run_sync_deletes(
    target: &SyncDeletesTestCase,
    (source, postgres_port): (&tokio_postgres::Client, u16),
) -> Result<Vec<String>, String> {
    let text = format!(
        "[connections.app_pg]\nkind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {postgres_port}\nuser = \"{USER}\"\npassword = \"{PASSWORD}\"\ndatabase = \"app\"\n\n[connections.target]\n{}\n\n[targets.dev]\nconnection = \"target\"\nloader_schema = \"{}\"\n\n[sources.app]\nkind = \"postgres\"\nconnection = \"app_pg\"\ninclude = [\"public.items\"]\n\n[sources.app.tables.\"public.items\"]\nwrite_strategy = \"merge\"\nunique_key = [\"id\"]\ncursor_column = \"updated_at\"\ndeletes = \"{}\"\nchecks = {{ max_row_drop = \"50%\" }}\n",
        target.connection, target.schema, target.deletes
    );
    let project = described_project(&text).await?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let destination = destinations
        .get("dev")
        .ok_or_else(|| "no dev destination".to_owned())?;
    let run_target = |sql: &[&str]| {
        execute(
            destination,
            sql.iter()
                .filter(|sql| !sql.is_empty())
                .map(|sql| sql.replace("{schema}", target.schema))
                .collect(),
        )
    };
    run_target(target.reset)
        .await
        .map_err(|error| error.to_string())?;
    source
        .batch_execute(ITEMS_SETUP)
        .await
        .map_err(|error| error.to_string())?;
    let mut steps = Vec::new();
    for (source_change, target_change, sync_deletes) in SYNC_STEPS {
        source
            .batch_execute(source_change)
            .await
            .map_err(|error| error.to_string())?;
        run_target(&[target_change])
            .await
            .map_err(|error| error.to_string())?;
        let options = PlanOptions {
            sync_deletes,
            ..PlanOptions::default()
        };
        let plan = plan_project(&project, &destinations, &options)
            .await
            .map_err(|error| error.to_string())?;
        let (lines, ok) = render_plan(&plan);
        ok.then_some(()).ok_or_else(|| lines.join("\n"))?;
        let scan = format!(
            "then a full key scan of public.items {} the rows of keys it no longer holds (at most 50% of the table)",
            target.marks
        );
        assert_eq!(
            words(&lines.join("\n")).contains(&scan),
            sync_deletes,
            "{}: {lines:?}",
            target.description
        );
        apply_project(&plan, &destinations)
            .await
            .map_err(|error| error.to_string())?;
        let run = LoadRun {
            project: &project,
            destinations: &destinations,
            host: None,
            options: &options,
        };
        let said = load_tables(&run, &plan, None)
            .await
            .pop()
            .ok_or_else(|| "no table loaded".to_owned())?
            .loaded
            .map_or_else(|error| error, |loaded| loaded.note.unwrap_or_default());
        let rows = single_text(destination, target.check_query.to_owned())
            .await
            .map_err(|error| error.to_string())?
            .unwrap_or_default();
        steps.push(format!("{rows} | {said}"));
    }
    source
        .batch_execute("DROP TABLE public.items")
        .await
        .map_err(|error| error.to_string())?;
    Ok(steps)
}

/// The plan of `public.estimates` from Postgres on `postgres_port` (reached by `source`) into an in-memory DuckDB by `delete_insert` with `settings` added, with `--sync-deletes` when `sync_deletes`.
pub(crate) async fn sync_deletes_plan(
    (source, postgres_port): (&tokio_postgres::Client, u16),
    (settings, sync_deletes): (&str, bool),
) -> Result<String, String> {
    let text = estimates_project(
        ("kind = \"duckdb\"\ndatabase = \":memory:\"", "raw"),
        postgres_port,
        settings,
    );
    source
        .batch_execute(ESTIMATES_SETUP)
        .await
        .map_err(|error| error.to_string())?;
    let project = described_project(&text).await?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let options = PlanOptions {
        sync_deletes,
        ..PlanOptions::default()
    };
    let plan = plan_project(&project, &destinations, &options)
        .await
        .map_err(|error| error.to_string())?;
    source
        .batch_execute("DROP TABLE public.estimates")
        .await
        .map_err(|error| error.to_string())?;
    Ok(render_plan(&plan).0.join("\n"))
}

/// The key scan refusing to mark every row: more than the table's `max_row_drop` of 50%.
const REFUSED: &str = "checks.max_row_drop failed: the key scan would mark 5 of the table's 5 rows deleted, more than 50%, so it marks none. Fix: check that the source still lists every row (an empty view or a changed permission reads fewer keys), or raise checks.max_row_drop";

/// What each of [`SYNC_STEPS`] leaves under `deletes = "soft"`: item 2 marked, the row a later load wrote left alone, item 2 back, nothing marked when every key is gone, then items 4 and 5 marked.
pub(crate) fn soft_steps() -> Vec<String> {
    [
        "1:live,2:live,3:live,4:live,5:live | ",
        "1:live,2:deleted,3:live,4:live,5:live | (key scan of 4 keys: 1 row soft-deleted)",
        "1:live,2:deleted,3:live,4:live,5:live,9:live | (key scan of 4 keys: 0 rows soft-deleted)",
        "1:live,2:live,3:live,4:live,5:live | ",
        &format!("1:live,2:live,3:live,4:live,5:live | {REFUSED}"),
        "1:live,2:live,3:live,4:deleted,5:deleted | (key scan of 3 keys: 2 rows soft-deleted)",
    ]
    .map(str::to_owned)
    .to_vec()
}

/// What each of [`SYNC_STEPS`] leaves under `deletes = "hard"`: the same rows removed rather than marked.
pub(crate) fn hard_steps() -> Vec<String> {
    [
        "1,2,3,4,5 | ",
        "1,3,4,5 | (key scan of 4 keys: 1 row deleted)",
        "1,3,4,5,9 | (key scan of 4 keys: 0 rows deleted)",
        "1,2,3,4,5 | ",
        &format!("1,2,3,4,5 | {REFUSED}"),
        "1,2,3 | (key scan of 3 keys: 2 rows deleted)",
    ]
    .map(str::to_owned)
    .to_vec()
}
