use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tokio::task::{Id, JoinError};

use crate::cdc::main::load_cdc::load_cdc;
use crate::loader::constants::DEFAULT_THREADS;
use crate::loader::main::_load_table::load_one;
use crate::loader::models::{LoadRun, ProjectPlan, SharedRun, TableOutcome, TablePlan};
use crate::project::models::ProjectConfig;

/// The most tables loading at once.
pub(crate) fn thread_count(run: &LoadRun<'_>) -> usize {
    run.options
        .concurrency
        .or(run.project.concurrency)
        .unwrap_or(DEFAULT_THREADS)
        .max(1)
}

/// What every table's thread needs, owned.
pub(crate) fn shared_run(run: &LoadRun<'_>) -> Arc<SharedRun> {
    Arc::new(SharedRun {
        project: run.project.clone(),
        destinations: run.destinations.clone(),
        host: run.host.clone(),
        options: run.options.clone(),
    })
}

/// Load one table, with everything it needs owned.
pub(crate) async fn load_task(shared: Arc<SharedRun>, table: TablePlan) -> TableOutcome {
    let run = LoadRun {
        project: &shared.project,
        destinations: &shared.destinations,
        host: shared.host.clone(),
        options: &shared.options,
    };
    let started = std::time::Instant::now();
    match load_one(&run, &table).await {
        Ok((name, loaded)) => TableOutcome {
            table: name,
            origin: Some(table.origin()),
            loaded: Ok(loaded),
            retryable: false,
            elapsed: started.elapsed(),
            skipped: false,
        },
        Err(error) => TableOutcome {
            table: table.outcome_name(),
            origin: Some(table.origin()),
            retryable: error.is_retryable(),
            loaded: Err(error.to_string()),
            elapsed: started.elapsed(),
            skipped: false,
        },
    }
}

/// The destination table `table` loads into, as one key: its target and its qualified name.
fn destination_key(table: &TablePlan) -> String {
    let destination = &table.spec.destination;
    format!(
        "{}\u{0}{}\u{0}{}\u{0}{}",
        table.spec.target,
        destination.database.as_deref().unwrap_or_default(),
        destination.schema,
        destination.table
    )
}

/// The plan's unblocked tables as they load: which may start next, and the outcomes so far.
pub(crate) struct TableQueue<'a> {
    project: &'a ProjectConfig,
    tables: Vec<&'a TablePlan>,
    waiting: Vec<usize>,
    /// Sources whose shared client is in use.
    busy: BTreeSet<&'a str>,
    /// Destination tables a table is loading into, so tables sharing one (tenants told apart by `constant_columns`) never change it at the same time.
    writing: BTreeSet<String>,
    started: BTreeMap<Id, usize>,
    outcomes: Vec<(usize, TableOutcome)>,
    /// `--fail-fast`: the first failure stops new tables starting.
    fail_fast: bool,
    /// The table whose failure stopped new tables starting, once `fail_fast` saw it fail.
    stopped: Option<String>,
}

impl<'a> TableQueue<'a> {
    pub(crate) fn new(project: &'a ProjectConfig, plan: &'a ProjectPlan, fail_fast: bool) -> Self {
        let tables: Vec<&TablePlan> = plan
            .tables
            .iter()
            .filter(|table| table.schema.blocked.is_none())
            .collect();
        Self {
            project,
            waiting: (0..tables.len()).collect(),
            tables,
            busy: BTreeSet::new(),
            writing: BTreeSet::new(),
            started: BTreeMap::new(),
            outcomes: Vec::new(),
            fail_fast,
            stopped: None,
        }
    }

    /// The outcomes of `plan`'s `postgres_cdc` sources, which load after the other tables, each as one unit; once `--fail-fast` stopped the run, none of them starts.
    pub(crate) async fn cdc_outcomes(
        &self,
        run: &LoadRun<'_>,
        plan: &ProjectPlan,
    ) -> Vec<TableOutcome> {
        load_cdc(run, plan, self.stopped.clone()).await
    }

    /// The next table that may start, with its place in the queue: the first waiting whose source's shared client (unless `parallel_tables`) is free, and whose destination table no other table is loading.
    pub(crate) fn next(&mut self) -> Option<(usize, &'a TablePlan)> {
        if self.stopped.is_some() {
            return None;
        }
        let tables = &self.tables;
        let busy = &self.busy;
        let writing = &self.writing;
        let position = self.waiting.iter().position(|index| {
            let table = tables[*index];
            !busy.contains(table.spec.source_name.as_str())
                && !writing.contains(&destination_key(table))
        })?;
        let index = self.waiting.remove(position);
        let table = self.tables[index];
        self.writing.insert(destination_key(table));
        let source = self.project.sources.get(&table.spec.source_name);
        if source.is_some_and(|source| source.shared_client && !source.parallel_tables) {
            self.busy.insert(table.spec.source_name.as_str());
        }
        Some((index, table))
    }

    pub(crate) fn started(&mut self, id: Id, index: usize) {
        self.started.insert(id, index);
    }

    /// Note a table's thread ending: its outcome, or a panic reported as its failure.
    pub(crate) fn finish(&mut self, joined: Result<(Id, TableOutcome), JoinError>) {
        let (id, outcome) = match joined {
            Ok((id, outcome)) => (id, Ok(outcome)),
            Err(error) => (error.id(), Err(error.to_string())),
        };
        let Some(index) = self.started.remove(&id) else {
            return;
        };
        let table = self.tables[index];
        self.busy.remove(table.spec.source_name.as_str());
        self.writing.remove(&destination_key(table));
        let outcome = outcome.unwrap_or_else(|reason| TableOutcome {
            table: table.outcome_name(),
            origin: Some(table.origin()),
            loaded: Err(reason),
            retryable: false,
            elapsed: std::time::Duration::ZERO,
            skipped: false,
        });
        if self.fail_fast && self.stopped.is_none() && outcome.loaded.is_err() {
            self.stopped = Some(outcome.table.clone());
        }
        self.outcomes.push((index, outcome));
    }

    /// The outcomes in plan order, a table `--fail-fast` never started among them as skipped.
    pub(crate) fn outcomes(mut self) -> Vec<TableOutcome> {
        if let Some(failed) = &self.stopped {
            for index in &self.waiting {
                let skipped = TableOutcome::skipped(self.tables[*index], failed);
                self.outcomes.push((*index, skipped));
            }
        }
        self.outcomes.sort_by_key(|(index, _)| *index);
        self.outcomes
            .into_iter()
            .map(|(_, outcome)| outcome)
            .collect()
    }
}
