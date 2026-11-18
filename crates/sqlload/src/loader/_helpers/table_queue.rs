use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tokio::runtime::Handle;
use tokio::task::{Id, JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::adapters::contract::models::WriteMode;
use crate::adapters::dispatch::main::session::helper_process::helper_process;
use crate::cdc::main::follow_cdc::follow_cdc;
use crate::cdc::main::load_cdc::load_cdc;
use crate::load_events::main::emit::emit;
use crate::load_events::models::EventKind;
use crate::loader::_helpers::run_log::record_numbered;
use crate::loader::constants::DEFAULT_THREADS;
use crate::loader::errors::LoaderError;
use crate::loader::main::_load_table::load_one;
use crate::loader::models::{LoadRun, ProjectPlan, SharedRun, TableOutcome, TablePlan};
use crate::memory_budget::main::start_watchdog::start_watchdog;
use crate::memory_budget::main::watch_table::watch_table;
use crate::memory_budget::models::MemoryGuard;
use crate::peak_memory::main::with_table_peak::with_table_peak;
use crate::project::models::ProjectConfig;

/// The most tables loading at once: `--concurrency`, else the project's `concurrency`, lowered to as many as the memory budget gives a share.
pub(crate) fn thread_count(run: &LoadRun<'_>) -> usize {
    if run.options.memory_per_table {
        return 1;
    }
    let concurrency = run
        .options
        .concurrency
        .or(run.project.concurrency)
        .unwrap_or(DEFAULT_THREADS)
        .max(1);
    match run.project.memory.budget {
        Some(budget) => concurrency.min(budget.tables_at_once.max(1)),
        None => concurrency,
    }
}

/// What every table's thread needs, owned, with the load's memory `guard`.
pub(crate) fn shared_run(run: &LoadRun<'_>, guard: Option<Arc<MemoryGuard>>) -> Arc<SharedRun> {
    let mut project = run.project.clone();
    project.memory.guard = guard;
    Arc::new(SharedRun {
        project,
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
    let guard = shared.project.memory.guard.as_ref();
    let name = table.outcome_name();
    let watched = watch_table(guard, &name, load_one(&run, &table));
    let ((loaded, tripped), peak_memory_bytes) =
        with_table_peak(shared.options.memory_per_table, Box::pin(watched)).await;
    let loaded = match (loaded, tripped) {
        (Err(_), Some(tripped)) => Err(LoaderError::Invalid(tripped)),
        (loaded, _) => loaded,
    };
    match loaded {
        Ok((name, loaded)) => TableOutcome {
            table: name,
            origin: Some(table.origin()),
            loaded: Ok(loaded),
            retryable: false,
            elapsed: started.elapsed(),
            skipped: false,
            peak_memory_bytes,
        },
        Err(error) => TableOutcome {
            table: table.outcome_name(),
            origin: Some(table.origin()),
            retryable: error.is_retryable(),
            loaded: Err(error.to_string()),
            elapsed: started.elapsed(),
            skipped: false,
            peak_memory_bytes,
        },
    }
}

/// Follow the plan's one `postgres_cdc` source until `stop` is cancelled (see [`follow_cdc`]), its unblocked tables reported started at once and their outcomes at the end.
pub(crate) async fn follow_tables(
    run: &LoadRun<'_>,
    plan: &ProjectPlan,
    stop: &CancellationToken,
) -> Vec<TableOutcome> {
    for table in plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
    {
        emit(
            Some(&table.outcome_name()),
            EventKind::TableStarted {
                strategy: strategy_name(table).to_owned(),
            },
        );
    }
    record_cdc_names(run, plan).await;
    let outcomes = Box::pin(follow_cdc(run, plan.clone(), stop)).await;
    outcomes.iter().for_each(emit_outcome);
    outcomes
}

/// Before `plan`'s `postgres_cdc` sources load: the names numbering gave each unblocked table's columns, in its run log, as other tables record them once loaded; numbers are fixed at plan, and a stream has no end to record them at. A table whose names could not be written warns: its next plan refuses a clash on its columns rather than number them anew.
async fn record_cdc_names(run: &LoadRun<'_>, plan: &ProjectPlan) {
    let captured = plan
        .tables
        .iter()
        .filter(|table| table.spec.cdc.is_some() && table.schema.blocked.is_none());
    for table in captured {
        let Some(destination) = run.destinations.get(&table.spec.target) else {
            continue;
        };
        if let Err(error) = Box::pin(record_numbered(table, destination)).await {
            tracing::warn!(
                table = %table.spec.destination.table,
                "could not record the names numbering gave its columns: {error}"
            );
        }
    }
}

/// `table`'s write strategy as the load events name it.
pub(crate) fn strategy_name(table: &TablePlan) -> &'static str {
    match table.mode {
        WriteMode::Merge { .. } => "merge",
        WriteMode::DeleteInsert { .. } => "delete_insert",
        WriteMode::Append => "append",
        WriteMode::Replace => "replace",
        WriteMode::Recreate => "recreate",
        WriteMode::Changes(_) => "changes",
        WriteMode::MarkMissing(_) => "sync_deletes",
    }
}

/// Send `outcome` as the load event it ends with: done, failed, or not started.
pub(crate) fn emit_outcome(outcome: &TableOutcome) {
    let elapsed_ms = u64::try_from(outcome.elapsed.as_millis()).unwrap_or(u64::MAX);
    let kind = match &outcome.loaded {
        Ok(loaded) => EventKind::TableDone {
            rows: loaded.rows,
            elapsed_ms,
            position: loaded.note.clone(),
            changes: loaded.changes.clone(),
            transforms: loaded.transforms.clone(),
            names: loaded.names.clone(),
            warnings: loaded.warnings.clone(),
            peak_memory_bytes: outcome.peak_memory_bytes,
        },
        Err(reason) if outcome.skipped => EventKind::TableNotStarted {
            reason: reason.clone(),
        },
        Err(reason) => EventKind::TableFailed {
            reason: reason.clone(),
            retryable: outcome.retryable,
            elapsed_ms,
            peak_memory_bytes: outcome.peak_memory_bytes,
        },
    };
    emit(Some(&outcome.table), kind);
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

/// Whether `table`'s tombstone table (`deletes_from`) is among the `unfinished` tables, which must load before it.
fn tombstones_loading(table: &TablePlan, unfinished: &[&TablePlan]) -> bool {
    let from = table
        .spec
        .deletes
        .as_ref()
        .and_then(|deletes| deletes.from.as_ref());
    let Some((destination, _)) = from.and_then(|from| from.loaded.as_ref()) else {
        return false;
    };
    unfinished.iter().any(|other| {
        other.spec.target == table.spec.target && other.spec.destination == *destination
    })
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
        let captured = plan.tables.iter().filter(|table| {
            table.spec.cdc.is_some() && table.schema.blocked.is_none() && self.stopped.is_none()
        });
        for table in captured {
            emit(
                Some(&table.outcome_name()),
                EventKind::TableStarted {
                    strategy: strategy_name(table).to_owned(),
                },
            );
        }
        if self.stopped.is_none() {
            record_cdc_names(run, plan).await;
        }
        let outcomes = load_cdc(run, plan, self.stopped.clone()).await;
        outcomes.iter().for_each(emit_outcome);
        outcomes
    }

    /// The next table that may start, with its place in the queue: the first waiting whose source's shared client (unless `parallel_tables`) is free, whose destination table no other table is loading, and whose tombstone table (`deletes_from`) has loaded, so the tombstones read this run mark rows after its merge.
    pub(crate) fn next(&mut self) -> Option<(usize, &'a TablePlan)> {
        if self.stopped.is_some() {
            return None;
        }
        let tables = &self.tables;
        let busy = &self.busy;
        let writing = &self.writing;
        let unfinished: Vec<&TablePlan> = self
            .waiting
            .iter()
            .chain(self.started.values())
            .map(|index| tables[*index])
            .collect();
        let position = self.waiting.iter().position(|index| {
            let table = tables[*index];
            !busy.contains(table.spec.source_name.as_str())
                && !writing.contains(&destination_key(table))
                && !tombstones_loading(table, &unfinished)
        })?;
        let index = self.waiting.remove(position);
        let table = self.tables[index];
        emit(
            Some(&table.outcome_name()),
            EventKind::TableStarted {
                strategy: strategy_name(table).to_owned(),
            },
        );
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
            peak_memory_bytes: None,
        });
        emit_outcome(&outcome);
        if self.fail_fast && self.stopped.is_none() && outcome.loaded.is_err() {
            self.stopped = Some(outcome.table.clone());
        }
        self.outcomes.push((index, outcome));
    }

    /// Load the queue's tables, `threads` at once, none starting while `guard`, the watchdog, holds the load at 80% of its budget (unless none is loading).
    pub(crate) async fn load_all(
        &mut self,
        threads: usize,
        (shared, guard): (Arc<SharedRun>, Option<Arc<MemoryGuard>>),
    ) {
        let mut running: JoinSet<TableOutcome> = JoinSet::new();
        loop {
            while running.len() < threads {
                if holding(guard.as_ref()) && !running.is_empty() {
                    break;
                }
                let Some((index, table)) = self.next() else {
                    break;
                };
                let (shared, table, handle) = (shared.clone(), table.clone(), Handle::current());
                let task =
                    running.spawn_blocking(move || handle.block_on(load_task(shared, table)));
                self.started(task.id(), index);
            }
            let joined = tokio::select! {
                joined = running.join_next_with_id() => joined,
                () = released(guard.as_ref()) => continue,
                () = run_stopped(guard.as_ref()) => {
                    self.stop_run(guard.as_ref());
                    return;
                }
            };
            let Some(joined) = joined else {
                break;
            };
            self.finish(joined);
        }
    }

    /// The watchdog stopped the run: each running table fails with its reason, left to end with the process, and none of the waiting ones starts.
    fn stop_run(&mut self, guard: Option<&Arc<MemoryGuard>>) {
        let reason = (guard.and_then(|guard| {
            guard
                .run_stop
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }))
        .unwrap_or_default();
        for index in std::mem::take(&mut self.started).into_values() {
            let outcome = TableOutcome {
                table: self.tables[index].outcome_name(),
                origin: Some(self.tables[index].origin()),
                loaded: Err(reason.clone()),
                retryable: false,
                elapsed: std::time::Duration::ZERO,
                skipped: false,
                peak_memory_bytes: None,
            };
            emit_outcome(&outcome);
            self.outcomes.push((index, outcome));
        }
        for index in std::mem::take(&mut self.waiting) {
            let outcome = TableOutcome::run_stopped(self.tables[index]);
            emit_outcome(&outcome);
            self.outcomes.push((index, outcome));
        }
    }

    /// The outcomes in plan order, a table `--fail-fast` never started among them as skipped.
    pub(crate) fn outcomes(mut self) -> Vec<TableOutcome> {
        if let Some(failed) = &self.stopped {
            for index in &self.waiting {
                let skipped = TableOutcome::skipped(self.tables[*index], failed);
                emit_outcome(&skipped);
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

/// The watchdog over the load's memory budget, counting the MotherDuck bridges' memory with the process's; `None` without a budget.
pub(crate) fn watch_load(run: &LoadRun<'_>) -> Option<Arc<MemoryGuard>> {
    let guard = run.project.memory.budget.map(start_watchdog)?;
    let helpers = run.destinations.values().filter_map(helper_process);
    guard
        .children
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .extend(helpers);
    Some(guard)
}

/// Whether the watchdog holds the load at 80% of its budget, so no table starts.
fn holding(guard: Option<&Arc<MemoryGuard>>) -> bool {
    guard.is_some_and(|guard| guard.holding.load(std::sync::atomic::Ordering::Relaxed))
}

/// Wait until the watchdog stops the whole run; never, without one.
async fn run_stopped(guard: Option<&Arc<MemoryGuard>>) {
    match guard {
        Some(guard) => guard.run_stopped.cancelled().await,
        None => std::future::pending().await,
    }
}

/// Wait until the watchdog stops holding the load; never, without one.
async fn released(guard: Option<&Arc<MemoryGuard>>) {
    match guard {
        Some(guard) => guard.released.notified().await,
        None => std::future::pending().await,
    }
}
