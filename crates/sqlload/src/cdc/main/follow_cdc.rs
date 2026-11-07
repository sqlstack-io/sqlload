//! Follow the plan's one `postgres_cdc` source: stream its changes, writing a batch every `batch_interval`, until asked to stop, planning it again whenever a table gains a column.

use std::collections::BTreeMap;

use tokio_util::sync::CancellationToken;

use crate::cdc::_helpers::stream::{
    failed_outcomes, follow_next, follow_round, replan, source_outcomes, with_changes,
};
use crate::loader::models::{LoadRun, PlanOptions, ProjectPlan, TableOutcome};

/// Follow `plan`'s one `postgres_cdc` source until `stop` is cancelled. When a table gains a column, nothing of that batch is written: the project is planned again (without `--resnapshot` or `--reload`, which apply once), applied, and followed from the slot's confirmed position. See [`follow_next`] for when it stops. Returns each table's rows over the whole follow; the first one also carries the lines the re-plans applied.
pub async fn follow_cdc(
    run: &LoadRun<'_>,
    plan: ProjectPlan,
    stop: &CancellationToken,
) -> Vec<TableOutcome> {
    let started = std::time::Instant::now();
    let again = PlanOptions {
        resnapshot: false,
        reload: false,
        ..run.options.clone()
    };
    let (mut plan, mut options) = (plan, run.options);
    let mut applied: Vec<String> = Vec::new();
    let mut rows: BTreeMap<String, u64> = BTreeMap::new();
    let mut last_reason: Option<String> = None;
    loop {
        let round = LoadRun {
            project: run.project,
            destinations: run.destinations,
            host: run.host.clone(),
            options,
        };
        tracing::info!("following");
        let (tables, followed) = follow_round(&round, &plan, stop).await;
        let next = follow_next(
            followed,
            (
                std::mem::take(&mut rows),
                last_reason.as_deref(),
                stop.is_cancelled(),
            ),
        );
        rows = next.0;
        let reason = match next.1 {
            Ok(None) => {
                tracing::info!(rows = rows.values().sum::<u64>(), "stopped following");
                let outcomes = source_outcomes(&tables, &Ok(rows), started.elapsed());
                return with_changes(outcomes, applied);
            }
            Ok(Some(reason)) => Ok(reason),
            Err(error) => Err(error),
        };
        let replanned = match reason {
            Ok(reason) => {
                last_reason = Some(reason.clone());
                replan(run, &again, &reason).await
            }
            Err(error) => Err(error),
        };
        match replanned {
            Ok((next, lines)) => {
                applied.extend(lines);
                (plan, options) = (next, &again);
            }
            Err(error) => {
                return failed_outcomes(&tables, error, started.elapsed());
            }
        }
    }
}
