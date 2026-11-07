//! Follow the plan's one `postgres_cdc` source: stream its changes, writing a batch every `batch_interval`, until asked to stop, planning it again whenever a table gains a column.

use std::collections::BTreeMap;

use tokio_util::sync::CancellationToken;

use crate::cdc::_helpers::stream::{added, follow_round, replan, source_outcomes, with_changes};
use crate::loader::models::{LoadRun, PlanOptions, ProjectPlan, TableOutcome};

/// Follow `plan`'s one `postgres_cdc` source until `stop` is cancelled. When a table gains a column, nothing of that batch is written: the project is planned again (without `--resnapshot` or `--reload`, which apply once), applied, and followed from the slot's confirmed position. A second re-plan with no batch written since the first stops it with the reason. Returns each table's rows over the whole follow; the first one also carries the lines the re-plans applied.
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
    let mut replanned_before = false;
    loop {
        let round = LoadRun {
            project: run.project,
            destinations: run.destinations,
            host: run.host.clone(),
            options,
        };
        tracing::info!("following");
        let (tables, followed) = follow_round(&round, &plan, stop).await;
        let followed = followed.map(|(written, replan)| {
            let wrote = written.values().sum::<u64>() > 0;
            rows = added(std::mem::take(&mut rows), written);
            (replan, wrote || !replanned_before)
        });
        let reason = match followed {
            Ok((Some(reason), true)) if !stop.is_cancelled() => Ok(reason),
            Ok((Some(reason), false)) => Err(format!(
                "{reason}; it was planned again just before, with no batch written since, so following stopped"
            )),
            Ok(_) => {
                tracing::info!(rows = rows.values().sum::<u64>(), "stopped following");
                let outcomes = source_outcomes(&tables, &Ok(rows), started.elapsed());
                return with_changes(outcomes, applied);
            }
            Err(error) => Err(error),
        };
        let replanned = match reason {
            Ok(reason) => replan(run, &again, &reason).await,
            Err(error) => Err(error),
        };
        match replanned {
            Ok((next, lines)) => {
                applied.extend(lines);
                (plan, options, replanned_before) = (next, &again, true);
            }
            Err(error) => {
                return source_outcomes(&tables, &Err(error), started.elapsed());
            }
        }
    }
}
