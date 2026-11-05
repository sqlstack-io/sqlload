//! Add a logged table's run log to its plan: its current state, a reset to read from, or a disagreement with the data to reconcile.

use crate::loader::_helpers::run_log::{
    committed_runs, data_disagreement, read_run_log, unreadable_problems,
};
use crate::loader::_helpers::table_plan::blocked_plan;
use crate::loader::errors::LoaderError;
use crate::loader::models::{Destination, LoggedPlan, PlanOptions, TablePlan};
use crate::run_log::main::current_run::current_run;
use crate::run_log::models::RunEvent;
use crate::adapters::contract::models::WriteMode;

/// `plan` with its run log when the table is logged: after `sqlload state reset`, the next run reads from the reset position (unless flags choose the window); otherwise a logged cursor that disagrees with the data's maximum is noted, and the data wins. A record that cannot be read blocks the table until a reset follows it.
pub(in crate::loader) async fn with_run_log(
    mut plan: TablePlan,
    destination: &Destination,
    options: &PlanOptions,
) -> Result<TablePlan, LoaderError> {
    if !plan.spec.logged || plan.schema.blocked.is_some() {
        return Ok(plan);
    }
    let (mut log, runs_table) = read_run_log(&plan, destination).await?;
    let committed = committed_runs(&plan);
    log.records.extend(committed.records);
    log.unreadable.extend(committed.unreadable);
    log.records.sort_by(|left, right| {
        (&left.recorded_at, &left.run_id).cmp(&(&right.recorded_at, &right.run_id))
    });
    log.records.dedup_by(|later, earlier| {
        (&later.run_id, later.batch) == (&earlier.run_id, earlier.batch)
    });
    let records = log.records;
    let current = current_run(&records).cloned();
    let flags = options.reload || options.start_cursor.is_some() || options.end_cursor.is_some();
    let reset = current
        .as_ref()
        .filter(|record| record.event == RunEvent::Reset && !flags);
    let disagreement = data_disagreement(&plan, current.as_ref());
    if let (Some(reset), Some(cursor), Some(window)) =
        (reset, plan.spec.cursor.clone(), plan.window.as_mut())
    {
        let from = reset.cursor_after.clone().or(cursor.start);
        window.from = from.clone();
        window.inclusive = true;
        plan.cursor_note = Some(match &from {
            Some(from) => format!(
                "{} from {from} (state reset at {})",
                cursor.column, reset.recorded_at
            ),
            None => format!(
                "{} everything (state reset at {})",
                cursor.column, reset.recorded_at
            ),
        });
        if let WriteMode::DeleteInsert {
            from: delete_from, ..
        } = &mut plan.mode
        {
            *delete_from = from;
        }
    }
    if let (Some(data), Some(record)) = (&disagreement, &current) {
        plan.schema.warnings.push(format!(
            "the run log has {} {} but the data's maximum is {data}; the load reads from the data's position and appends a reconciliation (sqlload state verify reports it)",
            record.cursor_column.as_deref().unwrap_or("cursor"),
            record.cursor_after.as_deref().unwrap_or("not set"),
        ));
    }
    let problems = unreadable_problems(&log.unreadable, &records);
    if !problems.is_empty() {
        plan.schema = blocked_plan(problems);
    }
    plan.logged = Some(LoggedPlan {
        records,
        current,
        disagreement,
        unreadable: log.unreadable,
        runs_table,
    });
    Ok(plan)
}
