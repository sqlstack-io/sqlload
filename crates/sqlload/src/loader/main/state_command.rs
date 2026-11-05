//! `sqlload state`: show, verify, reconcile or reset the run log of the selected logged tables.

use crate::adapters::dispatch::main::sql_adapter::sql_adapter;
use std::collections::BTreeMap;

use crate::loader::_helpers::run_log::{append_run, reconcile_record, reset_record};
use crate::loader::errors::LoaderError;
use crate::loader::models::{Destination, ProjectPlan, StateKind, StateRequest};
use crate::run_log::main::describe_state::describe_state;
use crate::run_log::main::unreadable_line::unreadable_line;
use crate::run_log::models::RunRecord;

/// The command's lines, and whether it succeeded (`verify` fails when a log disagrees with its data); `reconcile` and `reset` append only with `request.append`.
pub async fn state_command(
    plan: &ProjectPlan,
    destinations: &BTreeMap<String, Destination>,
    request: &StateRequest,
) -> Result<(Vec<String>, bool), LoaderError> {
    let mut lines: Vec<String> = Vec::new();
    let mut ok = true;
    for table in plan.tables.iter().filter(|table| table.spec.logged) {
        let name = sql_adapter(table.spec.warehouse).render_display_name(&table.spec.destination);
        let Some(logged) = &table.logged else {
            ok = false;
            lines.push(format!(
                "{name}  blocked: plan it to see why (sqlload plan {name})"
            ));
            continue;
        };
        let destination = destinations.get(&table.spec.target).ok_or_else(|| {
            LoaderError::Invalid(format!("target {:?} is not connected", table.spec.target))
        })?;
        let event = match request.kind {
            StateKind::Show => {
                lines.push(format!(
                    "{name}  {}",
                    describe_state(logged.current.as_ref())
                ));
                lines.extend(logged.records.iter().map(history_line));
                lines.extend(logged.unreadable.iter().map(unreadable_line));
                None
            }
            StateKind::Verify if table.schema.blocked.is_some() => {
                ok = false;
                lines.push(format!(
                    "{name}  blocked: sqlload state show {name} lists why"
                ));
                None
            }
            StateKind::Verify => {
                let disagrees = logged.disagreement.is_some();
                ok &= !disagrees;
                lines.push(match (&logged.disagreement, &logged.current) {
                    (Some(data), Some(current)) => format!(
                        "{name}  disagrees: the log has {} but the data's maximum is {data}; sqlload state reconcile --yes (or the next load) adopts the data's",
                        current.cursor_after.as_deref().unwrap_or("nothing")
                    ),
                    _ => format!("{name}  ok  {}", describe_state(logged.current.as_ref())),
                });
                None
            }
            StateKind::Reconcile => reconcile_record(table),
            StateKind::Reset => {
                Some(reset_record(table, request.to.as_deref()).map_err(LoaderError::Invalid)?)
            }
        };
        let Some(record) = event else {
            if request.kind == StateKind::Reconcile {
                lines.push(format!("{name}  nothing to reconcile"));
            }
            continue;
        };
        match request.append {
            true => {
                append_run(table, destination, &record).await?;
                lines.push(format!("appended  {name}  {}", event_line(&record)));
            }
            false => lines.push(format!(
                "would append  {name}  {} (add --yes to append it)",
                event_line(&record)
            )),
        }
    }
    if lines.is_empty() {
        lines.push("no selected table has state = \"logged\"".to_owned());
    }
    Ok((lines, ok))
}

fn event_line(record: &RunRecord) -> String {
    format!(
        "{}  {} -> {}, state {}",
        record.event,
        record.cursor_before.as_deref().unwrap_or("-"),
        record.cursor_after.as_deref().unwrap_or("-"),
        record.state.as_deref().unwrap_or("-")
    )
}

fn history_line(record: &RunRecord) -> String {
    let note = record
        .note
        .as_deref()
        .map(|note| format!("  {note}"))
        .unwrap_or_default();
    format!(
        "  {}  {}  {}  {} rows{note}",
        record.recorded_at,
        record.run_id,
        event_line(record),
        record.row_count
    )
}
