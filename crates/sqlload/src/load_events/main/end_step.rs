//! End the running step before the plan.

use std::time::Instant;

use crate::load_events::_helpers::steps::{STEPS, Step, phase_line};
use crate::load_events::main::emit::emit;
use crate::load_events::models::{EventKind, PhaseTiming, Role, StepEnd};

/// End the running step as `end` says, with its phase line, timing and load event: a connection that opened goes on as its source's `listing tables`, timed afresh; one that failed, a discovery, and a quiet end leave no step running. Nothing when no step runs, as when a source's connection failed before its discovery could.
pub fn end_step(end: StepEnd) {
    let Ok(mut steps) = STEPS.lock() else {
        return;
    };
    let Some(step) = steps.current.take() else {
        return;
    };
    let elapsed = step.started.elapsed();
    let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    let seconds = elapsed.as_secs_f64();
    let continues = matches!(
        end,
        StepEnd::Connected {
            role: Role::Source,
            ok: true,
            ..
        }
    );
    let (role, phase, ok, detail, kind) = match end {
        StepEnd::Quiet => return,
        StepEnd::Connected { role, label, ok } => {
            let kind = EventKind::Connected {
                role,
                name: step.subject.clone(),
                label: label.clone(),
                ok,
                elapsed_ms,
            };
            (role, "connect", ok, label, kind)
        }
        StepEnd::Discovered { tables } => {
            let detail = match tables {
                Some(1) => "1 table".to_owned(),
                Some(count) => format!("{count} tables"),
                None => String::new(),
            };
            let kind = EventKind::Discovered {
                source: step.subject.clone(),
                tables: tables.unwrap_or_default(),
                ok: tables.is_some(),
                elapsed_ms,
            };
            (Role::Source, "discover", tables.is_some(), detail, kind)
        }
    };
    let line = phase_line((role, phase, ok), (&step.subject, &detail), seconds);
    steps.lines.push(line);
    steps.timings.push(PhaseTiming {
        role,
        name: step.subject.clone(),
        phase,
        ok,
        elapsed_ms,
    });
    if continues {
        steps.next_id += 1;
        steps.current = Some(Step {
            id: steps.next_id,
            subject: step.subject,
            doing: "listing tables".to_owned(),
            started: Instant::now(),
        });
    }
    drop(steps);
    emit(None, kind);
}
