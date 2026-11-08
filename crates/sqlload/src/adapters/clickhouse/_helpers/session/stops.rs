//! A test-only stop: statements for one table fail from the Nth on, as if the process had been killed there.

use std::sync::Mutex;

/// The tables whose statements stop, each with how many still run before they do; one entry per table, so tests running at the same time stop only their own.
pub(crate) static STOP: Mutex<Vec<(String, usize)>> = Mutex::new(Vec::new());

/// Whether `statement` is one the armed stop refuses: it names the stopped table and the statements allowed before the stop have run.
pub(crate) fn stopped(statement: &str) -> bool {
    let Ok(mut stop) = STOP.lock() else {
        return false;
    };
    match stop
        .iter_mut()
        .find(|(table, _)| statement.contains(table.as_str()))
    {
        Some((_, left)) => match left {
            0 => true,
            _ => {
                *left -= 1;
                false
            }
        },
        _ => false,
    }
}
