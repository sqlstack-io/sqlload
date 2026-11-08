//! A test-only stop: statements for one table fail from the Nth on, as if the process had been killed there.

use std::sync::Mutex;

/// The table whose statements stop, and how many still run before they do.
pub(crate) static STOP: Mutex<Option<(String, usize)>> = Mutex::new(None);

/// Whether `statement` is one the armed stop refuses: it names the stopped table and the statements allowed before the stop have run.
pub(crate) fn stopped(statement: &str) -> bool {
    let Ok(mut stop) = STOP.lock() else {
        return false;
    };
    match stop.as_mut() {
        Some((table, left)) if statement.contains(table.as_str()) => match left {
            0 => true,
            _ => {
                *left -= 1;
                false
            }
        },
        _ => false,
    }
}
