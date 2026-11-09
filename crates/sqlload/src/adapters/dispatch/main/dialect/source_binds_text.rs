//! Which warehouse sources bind their text cursor bounds.

use crate::loader::constants::ODBC_SOURCE;

/// Whether a warehouse source of `system` takes its text cursor bounds as bound parameters (`?`) rather than literals: an ODBC source, whose database's string-literal rules SQLLoad can't know (some read a backslash as an escape, some don't).
pub fn source_binds_text(system: &str) -> bool {
    system == ODBC_SOURCE
}
