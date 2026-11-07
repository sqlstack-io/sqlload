//! What Postgres said when it refused a statement.

/// Why Postgres refused, as it said it: its message, SQLSTATE, detail and hint; the client's own error when the server sent none (a lost connection, say).
pub(crate) fn server_reason(error: &tokio_postgres::Error) -> String {
    let Some(server) = error.as_db_error() else {
        return error.to_string();
    };
    let mut reason = format!("{} (SQLSTATE {})", server.message(), server.code().code());
    if let Some(detail) = server.detail() {
        reason.push_str(&format!("; detail: {detail}"));
    }
    if let Some(hint) = server.hint() {
        reason.push_str(&format!("; hint: {hint}"));
    }
    reason
}
