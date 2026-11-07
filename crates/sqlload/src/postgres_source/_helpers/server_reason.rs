//! What Postgres said when it refused a statement.

/// Why Postgres refused, as it said it: its message, SQLSTATE, detail and hint, on one line; the client's own error and its causes when the server sent none (a refused connection, say).
pub(crate) fn server_reason(error: &tokio_postgres::Error) -> String {
    let Some(server) = error.as_db_error() else {
        let mut reason = error.to_string();
        let mut cause = std::error::Error::source(error);
        while let Some(inner) = cause {
            reason.push_str(&format!(": {inner}"));
            cause = inner.source();
        }
        return reason.replace('\n', " ");
    };
    let mut reason = format!("{} (SQLSTATE {})", server.message(), server.code().code());
    if let Some(detail) = server.detail() {
        reason.push_str(&format!("; detail: {detail}"));
    }
    if let Some(hint) = server.hint() {
        reason.push_str(&format!("; hint: {hint}"));
    }
    reason.replace('\n', " ")
}
