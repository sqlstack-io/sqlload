/// Raise the open-file soft limit to the hard limit; each stream holds several Kafka sockets.
pub(crate) fn raise_open_file_limit() {
    match rlimit::increase_nofile_limit(u64::MAX) {
        Ok(limit) => tracing::debug!(limit, "open-file limit"),
        Err(error) => tracing::warn!(%error, "cannot raise the open-file limit"),
    }
}
