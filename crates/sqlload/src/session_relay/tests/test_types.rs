pub(crate) struct AdmittedStartupTestCase {
    pub(crate) description: &'static str,
    pub(crate) message: Vec<u8>,
    /// The parameters forwarded to the server, as `name=value` joined by `,`; why the message is refused otherwise.
    pub(crate) expected_forwarded: Result<&'static str, &'static str>,
}

pub(crate) struct LoopbackRelayTestCase {
    pub(crate) description: &'static str,
    /// What each connection opened before the client sends; empty for one that sends nothing.
    pub(crate) probes: Vec<Vec<u8>>,
    pub(crate) expected_outcome: RelayOutcome,
}

/// What a loopback relay did with the connections opened before its client, and with the client.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RelayOutcome {
    /// Bytes the server received before the client connected.
    pub(crate) upstream_bytes_before_client: usize,
    /// The startup message the server received, as `name=value` joined by `,`.
    pub(crate) forwarded: String,
    pub(crate) secret_forwarded: bool,
    /// Whether bytes then crossed between the client and the server both ways.
    pub(crate) piped: bool,
    /// How many of the earlier connections the relay closed without sending them a byte.
    pub(crate) probes_closed_unanswered: usize,
    /// Whether a connection after the client's is refused.
    pub(crate) later_connection_refused: bool,
}
