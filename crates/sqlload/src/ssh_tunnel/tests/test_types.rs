pub(crate) struct AgentPipeTestCase {
    pub(crate) description: &'static str,
    /// `SSH_AUTH_SOCK`, unset when `None`.
    pub(crate) auth_sock: Option<&'static str>,
    pub(crate) expected_pipe: &'static str,
}
