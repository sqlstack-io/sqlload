use crate::ssh_tunnel::_helpers::session::agent_pipe;
use crate::ssh_tunnel::tests::test_types::AgentPipeTestCase;

#[test]
fn given_ssh_auth_sock_when_finding_the_windows_agent_then_its_pipe_or_the_openssh_service_is_used()
{
    let test_cases = [
        AgentPipeTestCase {
            description: "SSH_AUTH_SOCK unset",
            auth_sock: None,
            expected_pipe: r"\\.\pipe\openssh-ssh-agent",
        },
        AgentPipeTestCase {
            description: "SSH_AUTH_SOCK empty",
            auth_sock: Some(""),
            expected_pipe: r"\\.\pipe\openssh-ssh-agent",
        },
        AgentPipeTestCase {
            description: "SSH_AUTH_SOCK naming another agent's pipe",
            auth_sock: Some(r"\\.\pipe\pageant.user.1a2b"),
            expected_pipe: r"\\.\pipe\pageant.user.1a2b",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            agent_pipe(test_case.auth_sock.map(std::ffi::OsString::from)),
            test_case.expected_pipe,
            "{}",
            test_case.description
        );
    }
}
