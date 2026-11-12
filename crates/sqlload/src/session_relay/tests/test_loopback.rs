use crate::session_relay::constants::PROTOCOL_VERSION;
use crate::session_relay::tests::helpers::{
    SSL_REQUEST, block_on, client_startup, relay_scenario, relayed, startup_message,
};
use crate::session_relay::tests::test_types::LoopbackRelayTestCase;

#[test]
fn given_other_local_connections_when_the_client_connects_then_only_it_is_relayed_and_promptly()
-> Result<(), String> {
    let wrong = format!("sqlload-{}", "0".repeat(64));
    let test_cases = [
        LoopbackRelayTestCase {
            description: "no other connection",
            probes: Vec::new(),
            expected_outcome: relayed(0),
        },
        LoopbackRelayTestCase {
            description: "a silent connection first, which must not hold the client up",
            probes: vec![Vec::new()],
            expected_outcome: relayed(1),
        },
        LoopbackRelayTestCase {
            description: "two refused connections first, after which the relay must still admit the client",
            probes: vec![vec![0; 8], vec![0; 8]],
            expected_outcome: relayed(2),
        },
        LoopbackRelayTestCase {
            description: "a wrong secret, the plain name, an oversize length, an SSLRequest and protocol 2.0 first",
            probes: vec![
                client_startup(&wrong),
                client_startup("sqlload"),
                vec![0, 1, 0, 0, 0, 3, 0, 0],
                SSL_REQUEST.to_vec(),
                startup_message([0, 2, 0, 0], &[("user", "u")]),
            ],
            expected_outcome: relayed(5),
        },
        LoopbackRelayTestCase {
            description: "a startup message with no application name first",
            probes: vec![startup_message(PROTOCOL_VERSION, &[("user", "u")])],
            expected_outcome: relayed(1),
        },
    ];
    for test_case in test_cases {
        let found = block_on(relay_scenario(&test_case.probes))
            .map_err(|error| format!("{}: {error}", test_case.description))?;
        assert_eq!(
            found, test_case.expected_outcome,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
