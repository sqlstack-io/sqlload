use crate::session_relay::_helpers::startup::admitted_startup;
use crate::session_relay::constants::PROTOCOL_VERSION;
use crate::session_relay::tests::helpers::{
    FORWARDED, SECRET, SSL_REQUEST, client_startup, contains, declaring_one_less, framed,
    startup_message, startup_params, without_final_nul,
};
use crate::session_relay::tests::test_types::AdmittedStartupTestCase;

const REFUSED_NAME: &str = "not this relay's application name";
const NOT_ONE_NAME: &str = "not one application name";
const NOT_A_STARTUP: &str = "not one protocol 3.0 startup message";

#[test]
fn given_startup_messages_when_the_relay_checks_them_then_only_its_secret_is_admitted_and_never_forwarded()
 {
    let wrong_last = format!("{}0", &SECRET[..SECRET.len() - 1]);
    let longer = format!("{SECRET}0");
    let test_cases = [
        AdmittedStartupTestCase {
            description: "the secret, among the other parameters",
            message: client_startup(SECRET),
            expected_forwarded: Ok(FORWARDED),
        },
        AdmittedStartupTestCase {
            description: "the secret first, before an empty value",
            message: startup_message(
                PROTOCOL_VERSION,
                &[("application_name", SECRET), ("options", ""), ("user", "u")],
            ),
            expected_forwarded: Ok("application_name=sqlload,options=,user=u"),
        },
        AdmittedStartupTestCase {
            description: "a secret wrong in its last character",
            message: client_startup(&wrong_last),
            expected_forwarded: Err(REFUSED_NAME),
        },
        AdmittedStartupTestCase {
            description: "a prefix of the secret",
            message: client_startup(&SECRET[..SECRET.len() - 1]),
            expected_forwarded: Err(REFUSED_NAME),
        },
        AdmittedStartupTestCase {
            description: "the secret with more after it",
            message: client_startup(&longer),
            expected_forwarded: Err(REFUSED_NAME),
        },
        AdmittedStartupTestCase {
            description: "the plain application name",
            message: client_startup("sqlload"),
            expected_forwarded: Err(REFUSED_NAME),
        },
        AdmittedStartupTestCase {
            description: "no application name",
            message: startup_message(PROTOCOL_VERSION, &[("user", "u"), ("database", "d")]),
            expected_forwarded: Err(NOT_ONE_NAME),
        },
        AdmittedStartupTestCase {
            description: "the secret, then a second application name",
            message: startup_message(
                PROTOCOL_VERSION,
                &[("application_name", SECRET), ("application_name", "other")],
            ),
            expected_forwarded: Err(NOT_ONE_NAME),
        },
        AdmittedStartupTestCase {
            description: "an SSLRequest",
            message: SSL_REQUEST.to_vec(),
            expected_forwarded: Err(NOT_A_STARTUP),
        },
        AdmittedStartupTestCase {
            description: "protocol 2.0 with the secret",
            message: startup_message([0, 2, 0, 0], &[("application_name", SECRET)]),
            expected_forwarded: Err(NOT_A_STARTUP),
        },
        AdmittedStartupTestCase {
            description: "a length one short of the message",
            message: declaring_one_less(client_startup(SECRET)),
            expected_forwarded: Err(NOT_A_STARTUP),
        },
        AdmittedStartupTestCase {
            description: "no final NUL",
            message: without_final_nul(client_startup(SECRET)),
            expected_forwarded: Err("not NUL-terminated"),
        },
        AdmittedStartupTestCase {
            description: "a name without a value",
            message: framed(PROTOCOL_VERSION, b"application_name\0\0"),
            expected_forwarded: Err("not pairs of names and values"),
        },
        AdmittedStartupTestCase {
            description: "nothing",
            message: Vec::new(),
            expected_forwarded: Err("no length"),
        },
    ];
    for test_case in test_cases {
        let forwarded = admitted_startup(&test_case.message, SECRET);
        let found = forwarded.clone().map(|bytes| startup_params(&bytes));
        assert_eq!(
            found,
            test_case.expected_forwarded.map(str::to_owned),
            "{}",
            test_case.description
        );
        assert!(
            !forwarded.is_ok_and(|bytes| contains(&bytes, &SECRET["sqlload-".len()..])),
            "{}: the secret was forwarded",
            test_case.description
        );
    }
}
