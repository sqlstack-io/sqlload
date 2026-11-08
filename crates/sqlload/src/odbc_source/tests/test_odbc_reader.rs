use crate::auth::models::Secret;
use crate::odbc_source::main::odbc_reader::odbc_reader;
use crate::odbc_source::tests::test_types::OdbcReaderTestCase;

#[test]
fn given_odbc_connection_settings_when_building_the_reader_then_dsn_and_user_are_unbraced_and_the_password_braced()
 {
    let test_cases = [
        OdbcReaderTestCase {
            description: "a connection string is used as written",
            connection_string: Some("Driver={X};Uid=u;Pwd={p;w}"),
            dsn: None,
            user: None,
            password: None,
            expected_connection_string: "Driver={X};Uid=u;Pwd={p;w}",
        },
        OdbcReaderTestCase {
            description: "a DSN alone keeps the DSN's own user and password",
            connection_string: None,
            dsn: Some("erp"),
            user: None,
            password: None,
            expected_connection_string: "DSN=erp",
        },
        OdbcReaderTestCase {
            description: "dsn and user unbraced, as driver managers read them; the password braced, a closing brace doubled",
            connection_string: None,
            dsn: Some("erp"),
            user: Some("loader"),
            password: Some("p;w}d"),
            expected_connection_string: "DSN=erp;UID=loader;PWD={p;w}}d}",
        },
    ];
    for test_case in test_cases {
        let connection_string = test_case
            .connection_string
            .map(|text| Secret(text.to_owned()));
        let password = test_case.password.map(|text| Secret(text.to_owned()));
        let built = odbc_reader(
            (connection_string.as_ref(), test_case.dsn),
            (test_case.user, password.as_ref()),
            Default::default(),
        )
        .map(|reader| reader.connection_string.expose().to_owned());
        assert_eq!(
            built,
            Ok(test_case.expected_connection_string.to_owned()),
            "{}",
            test_case.description
        );
    }
}
