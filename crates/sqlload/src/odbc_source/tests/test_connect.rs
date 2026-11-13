use crate::auth::models::Secret;
use crate::odbc_source::_helpers::connect::connect;
use crate::odbc_source::main::odbc_reader::odbc_reader;
use crate::odbc_source::tests::helpers::elf_with_flags;
use crate::odbc_source::tests::test_types::ConnectErrorTestCase;

#[test]
fn given_a_driver_library_unixodbc_cannot_load_when_connecting_then_the_error_says_what_to_check() {
    let test_cases = [
        ConnectErrorTestCase {
            description: "a registered path with no file names the registration to correct",
            file: ("libother.so", Vec::new()),
            library: "libmissing.so",
            expected_reason: "The driver library {path} doesn't exist: correct the Driver= line that registers the driver in odbcinst.ini.",
        },
        ConnectErrorTestCase {
            description: "a file that exists but isn't a library points at what it needs",
            file: ("libtext.so", b"plain text, not a shared library".to_vec()),
            library: "libtext.so",
            expected_reason: "The driver library {path} exists, so a library it needs is missing or doesn't load: `ldd {path}` lists them.",
        },
        ConnectErrorTestCase {
            description: "a library flagged STATIC_TLS (among others) names the thread-local storage and the preload that works",
            file: ("libstatic_tls.so", elf_with_flags(0x10 | 0x8)),
            library: "libstatic_tls.so",
            expected_reason: "The driver library {path} exists, and is built for static thread-local storage (STATIC_TLS), which the dynamic loader refuses once it reaches thread-locals already in use, as libstdc++'s are in a Python process that loaded libstdc++ after it started. Start the process with LD_PRELOAD=libstdc++.so.6, or use the sqlload-cli package's sqlload.",
        },
        ConnectErrorTestCase {
            description: "dynamic flags without STATIC_TLS point at what it needs",
            file: ("libbind_now.so", elf_with_flags(0x8)),
            library: "libbind_now.so",
            expected_reason: "The driver library {path} exists, so a library it needs is missing or doesn't load: `ldd {path}` lists them.",
        },
    ];
    let folder = tempfile::tempdir().expect("temporary folder");
    for test_case in test_cases {
        let (written, bytes) = &test_case.file;
        std::fs::write(folder.path().join(written), bytes).expect("write the file");
        let path = folder.path().join(test_case.library).display().to_string();
        let connection_string = Secret(format!("Driver={path};"));
        let reader = odbc_reader(
            (Some(&connection_string), None),
            (None, None),
            Default::default(),
        )
        .expect("reader");
        let error = connect(&reader).err().map(|error| error.to_string());
        let reason = error
            .as_deref()
            .and_then(|error| error.split_once("file not found"))
            .map(|(_, reason)| reason.trim_start().to_owned());
        assert_eq!(
            reason,
            Some(test_case.expected_reason.replace("{path}", &path)),
            "{}: {error:?}",
            test_case.description
        );
    }
}
