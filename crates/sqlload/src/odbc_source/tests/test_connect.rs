use crate::auth::models::Secret;
use crate::odbc_source::_helpers::connect::connect;
use crate::odbc_source::main::odbc_reader::odbc_reader;
use crate::odbc_source::tests::helpers::elf_with_flags;
use crate::odbc_source::tests::test_types::ConnectErrorTestCase;

#[cfg(target_os = "linux")]
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
            description: "a path under //odbc/ whose file is in a multiarch folder, as MySQL's Debian 13 package registers it, names the file and the sed that fixes the registration",
            file: ("x86_64-linux-gnu/odbc/libmyodbc9w.so", Vec::new()),
            library: "/odbc/libmyodbc9w.so",
            expected_reason: "The driver library {path} doesn't exist, but {folder}/x86_64-linux-gnu/odbc/libmyodbc9w.so does: the driver's Debian package registers it under {folder}//odbc/, a packaging bug in MySQL Connector/ODBC's Debian 13 package. Correct the registration with: sudo sed -i 's#{folder}//odbc/#{folder}/x86_64-linux-gnu/odbc/#' /etc/odbcinst.ini",
        },
        ConnectErrorTestCase {
            description: "a path under //odbc/ whose file is in no multiarch folder names the registration to correct",
            file: ("x86_64-linux-gnu/odbc/libother.so", Vec::new()),
            library: "/odbc/libmissing.so",
            expected_reason: "The driver library {path} doesn't exist: correct the Driver= line that registers the driver in odbcinst.ini.",
        },
        ConnectErrorTestCase {
            description: "a path that can't be read, a folder here, gives the reason rather than calling it missing",
            file: ("libunused.so", Vec::new()),
            library: ".",
            expected_reason: "The driver library {path} can't be read: Is a directory (os error 21).",
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
            expected_reason: "The driver library {path} exists, and is built for static thread-local storage (STATIC_TLS), which the dynamic loader refuses once it reaches thread-locals already in use, as libstdc++'s are in a Python process that loaded libstdc++ after it started. Start the process with LD_PRELOAD=libstdc++.so.6, or use the sqlload-cli package's sqlload. If it still fails, `ldd {path}` lists the libraries it needs.",
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
        let file = folder.path().join(written);
        std::fs::create_dir_all(file.parent().expect("a folder")).expect("make the folder");
        std::fs::write(file, bytes).expect("write the file");
        let root = folder.path().display().to_string();
        let path = format!("{root}/{}", test_case.library);
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
            Some(
                test_case
                    .expected_reason
                    .replace("{path}", &path)
                    .replace("{folder}", &root)
            ),
            "{}: {error:?}",
            test_case.description
        );
    }
}
