use crate::cdc::_helpers::snapshot_log::snapshot_folder;
use crate::cdc::tests::test_types::SnapshotFolderTestCase;

/// A source's snapshot records sit under the path template's folders that name nothing but the source, so every table's folder stays its own and the place never moves with the tables selected.
#[test]
fn given_a_files_path_template_when_placing_snapshot_records_then_they_sit_under_its_fixed_folders()
{
    let test_cases = [
        SnapshotFolderTestCase {
            description: "the default template keeps the source folder",
            template: "{source}/{table}/",
            expected_folder: "app/_sqlload/cdc/app/sqlload_app/",
        },
        SnapshotFolderTestCase {
            description: "fixed folders before the source are kept",
            template: "lake/raw/{source}/{table}/",
            expected_folder: "lake/raw/app/_sqlload/cdc/app/sqlload_app/",
        },
        SnapshotFolderTestCase {
            description: "a folder mixing the source with the table stops the fixed part",
            template: "lake/{source}_{table}/",
            expected_folder: "lake/_sqlload/cdc/app/sqlload_app/",
        },
        SnapshotFolderTestCase {
            description: "a template starting with a placeholder other than the source keeps nothing",
            template: "{schema}/{table}/",
            expected_folder: "_sqlload/cdc/app/sqlload_app/",
        },
        SnapshotFolderTestCase {
            description: "folders after the first placeholder are not kept, even fixed ones",
            template: "lake/{table}/data/dt={cursor:date}/",
            expected_folder: "lake/_sqlload/cdc/app/sqlload_app/",
        },
        SnapshotFolderTestCase {
            description: "a template naming only the source keeps the records out of the table's folder",
            template: "lake/{source}/",
            expected_folder: "lake/_sqlload/cdc/app/sqlload_app/",
        },
        SnapshotFolderTestCase {
            description: "a fixed template keeps the records out of the table's folder",
            template: "lake/orders/",
            expected_folder: "lake/_sqlload/cdc/app/sqlload_app/",
        },
        SnapshotFolderTestCase {
            description: "an absolute template stays absolute",
            template: "/srv/out/{table}",
            expected_folder: "/srv/out/_sqlload/cdc/app/sqlload_app/",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            snapshot_folder(test_case.template, "app", "sqlload_app"),
            test_case.expected_folder,
            "{}",
            test_case.description
        );
    }
}
