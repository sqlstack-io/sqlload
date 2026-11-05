use crate::adapters::files::main::table_path::table_path;
use crate::adapters::files::models::TablePathRequest;
use crate::adapters::files::tests::helpers::bucket;
use crate::adapters::files::tests::test_types::TablePathTestCase;
use crate::store_paths::models::StoreKind;

#[test]
fn given_a_path_template_when_resolving_then_the_table_folder_and_partition_folder_are_found() {
    let test_cases = [
        TablePathTestCase {
            description: "the default path is source and table",
            template: "{source}/{table}/",
            store: bucket(),
            expected_folder: Ok("s3://lake-bucket/shop/orders/"),
            expected_partition: None,
        },
        TablePathTestCase {
            description: "a cursor date in the last folder makes partitions",
            template: "{source}/{schema}/{table}/day={cursor:date}/",
            store: bucket(),
            expected_folder: Ok("s3://lake-bucket/shop/public/orders/"),
            expected_partition: Some("day={cursor:date}/"),
        },
        TablePathTestCase {
            description: "the partition folder may hold other placeholders",
            template: "{table}/{table}_{cursor}",
            store: bucket(),
            expected_folder: Ok("s3://lake-bucket/orders/"),
            expected_partition: Some("orders_{cursor}/"),
        },
        TablePathTestCase {
            description: "an absolute path inside a filesystem connection is relative to its root",
            template: "/data/lake/{table}/",
            store: StoreKind::Filesystem {
                root: "/data/lake".to_owned(),
            },
            expected_folder: Ok("/data/lake/orders/"),
            expected_partition: None,
        },
        TablePathTestCase {
            description: "the cursor cannot be above the table's folder",
            template: "{cursor}/{table}/",
            store: bucket(),
            expected_folder: Err("may appear once, in the last folder"),
            expected_partition: None,
        },
        TablePathTestCase {
            description: "the partition folder takes one cursor placeholder",
            template: "{table}/{cursor}-{cursor:date}/",
            store: bucket(),
            expected_folder: Err("needs exactly one {cursor} or {cursor:date}"),
            expected_partition: None,
        },
        TablePathTestCase {
            description: "an unknown cursor format is rejected",
            template: "{table}/{cursor:month}/",
            store: bucket(),
            expected_folder: Err("needs exactly one {cursor} or {cursor:date}"),
            expected_partition: None,
        },
        TablePathTestCase {
            description: "an unknown placeholder names the known ones",
            template: "{tabel}/",
            store: bucket(),
            expected_folder: Err("uses {tabel}, which is not one of: {source}, {schema}, {table}"),
            expected_partition: None,
        },
        TablePathTestCase {
            description: "a table needs a folder of its own",
            template: "day={cursor:date}/",
            store: bucket(),
            expected_folder: Err("must give the table a folder of its own"),
            expected_partition: None,
        },
        TablePathTestCase {
            description: "a path is relative to the connection",
            template: "s3://lake-bucket/{table}/",
            store: bucket(),
            expected_folder: Err("not a URL"),
            expected_partition: None,
        },
        TablePathTestCase {
            description: "a path cannot leave the connection",
            template: "../{table}/",
            store: bucket(),
            expected_folder: Err("\"..\" segment"),
            expected_partition: None,
        },
    ];
    for test_case in test_cases {
        let request = TablePathRequest {
            template: test_case.template,
            connection: "lake",
            store: &test_case.store,
            source: "shop",
            schema: "public",
            table: "orders",
        };
        let path = table_path(&request);
        let folder = path.as_ref().map(|path| path.folder.to_string());
        let partition = path
            .as_ref()
            .ok()
            .and_then(|path| path.partition.as_ref())
            .map(ToString::to_string);
        assert_eq!(
            folder.as_deref().ok(),
            test_case.expected_folder.ok(),
            "{}",
            test_case.description
        );
        assert!(
            folder
                .err()
                .cloned()
                .unwrap_or_default()
                .contains(test_case.expected_folder.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
        assert_eq!(
            partition.as_deref(),
            test_case.expected_partition,
            "{}",
            test_case.description
        );
    }
}
