use crate::files_destination::main::overlapping_folders::overlapping_folders;
use crate::files_destination::tests::helpers::{bucket, location};
use crate::files_destination::tests::test_types::OverlappingFoldersTestCase;
use crate::store_paths::models::{Location, StoreKind};

#[test]
fn given_table_folders_when_checking_then_shared_and_nested_folders_are_named() {
    let local = StoreKind::Filesystem {
        root: "/data".to_owned(),
    };
    let test_cases = [
        OverlappingFoldersTestCase {
            description: "two tables in one folder both overlap",
            tables: vec![
                ("orders", location(bucket(), "shop/orders/")),
                ("orders_copy", location(bucket(), "shop/orders/")),
            ],
            expected_problems: &[
                "orders writes to s3://lake-bucket/shop/orders/, which overlaps orders_copy's folder s3://lake-bucket/shop/orders/; give each table a folder of its own",
                "orders_copy writes to s3://lake-bucket/shop/orders/, which overlaps orders's folder s3://lake-bucket/shop/orders/; give each table a folder of its own",
            ],
        },
        OverlappingFoldersTestCase {
            description: "a folder inside another table's folder overlaps it",
            tables: vec![
                ("orders", location(bucket(), "shop/orders/")),
                ("archive", location(bucket(), "shop/orders/archive/")),
            ],
            expected_problems: &[
                "orders writes to s3://lake-bucket/shop/orders/, which overlaps archive's folder s3://lake-bucket/shop/orders/archive/; give each table a folder of its own",
                "archive writes to s3://lake-bucket/shop/orders/archive/, which overlaps orders's folder s3://lake-bucket/shop/orders/; give each table a folder of its own",
            ],
        },
        OverlappingFoldersTestCase {
            description: "folders whose names only start the same, or in another store, are separate",
            tables: vec![
                ("orders", location(bucket(), "shop/orders/")),
                ("order", location(bucket(), "shop/order/")),
                ("local_orders", location(local.clone(), "shop/orders/")),
            ],
            expected_problems: &["", "", ""],
        },
    ];
    for test_case in test_cases {
        let mut tables: Vec<Option<(&str, &Location)>> = test_case
            .tables
            .iter()
            .map(|(name, folder)| Some((*name, folder)))
            .collect();
        tables.push(None);
        let problems: Vec<String> = overlapping_folders(&tables)
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect();
        assert_eq!(
            problems[..test_case.tables.len()],
            *test_case.expected_problems,
            "{}",
            test_case.description
        );
        assert_eq!(
            problems.last().map(String::as_str),
            Some(""),
            "{}: a warehouse table never overlaps",
            test_case.description
        );
    }
}
