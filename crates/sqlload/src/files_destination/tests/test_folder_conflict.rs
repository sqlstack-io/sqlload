use crate::files_destination::main::folder_conflict::folder_conflict;
use crate::files_destination::models::FolderClaim;
use crate::files_destination::tests::helpers::{bucket, bucket_elsewhere, claim, directory};
use crate::files_destination::tests::test_types::FolderConflictTestCase;
use crate::store_paths::models::StoreKind;

#[test]
fn given_configured_table_folders_when_checking_one_then_shared_and_nested_folders_are_named() {
    let test_cases = [
        FolderConflictTestCase {
            description: "the same bucket through another region and endpoint is the same place",
            claim: claim("app.public.orders", bucket(), "shop/orders/", "public"),
            others: vec![claim(
                "other.public.orders",
                bucket_elsewhere(),
                "shop/orders/",
                "public",
            )],
            expected_problem: "app.public.orders writes to s3://lake-bucket/shop/orders/, which overlaps other.public.orders's folder",
        },
        FolderConflictTestCase {
            description: "a folder inside another table's folder clashes",
            claim: claim("app.public.orders", bucket(), "shop/orders/", "public"),
            others: vec![claim(
                "app.public.archive",
                bucket(),
                "shop/orders/archive/",
                "public",
            )],
            expected_problem: "overlaps app.public.archive's folder s3://lake-bucket/shop/orders/archive/",
        },
        FolderConflictTestCase {
            description: "tables of two schemas with the same name are told to use {schema}",
            claim: claim("app.public.orders", bucket(), "app/orders/", "public"),
            others: vec![claim("app.sales.orders", bucket(), "app/orders/", "sales")],
            expected_problem: "add {schema} to the path",
        },
        FolderConflictTestCase {
            description: "directories are compared below their roots",
            claim: claim(
                "app.public.orders",
                directory("/srv/none/lake"),
                "orders/",
                "public",
            ),
            others: vec![claim(
                "app.public.x",
                directory("/srv/none/"),
                "lake/orders/",
                "public",
            )],
            expected_problem: "overlaps app.public.x's folder",
        },
        FolderConflictTestCase {
            description: "a folder name that only starts the same is separate",
            claim: claim("app.public.orders", bucket(), "shop/orders/", "public"),
            others: vec![claim("app.public.order", bucket(), "shop/order/", "public")],
            expected_problem: "",
        },
        FolderConflictTestCase {
            description: "another bucket is separate",
            claim: claim("app.public.orders", bucket(), "shop/orders/", "public"),
            others: vec![FolderClaim {
                folder: crate::store_paths::models::Location {
                    store: StoreKind::S3 {
                        bucket: "other-bucket".to_owned(),
                        region: None,
                        endpoint: None,
                    },
                    prefix: "shop/orders/".to_owned(),
                },
                ..claim("app.public.copy", bucket(), "", "public")
            }],
            expected_problem: "",
        },
        FolderConflictTestCase {
            description: "the table itself is no conflict",
            claim: claim("app.public.orders", bucket(), "shop/orders/", "public"),
            others: vec![claim(
                "app.public.orders",
                bucket(),
                "shop/orders/",
                "public",
            )],
            expected_problem: "",
        },
    ];
    for test_case in test_cases {
        let mut claims = test_case.others.clone();
        claims.push(test_case.claim.clone());
        let problem = folder_conflict(&test_case.claim, &claims).unwrap_or_default();
        assert!(
            problem.contains(test_case.expected_problem)
                && problem.is_empty() == test_case.expected_problem.is_empty(),
            "{}: {problem}",
            test_case.description
        );
    }
}
