use crate::playground::_helpers::project::create_playground;
use crate::playground::constants::{CUSTOMERS_CSV, ORDERS_CSV, PROJECT_TOML};
use crate::playground::tests::test_types::CreatePlaygroundTestCase;

const CREATED: &[&str] = &[
    "SQLLoad playground created",
    "",
    "  Project  shop",
    "  Source   CSV files in shop/data/: 50 customers, 1,000 orders",
    "  Target   DuckDB shop/shop.duckdb, schema raw",
    "",
    "Try:",
    "  cd shop",
    "  sqlload debug",
    "  sqlload plan",
    "  sqlload apply",
    "  sqlload load",
    "  sqlload playground --change",
    "  sqlload load",
];

const PROJECT: &[(&str, &str)] = &[
    ("sqlload.toml", PROJECT_TOML),
    (".gitignore", "shop.duckdb\nshop.duckdb.wal\n"),
    ("data/customers/customers.csv", CUSTOMERS_CSV),
    ("data/orders/orders_2026-11-01.csv", ORDERS_CSV),
    ("notes.txt", ""),
];

#[test]
fn given_a_folder_when_creating_a_playground_then_it_writes_the_project_only_into_a_new_or_empty_folder()
 {
    let test_cases = [
        CreatePlaygroundTestCase {
            description: "a folder that doesn't exist gets the project",
            existing_folder: "elsewhere",
            existing_files: &[],
            expected_lines: Ok(CREATED),
            expected_files: PROJECT,
        },
        CreatePlaygroundTestCase {
            description: "an empty folder gets the project",
            existing_folder: "shop",
            existing_files: &[],
            expected_lines: Ok(CREATED),
            expected_files: PROJECT,
        },
        CreatePlaygroundTestCase {
            description: "a folder holding anything is refused and left as it was",
            existing_folder: "shop",
            existing_files: &["notes.txt"],
            expected_lines: Err(
                "shop already exists and is not an empty folder; name a new folder, as in `sqlload playground my-shop`",
            ),
            expected_files: &[
                ("sqlload.toml", ""),
                (".gitignore", ""),
                ("data/customers/customers.csv", ""),
                ("notes.txt", "kept"),
            ],
        },
    ];
    for test_case in test_cases {
        let root = tempfile::tempdir().expect("temporary folder");
        let existing = root.path().join(test_case.existing_folder);
        std::fs::create_dir(&existing).expect("folder");
        for file in test_case.existing_files {
            std::fs::write(existing.join(file), "kept").expect("file");
        }
        let dir = root.path().join("shop");
        let outcome = create_playground(&dir, "shop");
        let lines: Result<Vec<&str>, &str> = outcome
            .as_ref()
            .map(|lines| lines.iter().map(String::as_str).collect())
            .map_err(String::as_str);
        assert_eq!(
            lines,
            test_case.expected_lines.map(<[&str]>::to_vec),
            "{}",
            test_case.description
        );
        let written: Vec<(&str, String)> = test_case
            .expected_files
            .iter()
            .map(|(path, _)| {
                (
                    *path,
                    std::fs::read_to_string(dir.join(path)).unwrap_or_default(),
                )
            })
            .collect();
        let expected_written: Vec<(&str, String)> = test_case
            .expected_files
            .iter()
            .map(|(path, text)| (*path, (*text).to_owned()))
            .collect();
        assert_eq!(written, expected_written, "{}", test_case.description);
    }
}
