use crate::playground::_helpers::project::apply_change;
use crate::playground::_helpers::project::create_playground;
use crate::playground::constants::{CHANGES, DEFAULT_NAME, PROJECT_TOML};
use crate::playground::tests::test_types::ApplyChangeTestCase;

const CHANGED: &[&str] = &[
    "added  data/orders/orders_2026-11-02.csv  1 changed, 1 new",
    "├── order 42    status placed → cancelled, updated_at 2026-11-01 00:56:00 → 2026-11-02 09:00:00",
    "└── order 1001  new: customer_id 7, status placed, total 99.90, updated_at 2026-11-02 09:05:00",
    "",
    "Run `sqlload load` to merge it.",
];

const CHANGE_FILE: &str = "order_id,customer_id,status,total,updated_at\n42,43,cancelled,67.54,2026-11-02 09:00:00\n1001,7,placed,99.90,2026-11-02 09:05:00\n";

#[test]
fn given_a_playground_when_changing_its_data_then_each_bundled_change_is_added_once_and_described()
{
    let test_cases = [
        ApplyChangeTestCase {
            description: "the first change adds its file and names each order it changes or adds",
            config: PROJECT_TOML,
            runs: 1,
            expected_lines: Ok(CHANGED),
            expected_change: CHANGE_FILE,
        },
        ApplyChangeTestCase {
            description: "once every change is in, another run adds nothing and says how to start again",
            config: PROJECT_TOML,
            runs: 2,
            expected_lines: Ok(&[
                "Nothing left to change: data/orders/orders_2026-11-02.csv, the last change, is already in the project.",
                "To start again, delete shop.duckdb and data/orders/orders_2026-11-02.csv.",
            ]),
            expected_change: CHANGE_FILE,
        },
        ApplyChangeTestCase {
            description: "a project the playground didn't write is refused and its data left alone",
            config: "default_target = \"local\"\n",
            runs: 1,
            expected_lines: Err(
                "<config> is not a playground's sqlload.toml; run `sqlload playground --change` in a project `sqlload playground` created",
            ),
            expected_change: "",
        },
    ];
    for test_case in test_cases {
        let root = tempfile::tempdir().expect("temporary folder");
        let dir = root.path().join(DEFAULT_NAME);
        create_playground(&dir, DEFAULT_NAME).expect("playground");
        let config = dir.join("sqlload.toml");
        std::fs::write(&config, test_case.config).expect("sqlload.toml");
        let mut outcome = Ok(Vec::new());
        for _ in 0..test_case.runs {
            outcome = apply_change(&config);
        }
        let shown = config.display().to_string();
        let expected = test_case
            .expected_lines
            .map(|lines| {
                lines
                    .iter()
                    .map(|line| (*line).to_owned())
                    .collect::<Vec<_>>()
            })
            .map_err(|message| message.replace("<config>", &shown));
        assert_eq!(outcome, expected, "{}", test_case.description);
        let added = std::fs::read_to_string(dir.join(CHANGES[0].path)).unwrap_or_default();
        assert_eq!(
            added, test_case.expected_change,
            "{}",
            test_case.description
        );
    }
}
