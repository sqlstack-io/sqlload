use crate::loader::_helpers::render::{merged_errors, view_lines};
use crate::loader::models::PlanView;
use crate::loader::tests::helpers::plan_items;
use crate::loader::tests::test_types::{MergedErrorsTestCase, ViewLinesTestCase};

#[test]
fn given_a_plan_view_when_printing_it_then_it_follows_the_sqb_plan_layout() {
    let test_cases = [
        ViewLinesTestCase {
            description: "the ticket's new table loading directly: actions in one column, labels padded, empty sections hidden",
            ok: true,
            summary: "1 selected, 1 object to create, 1 to load",
            selected: 1,
            objects: &[(
                "RAW.SHOP__ORDERS",
                "create table",
                &[
                    ("", "id          int             -> NUMBER(38,0)"),
                    ("", "day         date            -> DATE  (from the path)"),
                    (
                        "",
                        "+ _sqlload_source_file, _source_version, _source_row, _load_id, _loaded_at",
                    ),
                ],
            )],
            loads: &[(
                "shop__orders",
                "append (cursor: day)",
                &[
                    (
                        "source",
                        "files  s3://example-bucket-lake/lake/orders/  parquet",
                    ),
                    ("files", "12 new of 40 listed"),
                    (
                        "route",
                        "Snowflake COPYs directly from s3://example-bucket-lake/lake/orders/ (nothing through the loader)",
                    ),
                    ("cursor", "day from 2026-11-04 (destination maximum)"),
                ],
            )],
            errors: &[],
            warnings: &[],
            expected_output: "Plan ready  1 selected, 1 object to create, 1 to load\n\nObjects (1)                                         sqlload apply\n└── RAW.SHOP__ORDERS                                create table\n    ├── id          int             -> NUMBER(38,0)\n    ├── day         date            -> DATE  (from the path)\n    └── + _sqlload_source_file, _source_version, _source_row, _load_id, _loaded_at\n\nLoads (1)                                           sqlload load\n└── shop__orders                                    append (cursor: day)\n    ├── source  files  s3://example-bucket-lake/lake/orders/  parquet\n    ├── files   12 new of 40 listed\n    ├── route   Snowflake COPYs directly from s3://example-bucket-lake/lake/orders/ (nothing through the loader)\n    └── cursor  day from 2026-11-04 (destination maximum)\n\n✓ Plan complete  1 selected, 1 object to create, 1 to load",
        },
        ViewLinesTestCase {
            description: "a configured method that cannot be used, then the slower opt-ins: Errors before Warnings, continuation lines under their text",
            ok: false,
            summary: "2 selected",
            selected: 2,
            objects: &[],
            loads: &[],
            errors: &[(
                "events, customers",
                "",
                &[
                    (
                        "reason",
                        "load_method s3://example-bucket-lake/ cannot be used: stage _SQLLOAD_STAGE uses storage integration LAKE_INT, but the connection names OTHER_INT",
                    ),
                    (
                        "fix",
                        "drop the stage so SQLLoad recreates it, or correct storage_integration",
                    ),
                ],
            )],
            warnings: &[
                (
                    "events",
                    "",
                    &[(
                        "",
                        "copy_to_internal_stage: every file is downloaded and uploaded to @~ (much slower than a direct load).\nA storage integration for s3://other-bucket/ avoids this.",
                    )],
                ),
                (
                    "customers",
                    "",
                    &[(
                        "",
                        "email: hmac masking pulls every file through the loader.\nmask = \"hash\" stays on the direct route.",
                    )],
                ),
            ],
            expected_output: "Plan ready  2 selected\n\nErrors (1)\n└── events, customers\n    ├── reason  load_method s3://example-bucket-lake/ cannot be used: stage _SQLLOAD_STAGE uses storage integration LAKE_INT, but the connection names OTHER_INT\n    └── fix     drop the stage so SQLLoad recreates it, or correct storage_integration\n\nWarnings (2)\n├── events\n│   └── copy_to_internal_stage: every file is downloaded and uploaded to @~ (much slower than a direct load).\n│       A storage integration for s3://other-bucket/ avoids this.\n└── customers\n    └── email: hmac masking pulls every file through the loader.\n        mask = \"hash\" stays on the direct route.\n\n✗ Plan failed with errors  2 selected",
        },
        ViewLinesTestCase {
            description: "a long name keeps two spaces before its action",
            ok: true,
            summary: "1 selected, 1 to load",
            selected: 1,
            objects: &[],
            loads: &[(
                "RAW.SALESFORCE__OPPORTUNITY_LINE_ITEM_SCHEDULE_HISTORY",
                "append",
                &[],
            )],
            errors: &[],
            warnings: &[],
            expected_output: "Plan ready  1 selected, 1 to load\n\nLoads (1)                                           sqlload load\n└── RAW.SALESFORCE__OPPORTUNITY_LINE_ITEM_SCHEDULE_HISTORY  append\n\n✓ Plan complete  1 selected, 1 to load",
        },
    ];
    for test_case in test_cases {
        let view = PlanView {
            ok: test_case.ok,
            summary: test_case.summary.to_owned(),
            selected: test_case.selected,
            objects: plan_items(test_case.objects),
            loads: plan_items(test_case.loads),
            skipped: Vec::new(),
            errors: plan_items(test_case.errors),
            warnings: plan_items(test_case.warnings),
        };
        assert_eq!(
            view_lines(&view).join("\n"),
            test_case.expected_output,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_tables_stopped_for_the_same_reasons_when_printing_errors_then_they_share_one_item() {
    let test_cases = [
        MergedErrorsTestCase {
            description: "the same reason on two tables is one item naming both; another reason stays apart",
            errors: &[
                (
                    "events",
                    "",
                    &[("reason", "stage uses another integration")],
                ),
                ("orders", "", &[("reason", "no cursor_column")]),
                (
                    "customers",
                    "",
                    &[("reason", "stage uses another integration")],
                ),
            ],
            expected_names: &["events, customers", "orders"],
        },
        MergedErrorsTestCase {
            description: "a fix that names its table keeps each table's item",
            errors: &[
                (
                    "events",
                    "",
                    &[
                        ("reason", "a type change needs a rebuild"),
                        ("fix", "sqlload load events --allow-type-change-rebuild"),
                    ],
                ),
                (
                    "orders",
                    "",
                    &[
                        ("reason", "a type change needs a rebuild"),
                        ("fix", "sqlload load orders --allow-type-change-rebuild"),
                    ],
                ),
            ],
            expected_names: &["events", "orders"],
        },
    ];
    for test_case in test_cases {
        let names: Vec<String> = merged_errors(plan_items(test_case.errors))
            .into_iter()
            .map(|item| item.name)
            .collect();
        assert_eq!(names, test_case.expected_names, "{}", test_case.description);
    }
}
