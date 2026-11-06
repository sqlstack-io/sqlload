use crate::contract::tests::helpers::written;
use crate::contract::tests::test_types::WriteTestCase;

#[test]
fn given_a_project_file_when_writing_adoptions_then_only_the_tables_columns_change() {
    let test_cases = [
        WriteTestCase {
            description: "an inline columns table is edited in place, keeping comments and layout",
            text: "[sources.app]\nkind = \"postgres\" # the app\n\n[sources.app.tables.\"public.orders\"]\ndescription = \"Orders.\"\ncolumns = { status = \"int\", gone = \"text\" } # pinned\n\n[targets.dev]\nconnection = \"wh\"\n",
            table_description: "Orders.",
            expected_lines: &[
                "[sources.app]\nkind = \"postgres\" # the app\n\n[sources.app.tables.\"public.orders\"]\ndescription = \"Orders.\"\ncolumns = { status = \"text\", amount = \"float\" } # pinned\n\n[targets.dev]\nconnection = \"wh\"\n",
            ],
        },
        WriteTestCase {
            description: "a retyped column keeps the comments above and after it",
            text: "[sources.app.tables.\"public.orders\"]\n[sources.app.tables.\"public.orders\".columns]\n# int until the v2 migration\nstatus = \"int\"  # see the migration notes\ngone = \"text\"\n",
            table_description: "Orders.",
            expected_lines: &[
                "[sources.app.tables.\"public.orders\"]\n[sources.app.tables.\"public.orders\".columns]\n# int until the v2 migration\nstatus = \"text\"  # see the migration notes\namount = \"float\"\n",
            ],
        },
        WriteTestCase {
            description: "a table with no entry gets one after its source, with the description it had from its source comment",
            text: "[sources.app]\nkind = \"postgres\"\ninclude = [\"public.*\"]\n\n[targets.dev]\nconnection = \"wh\"\n",
            table_description: "Orders from the app.",
            expected_lines: &[
                "[sources.app]\nkind = \"postgres\"\ninclude = [\"public.*\"]\n\n[sources.app.tables.\"public.orders\"]\ndescription = \"Orders from the app.\"\n\n[sources.app.tables.\"public.orders\".columns]\namount = \"float\"\nstatus = \"text\"\n\n[targets.dev]\nconnection = \"wh\"\n",
            ],
        },
        WriteTestCase {
            description: "a table with no entry and no description is not written, since the entry would block it",
            text: "[sources.app]\nkind = \"postgres\"\ninclude = [\"public.*\"]\n",
            table_description: "",
            expected_lines: &[
                "",
                "[sources.app.tables.\"public.orders\"]: columns not written: a table entry needs a description and this table has none; add description = \"...\" there, then rerun contract generate --write",
            ],
        },
        WriteTestCase {
            description: "dotted columns keys stay dotted",
            text: "[sources.app.tables.\"public.orders\"]\ncolumns.status = \"int\"\ncolumns.gone = \"text\"\n",
            table_description: "Orders.",
            expected_lines: &[
                "[sources.app.tables.\"public.orders\"]\ncolumns.status = \"text\"\ncolumns.amount = \"float\"\n",
            ],
        },
        WriteTestCase {
            description: "a source not in the file is refused",
            text: "[targets.dev]\nconnection = \"wh\"\n",
            table_description: "Orders.",
            expected_lines: &["error: [sources.app] is not a table in sqlload.toml"],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            written(test_case.text, test_case.table_description),
            test_case.expected_lines,
            "{}",
            test_case.description
        );
    }
}
