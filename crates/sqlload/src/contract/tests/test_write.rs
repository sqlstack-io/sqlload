use crate::contract::tests::helpers::written;
use crate::contract::tests::test_types::WriteTestCase;

#[test]
fn given_a_project_file_when_writing_adoptions_then_only_the_tables_columns_change() {
    let test_cases = [
        WriteTestCase {
            description: "an inline columns table is edited in place, keeping comments and layout",
            text: "[sources.app]\nkind = \"postgres\" # the app\n\n[sources.app.tables.\"public.orders\"]\ndescription = \"Orders.\"\ncolumns = { status = \"int\", gone = \"text\" } # pinned\n\n[targets.dev]\nconnection = \"wh\"\n",
            table_description: "Orders.",
            expected: Ok((
                "[sources.app]\nkind = \"postgres\" # the app\n\n[sources.app.tables.\"public.orders\"]\ndescription = \"Orders.\"\ncolumns = { status = \"text\", amount = \"float\" } # pinned\n\n[targets.dev]\nconnection = \"wh\"\n",
                &[],
            )),
        },
        WriteTestCase {
            description: "a table with no entry gets one after its source, with the description it had from its source comment",
            text: "[sources.app]\nkind = \"postgres\"\ninclude = [\"public.*\"]\n\n[targets.dev]\nconnection = \"wh\"\n",
            table_description: "Orders from the app.",
            expected: Ok((
                "[sources.app]\nkind = \"postgres\"\ninclude = [\"public.*\"]\n\n[sources.app.tables.\"public.orders\"]\ndescription = \"Orders from the app.\"\n\n[sources.app.tables.\"public.orders\".columns]\namount = \"float\"\nstatus = \"text\"\n\n[targets.dev]\nconnection = \"wh\"\n",
                &[],
            )),
        },
        WriteTestCase {
            description: "a table with no entry and no description is not written, since the entry would block it",
            text: "[sources.app]\nkind = \"postgres\"\ninclude = [\"public.*\"]\n",
            table_description: "",
            expected: Ok((
                "",
                &[
                    "[sources.app.tables.\"public.orders\"]: columns not written: a table entry needs a description and this table has none; add description = \"...\" there, then rerun contract generate --write",
                ],
            )),
        },
        WriteTestCase {
            description: "dotted columns keys stay dotted",
            text: "[sources.app.tables.\"public.orders\"]\ncolumns.status = \"int\"\ncolumns.gone = \"text\"\n",
            table_description: "Orders.",
            expected: Ok((
                "[sources.app.tables.\"public.orders\"]\ncolumns.status = \"text\"\ncolumns.amount = \"float\"\n",
                &[],
            )),
        },
        WriteTestCase {
            description: "a source not in the file is refused",
            text: "[targets.dev]\nconnection = \"wh\"\n",
            table_description: "Orders.",
            expected: Err("[sources.app] is not a table in sqlload.toml"),
        },
    ];
    for test_case in test_cases {
        let expected = test_case
            .expected
            .map(|(text, problems)| {
                (
                    text.to_owned(),
                    problems
                        .iter()
                        .map(|problem| (*problem).to_owned())
                        .collect(),
                )
            })
            .map_err(str::to_owned);
        assert_eq!(
            written(test_case.text, test_case.table_description),
            expected,
            "{}",
            test_case.description
        );
    }
}
