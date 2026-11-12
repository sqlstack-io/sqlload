//! Child tables from list fields (`unnest`), and dlt's naming.

use crate::helpers::run_sqlload;
use crate::test_types::UnnestPlanTestCase;

const ORDERS: &str =
    "{\"id\": 1, \"items\": [{\"sku\": \"a\", \"parts\": [1, 2]}], \"tags\": [\"x\"]}\n";

#[test]
fn given_nesting_settings_a_table_cannot_use_when_planning_then_plan_names_each_problem()
-> Result<(), String> {
    let test_cases = [
        UnnestPlanTestCase {
            description: "parent and child paths, dlt naming and its row id plan",
            settings: "naming = \"dlt\"\nrow_id = true\n[sources.orders.tables.orders]\nunnest = [\"items\", \"items.parts\", \"tags\"]\n",
            files_target: false,
            expected_ok: true,
            expected_problem: "",
        },
        UnnestPlanTestCase {
            description: "a word other than all",
            settings: "[sources.orders.tables.orders]\nunnest = \"every\"\n",
            files_target: false,
            expected_ok: false,
            expected_problem: "unnest = \"every\": list the dotted paths of the list fields to unnest, or write \"all\"",
        },
        UnnestPlanTestCase {
            description: "a child path without its parent",
            settings: "[sources.orders.tables.orders]\nunnest = [\"items.parts\"]\n",
            files_target: false,
            expected_ok: false,
            expected_problem: "unnest \"items.parts\": its parent list \"items\" must be unnested too; add it",
        },
        UnnestPlanTestCase {
            description: "only the path deeper than max_depth",
            settings: "[sources.orders.tables.orders]\nunnest = [\"items\", \"items.parts\"]\nmax_depth = 1\n",
            files_target: false,
            expected_ok: false,
            expected_problem: "unnest \"items.parts\": it is 2 levels deep, past max_depth = 1; raise max_depth or drop it",
        },
        UnnestPlanTestCase {
            description: "an empty part",
            settings: "[sources.orders.tables.orders]\nunnest = [\"items\", \"items.\"]\n",
            files_target: false,
            expected_ok: false,
            expected_problem: "unnest \"items.\": a path names a field at each level, joined by \".\"",
        },
        UnnestPlanTestCase {
            description: "a masked list field",
            settings: "[sources.orders.tables.orders]\nunnest = [\"tags\"]\nmask = { tags = \"hash\" }\n",
            files_target: false,
            expected_ok: false,
            expected_problem: "unnest \"tags\": mask also names tags, which unnesting moves to child tables; drop one of them",
        },
        UnnestPlanTestCase {
            description: "a list field an exclude pattern matches",
            settings: "[sources.orders.tables.orders]\nunnest = [\"tags\"]\nexclude_columns = [\"ta*\"]\n",
            files_target: false,
            expected_ok: false,
            expected_problem: "unnest \"tags\": exclude_columns ta* also names tags",
        },
        UnnestPlanTestCase {
            description: "max_depth without unnest",
            settings: "[sources.orders.tables.orders]\nmax_depth = 2\n",
            files_target: false,
            expected_ok: false,
            expected_problem: "max_depth limits the child tables unnest makes, and this table sets no unnest",
        },
        UnnestPlanTestCase {
            description: "delete_insert",
            settings: "write_strategy = \"delete_insert\"\nunique_key = [\"id\"]\n[sources.orders.tables.orders]\nunnest = \"all\"\n",
            files_target: false,
            expected_ok: false,
            expected_problem: "unnest does not yet write child tables with write_strategy = \"delete_insert\"",
        },
        UnnestPlanTestCase {
            description: "an enforced contract",
            settings: "contract = \"enforced\"\n[sources.orders.tables.orders]\nunnest = \"all\"\n",
            files_target: false,
            expected_ok: false,
            expected_problem: "unnest does not yet write child tables with contract = \"enforced\"",
        },
        UnnestPlanTestCase {
            description: "a files target",
            settings: "[sources.orders.tables.orders]\nunnest = \"all\"\n",
            files_target: true,
            expected_ok: false,
            expected_problem: "unnest does not yet write child tables with a files target",
        },
        UnnestPlanTestCase {
            description: "row ids turned off under dlt naming",
            settings: "naming = \"dlt\"\nrow_id = false\n",
            files_target: false,
            expected_ok: false,
            expected_problem: "row_id = false: naming = \"dlt\" gives every row its _dlt_id",
        },
    ];
    for test_case in &test_cases {
        let root = tempfile::tempdir().map_err(|error| error.to_string())?;
        std::fs::create_dir_all(root.path().join("drop")).map_err(|error| error.to_string())?;
        std::fs::write(root.path().join("drop/orders.ndjson"), ORDERS)
            .map_err(|error| error.to_string())?;
        let target = [
            format!(
                "[connections.wh]\nkind = \"duckdb\"\ndatabase = \"{}\"\n\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"raw\"\n",
                root.path().join("out.duckdb").display()
            ),
            "[targets.dev]\nconnection = \"files\"\npath = \"out/{table}/\"\n".to_owned(),
        ][usize::from(test_case.files_target)]
        .clone();
        let config = root.path().join("sqlload.toml");
        let text = format!(
            "{target}\n[connections.files]\nkind = \"filesystem\"\nroot = \"{}\"\n\n[sources.orders]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"files\"\npath = \"drop/\"\nformat = \"ndjson\"\n{}",
            root.path().display(),
            test_case.settings
        );
        std::fs::write(&config, text).map_err(|error| error.to_string())?;
        let (ok, output) = run_sqlload(&config, &["plan"], ("", ""))?;
        assert_eq!(
            (ok, output.contains(test_case.expected_problem)),
            (test_case.expected_ok, true),
            "{}: {output}",
            test_case.description
        );
    }
    Ok(())
}
