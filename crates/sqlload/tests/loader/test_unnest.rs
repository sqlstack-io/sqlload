//! Child tables from list fields (`unnest`), and dlt's naming.

use crate::helpers::{duckdb_text, run_sqlload};
use crate::test_types::{RowIdTestCase, UnnestPlanTestCase};

const COLUMNS: &str = "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name = 'orders'";

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
            description: "the unnested field is not a column of the table: the plan lists id and items only",
            settings: "[sources.orders.tables.orders]\nunnest = [\"tags\"]\n",
            files_target: false,
            expected_ok: true,
            expected_problem: "├── id    int    -> BIGINT\n    ├── items json   -> JSON\n    └── + _sqlload_source_file",
        },
        UnnestPlanTestCase {
            description: "a merge, until its child rows are replaced by root",
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\n[sources.orders.tables.orders]\nunnest = [\"tags\"]\n",
            files_target: false,
            expected_ok: false,
            expected_problem: "unnest does not yet write child tables with write_strategy = \"merge\"",
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

#[test]
fn given_row_id_settings_when_loading_then_each_row_gets_its_id_and_a_merged_row_keeps_it()
-> Result<(), String> {
    let test_cases = [
        RowIdTestCase {
            description: "without row_id or naming the table is as before: no row id, SQLLoad's own names",
            settings: "",
            runs: &[("a.ndjson", "{\"id\": 1, \"v\": \"a\"}\n")],
            expected_columns: "id,v,_sqlload_source_file,_sqlload_source_version,_sqlload_source_row,_sqlload_load_id,_sqlload_loaded_at",
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM raw.orders",
            expected_result: "1",
        },
        RowIdTestCase {
            description: "row_id = true on an append: every row a new 14-character id, after the load time",
            settings: "row_id = true\nwrite_strategy = \"append\"\n",
            runs: &[
                (
                    "a.ndjson",
                    "{\"id\": 1, \"v\": \"a\"}\n{\"id\": 1, \"v\": \"a\"}\n",
                ),
                ("b.ndjson", "{\"id\": 1, \"v\": \"a\"}\n"),
            ],
            expected_columns: "id,v,_sqlload_source_file,_sqlload_source_version,_sqlload_source_row,_sqlload_load_id,_sqlload_loaded_at,_sqlload_row_id",
            check_query: "SELECT count(DISTINCT _sqlload_row_id) || '/' || count(*) || '/' || min(length(_sqlload_row_id)) || '/' || max(length(_sqlload_row_id)) FROM raw.orders",
            expected_result: "3/3/14/14",
        },
        RowIdTestCase {
            description: "naming = \"dlt\" on a merge: _dlt_load_id and _dlt_id, the id dlt's key hash of {\"id\":n}, kept when a later load changes the row",
            settings: "naming = \"dlt\"\nwrite_strategy = \"merge\"\nunique_key = [\"id\"]\n",
            runs: &[
                (
                    "a.ndjson",
                    "{\"id\": 1, \"v\": \"a\"}\n{\"id\": 2, \"v\": \"b\"}\n",
                ),
                ("b.ndjson", "{\"id\": 1, \"v\": \"c\"}\n"),
            ],
            expected_columns: "id,v,_sqlload_source_file,_sqlload_source_version,_sqlload_source_row,_dlt_load_id,_sqlload_loaded_at,_dlt_id",
            check_query: "SELECT string_agg(id || ':' || v || ':' || _dlt_id, ',' ORDER BY id) FROM raw.orders",
            expected_result: "1:c:z5jIxXIbUnmbKw,2:b:M6/KlLzJ2FeV/w",
        },
    ];
    for test_case in &test_cases {
        let root = tempfile::tempdir().map_err(|error| error.to_string())?;
        std::fs::create_dir_all(root.path().join("drop")).map_err(|error| error.to_string())?;
        let database = root.path().join("out.duckdb");
        let config = root.path().join("sqlload.toml");
        let text = format!(
            "[connections.wh]\nkind = \"duckdb\"\ndatabase = \"{}\"\n\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"raw\"\n\n[connections.files]\nkind = \"filesystem\"\nroot = \"{}\"\n\n[sources.orders]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"files\"\npath = \"drop/\"\nformat = \"ndjson\"\n{}",
            database.display(),
            root.path().display(),
            test_case.settings
        );
        std::fs::write(&config, text).map_err(|error| error.to_string())?;
        for (name, contents) in test_case.runs {
            std::fs::write(root.path().join("drop").join(name), contents)
                .map_err(|error| error.to_string())?;
            let (ok, output) = run_sqlload(&config, &["load"], ("", ""))?;
            assert!(ok, "{}: {output}", test_case.description);
        }
        assert_eq!(
            (
                duckdb_text(&database, COLUMNS)?,
                duckdb_text(&database, test_case.check_query)?
            ),
            (
                test_case.expected_columns.to_owned(),
                test_case.expected_result.to_owned()
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
