use crate::table_options::tests::helpers::{
    DUCKDB, FILES_SOURCE, FILES_TARGET, POSTGRES_SOURCE, POSTGRES_TABLE, python_problems,
    toml_problems,
};
use crate::table_options::tests::test_types::{PythonOptionsTestCase, TomlOptionsTestCase};

#[test]
fn given_table_options_that_do_not_combine_when_parsing_a_toml_project_then_parse_reports_each_with_plan_s_wording()
 {
    let test_cases = [
        TomlOptionsTestCase {
            description: "merge without a unique_key",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"merge\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: write_strategy = \"merge\" needs a unique_key",
        },
        TomlOptionsTestCase {
            description: "replace with a cursor",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"replace\"\ncursor_column = \"updated_at\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: write_strategy = \"replace\" reloads the whole table, so it takes no cursor_column",
        },
        TomlOptionsTestCase {
            description: "append with a lookback",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"append\"\ncursor_column = \"updated_at\"\nlookback = \"1h\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: lookback re-inserts the window on every run with append",
        },
        TomlOptionsTestCase {
            description: "delete_insert with neither a cursor nor a replace_key",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"delete_insert\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: write_strategy = \"delete_insert\" replaces a cursor range or the rows of the key values it reads, so it needs a cursor_column or a replace_key",
        },
        TomlOptionsTestCase {
            description: "replace_key on a merge",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\nreplace_key = [\"version\"]\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: replace_key is for write_strategy = \"delete_insert\"",
        },
        TomlOptionsTestCase {
            description: "lookback_from now with a lookback in cursor units",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"seq\"\nlookback = 1\nlookback_from = \"now\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: lookback_from = \"now\" counts lookback back from the time of the run, so seq needs a duration lookback",
        },
        TomlOptionsTestCase {
            description: "lookback_from now without a lookback",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"updated_at\"\nlookback_from = \"now\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: lookback_from = \"now\" counts lookback back from now, so updated_at needs a lookback",
        },
        TomlOptionsTestCase {
            description: "a database source's replace_key with a cursor",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"delete_insert\"\nreplace_key = [\"version\"]\ncursor_column = \"updated_at\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: cursor_column updated_at does not apply to delete_insert by replace_key: a load deletes every row of each key value it reads, so the rows of a key value outside the cursor range, which it never reads, would be lost. Fix: drop cursor_column, so every load reads the whole source and replaces each key value it holds; or use write_strategy = \"merge\" on a unique_key to read by the cursor",
        },
        TomlOptionsTestCase {
            description: "an empty cursor_column list",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"delete_insert\"\ncursor_column = []\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: cursor_column = [] names no column",
        },
        TomlOptionsTestCase {
            description: "a cursor column listed twice",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\ncursor_column = [\"updated_at\", \"updated_at\"]\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: cursor_column lists updated_at twice",
        },
        TomlOptionsTestCase {
            description: "null_cursor without a cursor",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"replace\"\nnull_cursor = \"ignore\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: null_cursor says what a cursor read does with rows whose cursor is NULL, but the table has no cursor_column",
        },
        TomlOptionsTestCase {
            description: "on_cursor_out_of_range on a database source",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"append\"\ncursor_column = \"updated_at\"\non_cursor_out_of_range = \"skip\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: on_cursor_out_of_range checks the rows a Python table's function returns against its cursor window",
        },
        TomlOptionsTestCase {
            description: "a coalesced cursor on a delete_insert",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"delete_insert\"\ncursor_column = [\"updated_at\", \"created_at\"]\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: cursor_column lists several columns to coalesce (coalesce(updated_at, created_at)), but delete_insert replaces the destination rows in a range of one cursor column",
        },
        TomlOptionsTestCase {
            description: "null_cursor include on an append",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"append\"\ncursor_column = \"updated_at\"\nnull_cursor = \"include\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: null_cursor = \"include\" reads the rows whose updated_at is NULL on every run, and append would append them again on every run, duplicating them; use write_strategy = \"merge\" with a unique_key",
        },
        TomlOptionsTestCase {
            description: "null_cursor include on a delete_insert",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"updated_at\"\nnull_cursor = \"include\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: null_cursor = \"include\" reads the rows whose updated_at is NULL on every run, and they sit outside every cursor range delete_insert replaces, so it would insert them again on every run, duplicating them; use write_strategy = \"merge\" with a unique_key, or set null_cursor = \"ignore\" to skip them",
        },
        TomlOptionsTestCase {
            description: "on_duplicate_keys on an append",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"append\"\non_duplicate_keys = \"fail\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: on_duplicate_keys is for write_strategy = \"merge\", which matches rows on unique_key; append keeps every row",
        },
        TomlOptionsTestCase {
            description: "newest_by naming a unique_key column",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\nnewest_by = \"id\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: newest_by column id is in the unique_key",
        },
        TomlOptionsTestCase {
            description: "newest_by naming a column twice",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\nnewest_by = [\"updated_at\", \"updated_at\"]\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: newest_by names updated_at twice",
        },
        TomlOptionsTestCase {
            description: "newest_by naming a masked column",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\nnewest_by = \"updated_at\"\nmask = { updated_at = \"hash\" }\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: newest_by column updated_at is masked, so its values cannot say which row is newer",
        },
        TomlOptionsTestCase {
            description: "newest_by with on_duplicate_keys fail",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\nnewest_by = \"updated_at\"\non_duplicate_keys = \"fail\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: newest_by picks the newest of the rows repeating a key, but on_duplicate_keys = \"fail\" keeps none",
        },
        TomlOptionsTestCase {
            description: "null_keys on an append",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"append\"\nnull_keys = \"fail\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: null_keys is for write_strategy = \"merge\"",
        },
        TomlOptionsTestCase {
            description: "checks.max_row_drop on a merge",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\nchecks = { max_row_drop = \"20%\" }\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: only write_strategy = \"replace\" publishes the whole table in one commit",
        },
        TomlOptionsTestCase {
            description: "a masked cursor column",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\ncursor_column = \"updated_at\"\nmask = { updated_at = \"hash\" }\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: mask updated_at: a masked column cannot be the cursor_column or part of the unique_key or replace_key",
        },
        TomlOptionsTestCase {
            description: "a masked unique_key column",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\nmask = { id = \"hash\" }\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: mask id: a masked column cannot be the cursor_column or part of the unique_key or replace_key",
        },
        TomlOptionsTestCase {
            description: "an hmac mask without a mask_key",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"replace\"\nmask = { email = \"hmac\" }\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: mask uses \"hmac\" but the source sets no mask_key",
        },
        TomlOptionsTestCase {
            description: "unnest with a replace_key",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"delete_insert\"\nreplace_key = [\"version\"]\nunnest = [\"items\"]\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: replace_key does not carry child tables yet",
        },
        TomlOptionsTestCase {
            description: "deletes on a delete_insert",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"updated_at\"\ndeletes = \"soft\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: deletes marks the rows whose unique_key the source no longer holds, which needs write_strategy = \"merge\"; delete_insert keeps no key to match",
        },
        TomlOptionsTestCase {
            description: "an unnest path whose parent list is not unnested",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"replace\"\nunnest = [\"items.lines\"]\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: unnest \"items.lines\": its parent list \"items\" must be unnested too",
        },
        TomlOptionsTestCase {
            description: "row_id = false under compat dlt",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"replace\"\ncompat = \"dlt\"\nrow_id = false\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: row_id = false: compat = \"dlt\" gives every row its _dlt_id",
        },
        TomlOptionsTestCase {
            description: "max_depth with neither unnest nor flatten",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"replace\"\nmax_depth = 2\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: max_depth limits the child tables unnest makes and the objects flatten flattens, and this table sets neither",
        },
        TomlOptionsTestCase {
            description: "a column both pinned and parsed",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"replace\"\ncolumns = { updated = \"text\" }\nparse = { updated = \"epoch_ms\" }\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: parse updated: the column is also pinned in columns",
        },
        TomlOptionsTestCase {
            description: "a timezone that is not an IANA zone",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"replace\"\ntimezone = \"Europe/Pari\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: timezone: \"Europe/Pari\" is not an IANA time zone",
        },
        TomlOptionsTestCase {
            description: "a ClickHouse layout on a DuckDB target",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"replace\"\norder_by = [\"id\"]\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: engine, order_by, partition_by, cluster_by, primary_key and table_settings lay out ClickHouse and BigQuery tables",
        },
        TomlOptionsTestCase {
            description: "{primary_key} listed with other columns",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"merge\"\nunique_key = [\"{primary_key}\", \"id\"]\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: public.t: unique_key \"{primary_key}\" stands for the whole primary key, so it can't be listed with other columns",
        },
        TomlOptionsTestCase {
            description: "on_overlapping_files on a database source",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"replace\"\non_overlapping_files = \"last_wins\"\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: on_overlapping_files says which file wins when two files of a files source's folder hold one replace_key value; this source reads no files",
        },
        TomlOptionsTestCase {
            description: "a source's own settings, with no table entries, are checked as every table's",
            target: DUCKDB,
            source: POSTGRES_SOURCE,
            settings: "write_strategy = \"merge\"\n",
            expected_problem: "[sources.s]: write_strategy = \"merge\" needs a unique_key",
        },
        TomlOptionsTestCase {
            description: "a source's merge without a key, when its listed table sets one, parses",
            target: DUCKDB,
            source: POSTGRES_SOURCE,
            settings: "write_strategy = \"merge\"\n[sources.s.tables.\"public.t\"]\nunique_key = [\"id\"]\n",
            expected_problem: "",
        },
        TomlOptionsTestCase {
            description: "a merge on its key with a cursor parses",
            target: DUCKDB,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\ncursor_column = \"updated_at\"\n",
            expected_problem: "",
        },
        TomlOptionsTestCase {
            description: "order = path on an append",
            target: DUCKDB,
            source: FILES_SOURCE,
            settings: "write_strategy = \"append\"\norder = \"path\"\n",
            expected_problem: "[sources.s]: order = \"path\" orders the files a merge or a delete_insert by replace_key applies in turn; append loads them without an order",
        },
        TomlOptionsTestCase {
            description: "on_overlapping_files on a files merge",
            target: DUCKDB,
            source: FILES_SOURCE,
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\non_overlapping_files = \"last_wins\"\n",
            expected_problem: "[sources.s]: on_overlapping_files says which file wins when two files of a files source's folder hold one replace_key value; a merge already lets the last file win each unique_key",
        },
        TomlOptionsTestCase {
            description: "on_overlapping_files on a files append",
            target: DUCKDB,
            source: FILES_SOURCE,
            settings: "write_strategy = \"append\"\non_overlapping_files = \"fail\"\n",
            expected_problem: "[sources.s]: on_overlapping_files says which file wins when two files of a files source's folder hold one replace_key value; append replaces no key values, so remove it",
        },
        TomlOptionsTestCase {
            description: "on_overlapping_files on a files delete_insert by cursor",
            target: DUCKDB,
            source: FILES_SOURCE,
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"day\"\non_overlapping_files = \"last_wins\"\n",
            expected_problem: "[sources.s]: on_overlapping_files says which file wins when two files of a files source's folder hold one replace_key value; this delete_insert replaces a cursor range, not key values, so remove it, or set replace_key",
        },
        TomlOptionsTestCase {
            description: "after_load on a replace",
            target: DUCKDB,
            source: FILES_SOURCE,
            settings: "write_strategy = \"replace\"\nafter_load = \"delete\"\n",
            expected_problem: "[sources.s]: after_load deletes or moves each file once it loads, so it needs write_strategy = \"append\" or \"merge\"",
        },
        TomlOptionsTestCase {
            description: "a files source's replace_key with a cursor",
            target: DUCKDB,
            source: FILES_SOURCE,
            settings: "write_strategy = \"delete_insert\"\nreplace_key = [\"version\"]\npath_columns = \"v_{file_version}.csv\"\ncursor_column = \"file_version\"\n",
            expected_problem: "[sources.s]: cursor_column file_version does not apply to a files source's delete_insert by replace_key",
        },
        TomlOptionsTestCase {
            description: "a files source's coalesced cursor",
            target: DUCKDB,
            source: FILES_SOURCE,
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\ncursor_column = [\"a\", \"b\"]\n",
            expected_problem: "[sources.s]: cursor_column lists several columns to coalesce (coalesce(a, b)), but a files source's cursor is one partition or path column",
        },
        TomlOptionsTestCase {
            description: "replace_key on a files target",
            target: FILES_TARGET,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"delete_insert\"\nreplace_key = [\"version\"]\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: replace_key deletes rows by key in a database table, and a files target replaces whole partition folders",
        },
        TomlOptionsTestCase {
            description: "unnest on a files target merge",
            target: FILES_TARGET,
            source: POSTGRES_TABLE,
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\nunnest = [\"items\"]\n",
            expected_problem: "[sources.s.tables.\"public.t\"]: unnest does not yet write child tables with write_strategy = \"merge\" on a files target",
        },
        TomlOptionsTestCase {
            description: "a postgres_cdc source merging into a files target",
            target: FILES_TARGET,
            source: "kind = \"postgres_cdc\"\nconnection = \"pg\"\ninclude = [\"public.t\"]\n",
            settings: "",
            expected_problem: "[sources.s]: a files target takes kind = \"postgres_cdc\" only with cdc_mode = \"changelog\"",
        },
    ];
    for test_case in test_cases {
        let problems = toml_problems(test_case.target, test_case.source, test_case.settings);
        assert!(
            problems.contains(test_case.expected_problem)
                && problems.is_empty() == test_case.expected_problem.is_empty(),
            "{}: {problems}",
            test_case.description
        );
    }
}

#[test]
fn given_python_table_options_that_do_not_combine_when_parsing_then_parse_reports_each_at_the_python_table()
-> Result<(), String> {
    let test_cases = [
        PythonOptionsTestCase {
            description: "merge without a unique_key",
            settings: "write_strategy = \"merge\"",
            expected_problem: "Python table t (s.py): write_strategy = \"merge\" needs a unique_key",
        },
        PythonOptionsTestCase {
            description: "replace with a cursor",
            settings: "write_strategy = \"replace\"\ncursor_column = \"updated_at\"",
            expected_problem: "Python table t (s.py): write_strategy = \"replace\" reloads the whole table, so it takes no cursor_column",
        },
        PythonOptionsTestCase {
            description: "on_cursor_out_of_range without a cursor",
            settings: "write_strategy = \"append\"\non_cursor_out_of_range = \"skip\"",
            expected_problem: "Python table t (s.py): on_cursor_out_of_range says what a load does with rows outside the cursor window, but the table has no cursor_column",
        },
        PythonOptionsTestCase {
            description: "a merge on {primary_key}, which a Python source never reports",
            settings: "write_strategy = \"merge\"\nunique_key = \"{primary_key}\"",
            expected_problem: "Python table t (s.py): t (kind = \"python\"): unique_key = \"{primary_key}\" merges on the primary key the source reports, and a python source reports none",
        },
        PythonOptionsTestCase {
            description: "a coalesced cursor",
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\ncursor_column = [\"updated_at\", \"created_at\"]",
            expected_problem: "Python table t (s.py): cursor_column lists several columns to coalesce (coalesce(updated_at, created_at)), but a Python table's cursor is one field of the rows it yields",
        },
        PythonOptionsTestCase {
            description: "newest_by naming a unique_key column",
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\nnewest_by = \"id\"",
            expected_problem: "Python table t (s.py): newest_by column id is in the unique_key",
        },
        PythonOptionsTestCase {
            description: "deletes on an append",
            settings: "write_strategy = \"append\"\ndeletes = \"soft\"",
            expected_problem: "Python table t (s.py): deletes marks the rows whose unique_key the source no longer holds",
        },
        PythonOptionsTestCase {
            description: "a merge on its key with a cursor and on_cursor_out_of_range parses",
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\ncursor_column = \"updated_at\"\non_cursor_out_of_range = \"skip\"",
            expected_problem: "",
        },
    ];
    for test_case in test_cases {
        let problems = python_problems(test_case.settings)?;
        assert!(
            problems.contains(test_case.expected_problem)
                && problems.is_empty() == test_case.expected_problem.is_empty(),
            "{}: {problems}",
            test_case.description
        );
    }
    Ok(())
}
