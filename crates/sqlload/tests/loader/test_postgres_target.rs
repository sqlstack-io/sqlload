use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::models::PlanOptions;
use sqlload_testkit::postgres::constants::PASSWORD;
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;
use sqlload_testkit::postgres::main::start_postgres_tls::start_postgres_tls;

use crate::helpers::{described_project, run_async, run_merge_case, run_sqlload, words};
use crate::postgres_helpers::{load_once, project_text, text};
use crate::test_files_to_duckdb::{
    MERGE_CHECK, MERGE_FAIL_SETTINGS, MERGE_SETTINGS, MERGE_SORT_SETTINGS,
};
use crate::test_types::{FilesMergeTestCase, MergeFile, MergeStep};
use crate::test_types::{PostgresTargetTestCase, TargetTlsTestCase};

const SETUP: &str = "
CREATE TABLE public.accounts (id int8 PRIMARY KEY, name text, balance numeric(12,2), tags text[], updated_at timestamptz NOT NULL);
CREATE TABLE public.events (event_id int8, kind text, at timestamptz NOT NULL);
CREATE TABLE public.visits (visit_id int8, at timestamptz NOT NULL);
CREATE TABLE public.stores (code text, city text, extra int);
INSERT INTO public.accounts VALUES
  (1, 'Acme', 10.50, '{a,b}', '2026-11-01 10:00:00+00'),
  (2, 'Beta', 20.00, NULL, '2026-11-02 10:00:00+00');
INSERT INTO public.events VALUES (1, 'open', '2026-11-01 09:00:00+00'), (2, 'close', '2026-11-02 09:00:00+00');
INSERT INTO public.visits VALUES (1, '2026-11-01 09:00:00+00');
INSERT INTO public.stores VALUES ('L1', 'Leeds', 1), ('Y1', 'York', 2);
CREATE TABLE public.keys_src (id int8, k text COLLATE \"und-x-icu\" NOT NULL);
CREATE TABLE public.keys_dst (id int8, k text NOT NULL);
INSERT INTO public.keys_src VALUES (1, 'B1'), (2, 'a9');
INSERT INTO public.keys_dst VALUES (1, 'B1'), (2, 'a9');
CREATE SCHEMA mart;
CREATE TABLE mart.stores (id bigserial PRIMARY KEY, code varchar(5) NOT NULL, city text, note text DEFAULT 'none');
CREATE VIEW mart.store_codes AS SELECT code, city FROM mart.stores;
INSERT INTO mart.stores (code, city) VALUES ('OLD', 'Nowhere');
CREATE ROLE reader;
GRANT SELECT ON mart.stores TO reader;
";

#[test]
fn given_postgres_tables_when_loading_repeatedly_into_a_postgres_target_then_each_strategy_keeps_the_table_right()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(SETUP)
            .await
            .map_err(|error| error.to_string())?;
        let project = described_project(&project_text(postgres.port)).await?;
        let destinations = connect_targets(&project)
            .await
            .map_err(|error| error.to_string())?;
        let test_cases = [
            PostgresTargetTestCase {
                description: "the first load creates the new tables with the load's metadata, and a unique index for merge",
                source_change: "",
                options: PlanOptions::default(),
                expected_load: "ok",
                check_query: "SELECT string_agg(indexdef, ',') FROM pg_indexes WHERE schemaname = 'mart' AND tablename = 'accounts'",
                expected_result: "CREATE UNIQUE INDEX accounts_sqlload_key ON mart.accounts USING btree (id)",
            },
            PostgresTargetTestCase {
                description: "an array arrives as jsonb, and every column has its portable type",
                source_change: "",
                options: PlanOptions::default(),
                expected_load: "ok",
                check_query: "SELECT string_agg(column_name || ' ' || data_type, ',' ORDER BY ordinal_position) || ' / ' || (SELECT tags::text FROM mart.accounts WHERE id = 1) FROM information_schema.columns WHERE table_schema = 'mart' AND table_name = 'accounts'",
                expected_result: "id bigint,name text,balance numeric,tags jsonb,updated_at timestamp with time zone,_sqlload_load_id text,_sqlload_loaded_at timestamp with time zone / [\"a\", \"b\"]",
            },
            PostgresTargetTestCase {
                description: "replace into a table SQLLoad did not create loads the columns it has, giving the rest their defaults and sequence, and adds none",
                source_change: "",
                options: PlanOptions::default(),
                expected_load: "ok",
                check_query: "SELECT string_agg(code || '@' || city || '@' || note || '@' || (id > 1), ',' ORDER BY code) || ' / ' || (SELECT count(*) FROM information_schema.columns WHERE table_schema = 'mart' AND table_name = 'stores') FROM mart.stores",
                expected_result: "L1@Leeds@none@true,Y1@York@none@true / 4",
            },
            PostgresTargetTestCase {
                description: "merge with a cursor upserts changed rows and keeps the rest",
                source_change: "UPDATE public.accounts SET balance = 99.99, updated_at = '2026-11-03 10:00:00+00' WHERE id = 2; INSERT INTO public.accounts VALUES (3, 'Gamma', 1.00, '{}', '2026-11-03 11:00:00+00');",
                options: PlanOptions::default(),
                expected_load: "ok",
                check_query: "SELECT string_agg(id || '=' || balance, ',' ORDER BY id) FROM mart.accounts",
                expected_result: "1=10.50,2=99.99,3=1.00",
            },
            PostgresTargetTestCase {
                description: "delete_insert replaces the rows from the cursor onwards without duplicates",
                source_change: "INSERT INTO public.events VALUES (3, 'reopen', '2026-11-02 09:00:00+00');",
                options: PlanOptions::default(),
                expected_load: "ok",
                check_query: "SELECT string_agg(event_id || ':' || kind, ',' ORDER BY event_id) FROM mart.events",
                expected_result: "1:open,2:close,3:reopen",
            },
            PostgresTargetTestCase {
                description: "append adds only the rows after the cursor",
                source_change: "INSERT INTO public.visits VALUES (2, '2026-11-02 09:00:00+00');",
                options: PlanOptions::default(),
                expected_load: "ok",
                check_query: "SELECT string_agg(CAST(visit_id AS text), ',' ORDER BY visit_id) FROM mart.visits",
                expected_result: "1,2",
            },
            PostgresTargetTestCase {
                description: "replace mirrors deletes in place: the dependent view, the grant and the primary key stay",
                source_change: "DELETE FROM public.stores WHERE code = 'Y1';",
                options: PlanOptions::default(),
                expected_load: "ok",
                check_query: "SELECT (SELECT string_agg(code, ',') FROM mart.store_codes) || ' ' || has_table_privilege('reader', 'mart.stores', 'SELECT') || ' ' || (SELECT count(*) FROM pg_indexes WHERE schemaname = 'mart' AND tablename = 'stores')",
                expected_result: "L1 true 1",
            },
            PostgresTargetTestCase {
                description: "a value too long for the existing table's column fails the replace, and the table keeps its rows",
                source_change: "INSERT INTO public.stores VALUES ('TOOLONG', 'Hull', 3);",
                options: PlanOptions {
                    batch_bytes: Some(1),
                    ..PlanOptions::default()
                },
                expected_load: "Postgres query failed: value too long for type character varying(5) (SQLSTATE 22001)",
                check_query: "SELECT string_agg(code, ',' ORDER BY code) FROM mart.stores",
                expected_result: "L1",
            },
            PostgresTargetTestCase {
                description: "--reload is refused rather than dropping and recreating a table",
                source_change: "DELETE FROM public.stores WHERE code = 'TOOLONG';",
                options: PlanOptions {
                    select: vec!["stores".to_owned()],
                    reload: true,
                    ..PlanOptions::default()
                },
                expected_load: "--reload drops and recreates the table, which SQLLoad never does on a Postgres target",
                check_query: "SELECT string_agg(code, ',' ORDER BY code) FROM mart.store_codes",
                expected_result: "L1",
            },
            PostgresTargetTestCase {
                description: "merge into a table without a unique index on its key is refused at plan",
                source_change: "DROP INDEX mart.accounts_sqlload_key;",
                options: PlanOptions {
                    select: vec!["accounts".to_owned()],
                    ..PlanOptions::default()
                },
                expected_load: "merge upserts with INSERT ... ON CONFLICT (id), which needs a valid, non-deferrable unique index or constraint on exactly those columns, and mart.accounts has none",
                check_query: "SELECT CAST(count(*) AS text) FROM mart.accounts",
                expected_result: "3",
            },
            PostgresTargetTestCase {
                description: "a load leaves no session or temporary staging table behind",
                source_change: "CREATE UNIQUE INDEX accounts_sqlload_key ON mart.accounts (id);",
                options: PlanOptions::default(),
                expected_load: "ok",
                check_query: "SELECT CAST(count(*) AS text) FROM pg_class WHERE relpersistence = 't' AND relname LIKE '%sqlload_staging%'",
                expected_result: "0",
            },
            PostgresTargetTestCase {
                description: "a table of the user's own named like an old staging table survives a load, since Postgres staging tables are temporary",
                source_change: "CREATE TABLE mart.stores__sqlload_staging_20200101000000_keep (x int);",
                options: PlanOptions::default(),
                expected_load: "ok",
                check_query: "SELECT CAST(count(*) AS text) FROM pg_tables WHERE schemaname = 'mart' AND tablename = 'stores__sqlload_staging_20200101000000_keep'",
                expected_result: "1",
            },
            PostgresTargetTestCase {
                description: "replace into a table another table references with ON DELETE CASCADE is refused at plan, so the other table keeps its rows",
                source_change: "ALTER TABLE mart.stores ADD CONSTRAINT stores_code_key UNIQUE (code); CREATE TABLE mart.store_orders (code varchar(5) REFERENCES mart.stores (code) ON DELETE CASCADE); INSERT INTO mart.store_orders VALUES ('L1');",
                options: PlanOptions {
                    select: vec!["stores".to_owned()],
                    ..PlanOptions::default()
                },
                expected_load: "mart.store_orders references mart.stores (store_orders_code_fkey), and its ON DELETE CASCADE deletes its rows when replace deletes the rows it replaces",
                check_query: "SELECT (SELECT string_agg(code, ',') FROM mart.store_orders) || ' ' || (SELECT string_agg(code, ',') FROM mart.stores)",
                expected_result: "L1 L1",
            },
            PostgresTargetTestCase {
                description: "a column of a table SQLLoad did not create that would round the source's values is refused at plan",
                source_change: "DROP TABLE mart.store_orders; ALTER TABLE public.stores ADD COLUMN price numeric(8,2); UPDATE public.stores SET price = 1.25; ALTER TABLE mart.stores ADD COLUMN price numeric(8,1);",
                options: PlanOptions {
                    select: vec!["stores".to_owned()],
                    ..PlanOptions::default()
                },
                expected_load: "price is decimal(8,1) in the table, which SQLLoad did not create and does not alter, and decimal(8,2) values would not all load exactly there",
                check_query: "SELECT string_agg(code || '@' || coalesce(CAST(price AS text), 'none'), ',') FROM mart.stores",
                expected_result: "L1@none",
            },
            PostgresTargetTestCase {
                description: "replace into a table that references itself with ON DELETE CASCADE runs, since replace deletes every row and touches no other table",
                source_change: "ALTER TABLE mart.stores DROP COLUMN price; ALTER TABLE mart.stores ADD COLUMN price numeric(8,2), ADD COLUMN parent varchar(5) REFERENCES mart.stores (code) ON DELETE CASCADE; UPDATE mart.stores SET parent = code;",
                options: PlanOptions {
                    select: vec!["stores".to_owned()],
                    ..PlanOptions::default()
                },
                expected_load: "ok",
                check_query: "SELECT string_agg(code || '@' || price || '@' || coalesce(parent, 'none'), ',') FROM mart.stores",
                expected_result: "L1@1.25@none",
            },
            PostgresTargetTestCase {
                description: "append into a table without its cursor column is refused with advice a Postgres target can follow, not --reload",
                source_change: "ALTER TABLE mart.visits DROP COLUMN at;",
                options: PlanOptions {
                    select: vec!["visits".to_owned()],
                    ..PlanOptions::default()
                },
                expected_load: "the table has no at column yet, so this load would read every row again and duplicate the rows already there; SQLLoad never drops and recreates a Postgres table (--reload), so add the column with ALTER TABLE",
                check_query: "SELECT CAST(count(*) AS text) FROM mart.visits",
                expected_result: "2",
            },
            PostgresTargetTestCase {
                description: "a text cursor compares by code point on both sides whatever their collations ('B1' < 'a9'): an ICU source column is filtered bytewise, and an ICU target column's maximum is read bytewise, so neither append reads a row again",
                source_change: "ALTER TABLE mart.keys_dst ALTER COLUMN k TYPE text COLLATE \"und-x-icu\"; INSERT INTO public.keys_src VALUES (3, 'c1'); INSERT INTO public.keys_dst VALUES (3, 'c1');",
                options: PlanOptions {
                    select: vec!["keys_src".to_owned(), "keys_dst".to_owned()],
                    ..PlanOptions::default()
                },
                expected_load: "ok",
                check_query: "SELECT (SELECT string_agg(k, ',' ORDER BY id) FROM mart.keys_src) || ' / ' || (SELECT string_agg(k, ',' ORDER BY id) FROM mart.keys_dst)",
                expected_result: "B1,a9,c1 / B1,a9,c1",
            },
        ];
        for test_case in test_cases {
            client
                .batch_execute(test_case.source_change)
                .await
                .map_err(|error| format!("{}: {error}", test_case.description))?;
            let loaded = load_once(&project, &destinations, &test_case.options)
                .await
                .unwrap_or_else(|error| error);
            assert!(
                words(&loaded).contains(&words(test_case.expected_load)),
                "{}: {loaded}",
                test_case.description
            );
            assert_eq!(
                text(&client, test_case.check_query).await?,
                test_case.expected_result,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_tls_only_server_when_loading_into_a_postgres_target_then_the_target_checks_its_certificate_as_a_source_does()
-> Result<(), String> {
    let test_cases = [
        TargetTlsTestCase {
            description: "verify-full checks the CA and the host name",
            settings: "sslmode = \"verify-full\"\nsslrootcert = \"FILES/ca.pem\"",
            expected_debug: "TLS, CA and host name checked",
            expected_result: "L1,Y1",
        },
        TargetTlsTestCase {
            description: "require encrypts without checking the certificate",
            settings: "sslmode = \"require\"",
            expected_debug: "TLS, certificate not checked",
            expected_result: "L1,Y1",
        },
    ];
    for test_case in test_cases {
        run_async(async {
            let tls = start_postgres_tls().await?;
            let client = connect(&tls.postgres).await?;
            client
                .batch_execute(SETUP)
                .await
                .map_err(|error| error.to_string())?;
            let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
            let config = folder.path().join("sqlload.toml");
            let settings = test_case
                .settings
                .replace("FILES", &tls.files.path().display().to_string());
            let project = project_text(tls.postgres.port).replacen(
                "host = \"127.0.0.1\"",
                &format!("host = \"localhost\"\n{settings}"),
                1,
            );
            std::fs::write(&config, project).map_err(|error| error.to_string())?;
            let debug = run_sqlload(&config, &["debug"], ("", ""))?;
            let loaded = run_sqlload(&config, &["load", "--select", "stores"], ("", ""))?;
            let target_line = debug
                .1
                .lines()
                .find(|line| line.contains("target mart"))
                .unwrap_or_default()
                .to_owned();
            assert_eq!(
                (
                    target_line.contains(test_case.expected_debug),
                    loaded.0,
                    text(
                        &client,
                        "SELECT string_agg(code, ',' ORDER BY code) FROM mart.stores"
                    )
                    .await?
                ),
                (true, true, test_case.expected_result.to_owned()),
                "{}:\n{}\n{}",
                test_case.description,
                debug.1,
                loaded.1
            );
            Ok(())
        })?;
    }
    Ok(())
}

#[test]
fn given_csv_files_when_merging_into_postgres_then_the_newest_modified_file_wins_and_each_file_merges_once()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        let target = format!(
            "kind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"sqlload\"\npassword = \"{PASSWORD}\"\ndatabase = \"app\"",
            postgres.port
        );
        let test_cases = [
            FilesMergeTestCase {
                description: "a key repeated across files ends with the newest modified file's value, whatever the names",
                settings: MERGE_SETTINGS,
                steps: &[MergeStep {
                    before: &[],
                    files: &[
                        MergeFile {
                            key: "c.csv",
                            contents: "id,v\n1,c\n2,c\n",
                            modified_secs: 1_000,
                        },
                        MergeFile {
                            key: "a.csv",
                            contents: "id,v\n1,a\n3,a\n",
                            modified_secs: 2_000,
                        },
                        MergeFile {
                            key: "b.csv",
                            contents: "id,v\n1,b\n",
                            modified_secs: 3_000,
                        },
                    ],
                    expected_error: "",
                    loads: true,
                    expected_plan: "3 files, one commit each, oldest modified first (ties by path)",
                }],
                check_query: MERGE_CHECK,
                expected_result: "1:b,2:c,3:a|3",
            },
            FilesMergeTestCase {
                description: "files modified at the same time merge in path order",
                settings: MERGE_SETTINGS,
                steps: &[MergeStep {
                    before: &[],
                    files: &[
                        MergeFile {
                            key: "b.csv",
                            contents: "id,v\n1,b\n",
                            modified_secs: 1_000,
                        },
                        MergeFile {
                            key: "a.csv",
                            contents: "id,v\n1,a\n",
                            modified_secs: 1_000,
                        },
                    ],
                    expected_error: "",
                    loads: true,
                    expected_plan: "",
                }],
                check_query: MERGE_CHECK,
                expected_result: "1:b|2",
            },
            FilesMergeTestCase {
                description: "by default a file repeating a key merges its last row of that key",
                settings: MERGE_SETTINGS,
                steps: &[MergeStep {
                    before: &[],
                    files: &[MergeFile {
                        key: "a.csv",
                        contents: "id,v\n5,b\n6,c\n5,a\n",
                        modified_secs: 1_000,
                    }],
                    loads: true,
                    expected_error: "",
                    expected_plan: "merge (unique_key: id)",
                }],
                check_query: MERGE_CHECK,
                expected_result: "5:a,6:c|1",
            },
            FilesMergeTestCase {
                description: "with dedup_sort a file repeating a key merges the row that sorts first, and plan warns that a later file still overwrites it",
                settings: MERGE_SORT_SETTINGS,
                steps: &[MergeStep {
                    before: &[],
                    files: &[
                        MergeFile {
                            key: "a.csv",
                            contents: "id,v\n5,b\n5,a\n5,c\n",
                            modified_secs: 1_000,
                        },
                        MergeFile {
                            key: "b.csv",
                            contents: "id,v\n6,z\n",
                            modified_secs: 2_000,
                        },
                    ],
                    loads: true,
                    expected_error: "",
                    expected_plan: "dedup_sort ranks repeated keys within one commit, and this table commits once per file (2 files)",
                }],
                check_query: MERGE_CHECK,
                expected_result: "5:a,6:z|2",
            },
            FilesMergeTestCase {
                description: "with on_duplicate_keys = \"fail\" a file repeating a key no row has yet fails before it merges, naming the key",
                settings: MERGE_FAIL_SETTINGS,
                steps: &[MergeStep {
                    before: &[],
                    files: &[MergeFile {
                        key: "a.csv",
                        contents: "id,v\n5,a\n5,b\n6,c\n",
                        modified_secs: 1_000,
                    }],
                    loads: true,
                    expected_error: "drop/a.csv: the file repeats its unique_key: 1 value(s) of (id) appear in more than one row, such as (5)",
                    expected_plan: "",
                }],
                check_query: "SELECT CAST(count(*) AS VARCHAR) FROM raw.scores",
                expected_result: "0",
            },
            FilesMergeTestCase {
                description: "a failure in the second file keeps the first committed, names the file, and a rerun merges from it without merging the first again; a run with nothing new merges nothing",
                settings: MERGE_FAIL_SETTINGS,
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[
                            MergeFile {
                                key: "a.csv",
                                contents: "id,v\n1,a\n2,a\n",
                                modified_secs: 1_000,
                            },
                            MergeFile {
                                key: "b.csv",
                                contents: "id,v\n1,b\n1,bb\n",
                                modified_secs: 2_000,
                            },
                            MergeFile {
                                key: "c.csv",
                                contents: "id,v\n2,c\n",
                                modified_secs: 3_000,
                            },
                        ],
                        expected_error: "drop/b.csv: the file repeats its unique_key: 1 value(s) of (id) appear in more than one row, such as (1)",
                        loads: true,
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "b.csv",
                            contents: "id,v\n1,b\n",
                            modified_secs: 2_000,
                        }],
                        expected_error: "",
                        loads: true,
                        expected_plan: "2 files, one commit each",
                    },
                    MergeStep {
                        before: &[],
                        files: &[],
                        expected_error: "",
                        loads: true,
                        expected_plan: "",
                    },
                ],
                check_query: MERGE_CHECK,
                expected_result: "1:b,2:c|3",
            },
            FilesMergeTestCase {
                description: "a merged file rewritten in place merges again, as the newest",
                settings: MERGE_SETTINGS,
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[
                            MergeFile {
                                key: "a.csv",
                                contents: "id,v\n1,a\n",
                                modified_secs: 1_000,
                            },
                            MergeFile {
                                key: "b.csv",
                                contents: "id,v\n1,b\n",
                                modified_secs: 2_000,
                            },
                        ],
                        expected_error: "",
                        loads: true,
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "a.csv",
                            contents: "id,v\n1,a2\n",
                            modified_secs: 3_000,
                        }],
                        expected_error: "",
                        loads: true,
                        expected_plan: "1 file, one commit each",
                    },
                ],
                check_query: MERGE_CHECK,
                expected_result: "1:a2|3",
            },
        ];
        for test_case in &test_cases {
            client
                .batch_execute("DROP SCHEMA IF EXISTS raw CASCADE")
                .await
                .map_err(|error| error.to_string())?;
            let result = run_merge_case((&target, "raw"), test_case).await?;
            assert_eq!(
                result.as_deref(),
                Some(test_case.expected_result),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}
