use sqlload_testkit::fake_sheets::main::start_fake_sheets::start_fake_sheets;

use crate::helpers::{duckdb_text, run_async, run_sqlload, words};
use crate::sheets_helpers::{
    duplicated_tabs, edited_tabs, first_tabs, project_text, retyped_tabs, run_with_cache,
    user_project_text,
};
use crate::test_types::{SheetsLoadTestCase, SheetsRefusalTestCase, SheetsUserTestCase};

const LOCATIONS: &str = "SELECT string_agg(Code || ':' || coalesce(Name, '') || ':' || coalesce(CAST(Opened AS VARCHAR), '') || ':' || coalesce(CAST(Active AS VARCHAR), '') || ':' || coalesce(CAST(Capacity AS VARCHAR), '') || ':' || coalesce(CAST(updated_at AS VARCHAR), '') || ':' || coalesce(Ref, ''), ',' ORDER BY Code) || '|' || (SELECT string_agg(column_name || ' ' || data_type, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = 'reference__locations' AND column_name NOT LIKE '\\_sqlload%' ESCAPE '\\') FROM raw.reference__locations";

#[test]
fn given_a_google_sheets_source_when_loading_then_each_tab_or_range_loads_typed_and_replaces_its_table()
-> Result<(), String> {
    run_async(async {
        let fake = start_fake_sheets().await?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let database = folder.path().join("out.duckdb");
        let config = folder.path().join("sqlload.toml");
        let project = project_text(&fake.key_json, &fake.base_url, &database);
        std::fs::write(&config, project).map_err(|error| error.to_string())?;
        let test_cases = [
            SheetsLoadTestCase {
                description: "every tab loads as a table named after it: dates, booleans and whole numbers typed, numbers stored as text kept with their zeros, a ragged row padded with NULLs, a blank row skipped, and the empty tab left out with a warning",
                tabs: first_tabs,
                refused_reads: 0,
                expected_output: "tab \"Empty\" is empty, so it loads no table",
                query: LOCATIONS,
                expected_result: "L1:North:2026-01-15:true:120:2026-11-08 09:30:00:00123,L2:South, Upper:2025-12-31:false:::|code VARCHAR,name VARCHAR,opened DATE,active BOOLEAN,capacity BIGINT,updated_at TIMESTAMP,ref VARCHAR",
            },
            SheetsLoadTestCase {
                description: "a range below title rows takes its header from header_row and stops at the range's last column; the header-only tab is an empty table; each row's _sqlload_source_row is its row in the sheet, skipped blank rows leaving gaps",
                tabs: first_tabs,
                refused_reads: 0,
                expected_output: "",
                query: "SELECT string_agg(Item || ':' || CAST(Qty AS VARCHAR) || ':' || CAST(_sqlload_source_row AS VARCHAR), ',' ORDER BY Item) || '|' || (SELECT count(*) FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = 'report__items' AND column_name = 'note') || '|' || (SELECT count(*) FROM raw.reference__header_only) || '|' || (SELECT string_agg(Code || ':' || CAST(_sqlload_source_row AS VARCHAR), ',' ORDER BY Code) FROM raw.reference__locations) FROM raw.report__items",
                expected_result: "alpha:1.0:4,beta:2.5:5|0|0|L1:2,L2:4",
            },
            SheetsLoadTestCase {
                description: "after the sheet is edited, the next load replaces the table with the sheet as it is now, reading again with a new token when the old one is refused",
                tabs: edited_tabs,
                refused_reads: 1,
                expected_output: "",
                query: "SELECT string_agg(Code || ':' || Name || ':' || CAST(Capacity AS VARCHAR), ',' ORDER BY Code) FROM raw.reference__locations",
                expected_result: "L1:North Gate:150,L3:East:90",
            },
            SheetsLoadTestCase {
                description: "values that fit the table's types keep them: whole quantities in a float column stay float, numbers typed into a column first loaded empty load as its text, and a date in a timestamp column loads at midnight",
                tabs: retyped_tabs,
                refused_reads: 0,
                expected_output: "",
                query: "SELECT string_agg(Item || ':' || CAST(Qty AS VARCHAR), ',' ORDER BY Item) || '|' || (SELECT string_agg(id || ':' || name, ',') FROM raw.reference__header_only) || '|' || (SELECT string_agg(data_type, ',' ORDER BY table_name) FROM information_schema.columns WHERE table_schema = 'raw' AND column_name IN ('qty', 'id')) || '|' || (SELECT CAST(updated_at AS VARCHAR) FROM raw.reference__locations WHERE Code = 'L1') FROM raw.report__items",
                expected_result: "alpha:3.0,beta:4.0|7:seven|VARCHAR,DOUBLE|2026-01-15 00:00:00",
            },
        ];
        for test_case in test_cases {
            fake.place("fake-sheet", (test_case.tabs)());
            fake.state.lock().refuse_requests = test_case.refused_reads;
            let (succeeded, output) = run_sqlload(&config, &["load"], ("", ""))?;
            assert!(
                succeeded && words(&output).contains(&words(test_case.expected_output)),
                "{}:\n{output}",
                test_case.description
            );
            assert_eq!(
                duckdb_text(&database, test_case.query)?,
                test_case.expected_result,
                "{}",
                test_case.description
            );
        }
        let tokens = fake.state.lock().tokens;
        assert!(
            tokens >= 4,
            "a refused token was replaced, so later runs asked for more tokens than runs: {tokens}"
        );
        Ok(())
    })
}

#[test]
fn given_a_sheet_s_date_time_column_when_loading_with_parse_and_tz_then_its_wall_clock_times_load_as_instants()
-> Result<(), String> {
    run_async(async {
        let fake = start_fake_sheets().await?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let database = folder.path().join("out.duckdb");
        let config = folder.path().join("sqlload.toml");
        let project = project_text(&fake.key_json, &fake.base_url, &database);
        let parsed = "[sources.reference.tables.locations]\nparse = { \"Updated At\" = { timestamp = \"iso\", tz = \"Europe/Paris\" } }\n";
        std::fs::write(&config, format!("{project}\n{parsed}"))
            .map_err(|error| error.to_string())?;
        let test_cases = [SheetsLoadTestCase {
            description: "the sheet's 09:30 in Paris loads as 08:30 UTC, typed timestamp_tz, and plan names the column's tz",
            tabs: first_tabs,
            refused_reads: 0,
            expected_output: "",
            query: "SELECT string_agg(Code || ':' || coalesce(CAST(timezone('UTC', updated_at) AS VARCHAR), ''), ',' ORDER BY Code) || '|' || (SELECT data_type FROM information_schema.columns WHERE table_name = 'reference__locations' AND column_name = 'updated_at') FROM raw.reference__locations",
            expected_result: "L1:2026-11-08 08:30:00,L2:|TIMESTAMP WITH TIME ZONE",
        }];
        for test_case in test_cases {
            fake.place("fake-sheet", (test_case.tabs)());
            let (_, plan) = run_sqlload(&config, &["plan"], ("", ""))?;
            let (succeeded, output) = run_sqlload(&config, &["load"], ("", ""))?;
            assert!(
                succeeded
                    && words(&output).contains(&words(test_case.expected_output))
                    && words(&plan).contains(&words("updated_at timestamp_tz from iso; times without an offset in Europe/Paris (its tz)")),
                "{}:\n{plan}\n{output}",
                test_case.description
            );
            assert_eq!(
                duckdb_text(&database, test_case.query)?,
                test_case.expected_result,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_sheet_range_under_infer_types_false_when_loading_then_every_column_but_the_pinned_one_is_text()
-> Result<(), String> {
    run_async(async {
        let fake = start_fake_sheets().await?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let database = folder.path().join("out.duckdb");
        let config = folder.path().join("sqlload.toml");
        let project = project_text(&fake.key_json, &fake.base_url, &database);
        let settings = "[sources.reference.tables.locations]\ninfer_types = false\ncolumns = { Capacity = \"int\" }\n";
        std::fs::write(&config, format!("{project}\n{settings}"))
            .map_err(|error| error.to_string())?;
        let test_cases = [SheetsLoadTestCase {
            description: "dates, booleans and codes load as the text the sheet shows, the pinned Capacity as int, and plan says so",
            tabs: first_tabs,
            refused_reads: 0,
            expected_output: "schema  every column text except columns pinned, objects and lists (infer_types = false)",
            query: "SELECT string_agg(Code || ':' || coalesce(Opened, '') || ':' || coalesce(Active, '') || ':' || coalesce(CAST(Capacity AS VARCHAR), '') || ':' || coalesce(Ref, ''), ',' ORDER BY Code) || '|' || (SELECT string_agg(column_name || ' ' || data_type, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = 'reference__locations' AND column_name NOT LIKE '\\_sqlload%' ESCAPE '\\') FROM raw.reference__locations",
            expected_result: "L1:2026-01-15:TRUE:120:00123,L2:2025-12-31:FALSE::|code VARCHAR,name VARCHAR,opened VARCHAR,active VARCHAR,capacity BIGINT,updated_at VARCHAR,ref VARCHAR",
        }];
        for test_case in test_cases {
            fake.place("fake-sheet", (test_case.tabs)());
            let (_, plan) = run_sqlload(&config, &["plan"], ("", ""))?;
            let (succeeded, output) = run_sqlload(&config, &["load"], ("", ""))?;
            assert!(
                succeeded && words(&plan).contains(&words(test_case.expected_output)),
                "{}:\n{plan}\n{output}",
                test_case.description
            );
            assert_eq!(
                duckdb_text(&database, test_case.query)?,
                test_case.expected_result,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_google_sheets_source_when_planning_then_a_shared_table_name_and_a_repeated_merge_key_are_refused()
-> Result<(), String> {
    run_async(async {
        let fake = start_fake_sheets().await?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let config = folder.path().join("sqlload.toml");
        let base = project_text(
            &fake.key_json,
            &fake.base_url,
            &folder.path().join("out.duckdb"),
        );
        let test_cases = [
            SheetsRefusalTestCase {
                description: "two ranges into one table_name without {table}",
                source: "[sources.many]\nkind = \"google_sheets\"\nwrite_strategy = \"replace\"\nconnection = \"sheets\"\nspreadsheet_id = \"fake-sheet\"\ntable_name = \"fixed\"\nranges = { a = \"Report\", b = \"Locations\" }\n",
                expected_output: "table_name \"fixed\" must contain {table}",
            },
            SheetsRefusalTestCase {
                description: "merge on a key the sheet repeats, under on_duplicate_keys = \"fail\", naming both rows",
                source: "[sources.keyed]\nkind = \"google_sheets\"\ndescription = \"Codes keyed by code.\"\nconnection = \"sheets\"\nspreadsheet_id = \"fake-sheet\"\nranges = { codes = \"Codes\" }\nunique_key = [\"code\"]\nwrite_strategy = \"merge\"\non_duplicate_keys = \"fail\"\n",
                expected_output: "Codes!A2 and Codes!A4 hold the same unique_key (code)",
            },
            SheetsRefusalTestCase {
                description: "a connection with both a service-account key and an OAuth client",
                source: "[connections.mixed]\nkind = \"google_sheets\"\nservice_account_json = '{}'\nclient_id = \"c\"\nclient_secret = \"s\"\n[sources.mixed]\nkind = \"google_sheets\"\nwrite_strategy = \"replace\"\ndescription = \"Mixed.\"\nconnection = \"mixed\"\nspreadsheet_id = \"fake-sheet\"\n",
                expected_output: "connection \"mixed\" sets more than one of service_account_json, service_account_json_base64, service_account_file and client_id; keep one",
            },
            SheetsRefusalTestCase {
                description: "an OAuth client without its secret",
                source: "[connections.half]\nkind = \"google_sheets\"\nclient_id = \"c\"\n[sources.half]\nkind = \"google_sheets\"\nwrite_strategy = \"replace\"\ndescription = \"Half.\"\nconnection = \"half\"\nspreadsheet_id = \"fake-sheet\"\n",
                expected_output: "connection \"half\" sets client_id without client_secret",
            },
        ];
        fake.place("fake-sheet", duplicated_tabs());
        for test_case in test_cases {
            std::fs::write(&config, format!("{base}\n{}", test_case.source))
                .map_err(|error| error.to_string())?;
            let (succeeded, output) = run_sqlload(&config, &["plan"], ("", ""))?;
            assert!(
                !succeeded && words(&output).contains(&words(test_case.expected_output)),
                "{}:\n{output}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_google_user_connection_when_loading_then_its_refresh_token_reads_and_a_refused_one_fails_for_good()
-> Result<(), String> {
    run_async(async {
        let fake = start_fake_sheets().await?;
        fake.place("fake-sheet", first_tabs());
        let test_cases = [
            SheetsUserTestCase {
                description: "the connection's refresh token is exchanged for an access token that reads the sheet",
                refresh: Some("refresh-own"),
                revoked: &[],
                expected_code: 0,
                expected_output: "\"status\": \"loaded\"",
                expected_grant: Some("refresh_token"),
            },
            SheetsUserTestCase {
                description: "a revoked refresh token fails with exit 1 and retryable false, saying how to get a new one",
                refresh: Some("refresh-own"),
                revoked: &["refresh-own"],
                expected_code: 1,
                expected_output: "Google refused the refresh_token of connection \\\"sheets\\\" (invalid_grant: Token has been expired or revoked.)",
                expected_grant: Some("refresh_token"),
            },
            SheetsUserTestCase {
                description: "with no refresh token and nothing cached, a --json run asks nobody to sign in and fails at once, saying how to",
                refresh: None,
                revoked: &[],
                expected_code: 1,
                expected_output: "signing in in the browser needs a terminal; run `sqlload plan` from a terminal once to sign in",
                expected_grant: None,
            },
        ];
        for test_case in test_cases {
            let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
            let config = folder.path().join("sqlload.toml");
            let project = user_project_text(
                (&fake.base_url, &fake.token_url),
                test_case.refresh,
                &folder.path().join("out.duckdb"),
            );
            std::fs::write(&config, project).map_err(|error| error.to_string())?;
            {
                let mut state = fake.state.lock();
                state.grants.clear();
                state.revoked_refresh_tokens = test_case
                    .revoked
                    .iter()
                    .map(|token| (*token).to_owned())
                    .collect();
            }
            let (code, output) = run_with_cache(&config, &["load", "--json"], folder.path())?;
            let grants = fake.state.lock().grants.clone();
            assert!(
                code == Some(test_case.expected_code)
                    && words(&output).contains(&words(test_case.expected_output))
                    && output
                        .contains(&format!("\"retryable\": {}", test_case.expected_code == 75))
                    && grants.is_empty() == test_case.expected_grant.is_none()
                    && grants
                        .iter()
                        .all(|grant| Some(grant.as_str()) == test_case.expected_grant),
                "{}: exit {code:?}, grants {grants:?}:\n{output}",
                test_case.description
            );
        }
        Ok(())
    })
}
