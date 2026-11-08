use sqlload::file_store::main::write_file::write_file;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::models::PlanOptions;
use sqlload_testkit::clickhouse::main::start_clickhouse::start_clickhouse;
use sqlload_testkit::seaweedfs::main::create_bucket::create_bucket;
use sqlload_testkit::seaweedfs::main::start_seaweedfs::start_seaweedfs;

use crate::clickhouse_helpers::{
    PULL_BUCKET, clickhouse_files_project, clocks_parquet, plan_apply_load_with, pull_folder,
    put_day_parts, put_events, read_text, run_clickhouse_stops, select_only, shared_s3_endpoint,
};
use crate::helpers::{described_project, run_async};
use crate::test_types::{EventsFile, FilesPullTestCase, StoppedLoadTestCase};

const EVENTS: &str = "SELECT toString(count()) || '|' || toString(countIf(_sqlload_source_row != id - if(id > 9000, 9000, 0))) || '|' || arrayStringConcat(groupUniqArray(hex(raw)), ',') || '|' || toString(countIf(note IS NULL)) || '|' || arrayStringConcat(arraySort(groupUniqArray(toString(day) || '@' || _sqlload_source_file)), ',') FROM raw.events";

#[test]
fn given_parquet_in_s3_when_loading_into_clickhouse_then_clickhouse_reads_the_files_itself_exactly()
-> Result<(), String> {
    run_async(async {
        let (clickhouse, seaweedfs) = tokio::try_join!(start_clickhouse(), start_seaweedfs())?;
        create_bucket(&seaweedfs, PULL_BUCKET).await?;
        let endpoint = shared_s3_endpoint(&seaweedfs).await?;
        let folder = pull_folder(&endpoint, "pulls")?;
        write_file(&folder, "clocks/c.parquet", clocks_parquet()?)
            .await
            .map_err(|error| error.to_string())?;
        let project = described_project(&clickhouse_files_project(
            &endpoint,
            clickhouse.port,
            ("raw", "pulls"),
        ))
        .await?;
        let destinations = connect_targets(&project)
            .await
            .map_err(|error| error.to_string())?;
        let serving = destinations
            .get("dev")
            .ok_or_else(|| "no ClickHouse destination".to_owned())?;
        let test_cases = [
            FilesPullTestCase {
                description: "ClickHouse reads Parquet in S3 itself: each file's rows numbered in stored order across row groups, fixed-size values kept whole, a column a file lacks NULL, days from the folders, and a key with a space read",
                files: vec![
                    EventsFile {
                        key: "events/day=2026-10-30/part one.parquet",
                        ids: 1..=5000,
                        note: Some("a"),
                    },
                    EventsFile {
                        key: "events/day=2026-11-01/b.parquet",
                        ids: 9001..=9002,
                        note: None,
                    },
                ],
                expected_plan: "route   ClickHouse reads directly from s3://example-bucket-test/pulls/events/ (nothing through the loader)",
                check_query: EVENTS,
                expected_result: "5002|0|00010000|2|2026-10-30@s3://example-bucket-test/pulls/events/day=2026-10-30/part one.parquet,2026-11-01@s3://example-bucket-test/pulls/events/day=2026-11-01/b.parquet",
            },
            FilesPullTestCase {
                description: "a time column, which ClickHouse reads from Parquet as a timestamp, goes through SQLLoad and loads as its text",
                files: Vec::new(),
                expected_plan: "route   read parquet into Arrow -> ClickHouse takes Arrow",
                check_query: "SELECT arrayStringConcat(groupArray(t), ',') FROM raw.clocks",
                expected_result: "13:04:05.123456",
            },
            FilesPullTestCase {
                description: "a new file in a folder replaces that day's rows with every file's, once",
                files: vec![EventsFile {
                    key: "events/day=2026-11-01/c.parquet",
                    ids: 9003..=9003,
                    note: Some("c"),
                }],
                expected_plan: "",
                check_query: "SELECT toString(count()) || '|' || arrayStringConcat(arraySort(groupArray(toString(id))), ',') FROM raw.events WHERE day = '2026-11-01'",
                expected_result: "3|9001,9002,9003",
            },
            FilesPullTestCase {
                description: "the other day's rows are kept",
                files: Vec::new(),
                expected_plan: "",
                check_query: "SELECT toString(count()) FROM raw.events",
                expected_result: "5003",
            },
        ];
        for test_case in test_cases {
            put_events(&folder, &test_case.files).await?;
            let plan = plan_apply_load_with(&project, &destinations, &PlanOptions::default())
                .await
                .map_err(|error| format!("{}: {error}", test_case.description))?;
            assert!(
                plan.contains(test_case.expected_plan),
                "{}: {plan}",
                test_case.description
            );
            assert_eq!(
                read_text(serving, test_case.check_query).await?,
                test_case.expected_result,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_pull_stopped_at_every_statement_when_it_runs_again_then_the_table_was_never_half_published_and_converges()
-> Result<(), String> {
    run_async(async {
        let (clickhouse, seaweedfs) = tokio::try_join!(start_clickhouse(), start_seaweedfs())?;
        create_bucket(&seaweedfs, PULL_BUCKET).await?;
        let endpoint = shared_s3_endpoint(&seaweedfs).await?;
        let folder = pull_folder(&endpoint, "stops")?;
        put_events(
            &folder,
            &[EventsFile {
                key: "events/day=2026-10-30/kept.parquet",
                ids: 50..=50,
                note: Some("kept"),
            }],
        )
        .await?;
        let project = described_project(&clickhouse_files_project(
            &endpoint,
            clickhouse.port,
            ("stops", "stops"),
        ))
        .await?;
        let destinations = connect_targets(&project)
            .await
            .map_err(|error| error.to_string())?;
        let set_state = async |days: &'static str| put_day_parts(&folder, days).await;
        let test_cases = [StoppedLoadTestCase {
            description: "the window rereads from the day before the newest, so each run rebuilds October (keeping the 30th's rows) and November at once",
            table: "events",
            options: select_only("events"),
            query: "SELECT arrayStringConcat(arraySort(groupArray(toString(id))), ',') FROM stops.events",
            states: [
                ("2026-10-31=100,2026-11-01=200", "100,200,50"),
                ("2026-10-31=101,2026-11-01=201", "101,201,50"),
            ],
            expected_minimum_stops: 8,
        }];
        for test_case in &test_cases {
            let unstopped =
                run_clickhouse_stops(&project, &destinations, &set_state, ("stops", test_case))
                    .await
                    .map_err(|error| format!("{}: {error}", test_case.description))?;
            assert!(
                unstopped.is_some_and(|stops| stops >= test_case.expected_minimum_stops),
                "{}: a run finished unstopped after {unstopped:?} stop points",
                test_case.description
            );
        }
        Ok(())
    })
}
