use std::collections::BTreeSet;
use std::time::Duration;

use sqlload::time_text::main::now_ms::now_ms;
use sqlload::time_text::main::utc_date::utc_date;
use sqlload_testkit::configs::models::S3Settings;
use sqlload_testkit::fake_snowflake::models::LoadOutcome;
use sqlload_testkit::harness::main::read_metrics::read_metrics;
use sqlload_testkit::harness::main::start_exporter::start_exporter;
use sqlload_testkit::harness::main::start_stack::start_stack;
use sqlload_testkit::harness::main::stop_exporter::stop_exporter;
use sqlload_testkit::harness::main::wait_for_commits::wait_for_commits;
use sqlload_testkit::harness::main::wait_for_rows::wait_for_rows;
use sqlload_testkit::records::main::distinct_positions::distinct_positions;
use sqlload_testkit::records::main::order_values::order_values;
use sqlload_testkit::redpanda::main::produce_records::produce_records;

use crate::helpers::{ORDERS_GROUP, PARTITIONS, committed, run_async, s3_test_config};
use crate::test_types::{
    S3LayoutTestCase, S3LoadFailureTestCase, S3LoadReportTestCase, S3PauseTestCase,
    S3RetryTestCase, S3ScanFailureTestCase, S3ShutdownTestCase, S3SkipTestCase, S3SlowLoadTestCase,
};

const SHARED_TABLE: &str = "[batching]\nmax_age = \"1s\"\nmax_rows = 100\n[[naming.rules]]\nmatch = \"orders.{kind}\"\ntable = \"ORDERS\"\n";
const TOPICS: [&str; 2] = ["orders.created", "orders.cancelled"];
const WAIT: Duration = Duration::from_secs(90);
const LOAD_ERROR: &str = "Numeric value 'abc' is not recognized";

#[test]
fn given_two_topics_sharing_a_table_when_exporting_to_s3_then_files_follow_the_path_template()
-> Result<(), String> {
    run_async(async {
        let test_cases = [
            S3LayoutTestCase {
                description: "insertFiles loads one file per topic per batch under Hive date folders",
                staging: "",
                s3: "",
                records_per_topic: 150,
                expected_folders: &[
                    "fake-bucket/exports/orders.created/date=",
                    "fake-bucket/exports/orders.cancelled/date=",
                ],
                expected_after_date: "/",
                expected_extension: ".json.gz",
                expected_rows: 300,
            },
            S3LayoutTestCase {
                description: "COPY loads table folders with hourly partitions",
                staging: "load = \"copy\"",
                s3: "path = \"{table}/date={date}/hour={hour}/\"",
                records_per_topic: 150,
                expected_folders: &["fake-bucket/exports/ORDERS/date="],
                expected_after_date: "/hour=",
                expected_extension: ".json.gz",
                expected_rows: 300,
            },
            S3LayoutTestCase {
                description: "Parquet files load through insertFiles with every row",
                staging: "format = \"parquet\"",
                s3: "",
                records_per_topic: 150,
                expected_folders: &[
                    "fake-bucket/exports/orders.created/date=",
                    "fake-bucket/exports/orders.cancelled/date=",
                ],
                expected_after_date: "/",
                expected_extension: ".parquet",
                expected_rows: 300,
            },
        ];
        for test_case in &test_cases {
            let stack = start_stack().await?;
            for topic in TOPICS {
                produce_records(
                    &stack.redpanda.brokers,
                    topic,
                    PARTITIONS,
                    &order_values(0, test_case.records_per_topic),
                )
                .await?;
            }
            let settings = S3Settings {
                staging: test_case.staging,
                s3: test_case.s3,
                extra: SHARED_TABLE,
            };
            let running = start_exporter(s3_test_config(&stack, "orders.*", settings)?);
            wait_for_rows(&stack.fake, "ORDERS", test_case.expected_rows).await?;
            stop_exporter(running).await?;
            let rows = stack.fake.rows("ORDERS");
            assert_eq!(
                rows.len(),
                test_case.expected_rows,
                "{}",
                test_case.description
            );
            assert_eq!(
                distinct_positions(&rows),
                test_case.expected_rows,
                "{}",
                test_case.description
            );
            let today = utc_date(now_ms());
            let keys = stack.fake.object_keys();
            let folders: BTreeSet<&str> = keys
                .iter()
                .filter_map(|key| {
                    test_case.expected_folders.iter().copied().find(|folder| {
                        key.strip_prefix(folder)
                            .and_then(|rest| rest.strip_prefix(&today))
                            .is_some_and(|rest| {
                                rest.starts_with(test_case.expected_after_date)
                                    && rest.rsplit('/').next().is_some_and(|file| {
                                        file.starts_with("orders_")
                                            && file.ends_with(test_case.expected_extension)
                                    })
                            })
                    })
                })
                .collect();
            assert_eq!(
                folders.len(),
                test_case.expected_folders.len(),
                "{}: {keys:?}",
                test_case.description
            );
            let matched = keys
                .iter()
                .filter(|key| {
                    test_case
                        .expected_folders
                        .iter()
                        .any(|folder| key.starts_with(folder))
                })
                .count();
            assert_eq!(matched, keys.len(), "{}: {keys:?}", test_case.description);
        }
        Ok(())
    })
}

#[test]
fn given_failing_uploads_when_exporting_to_s3_then_retries_reuse_the_key_and_load_once()
-> Result<(), String> {
    run_async(async {
        let test_cases = [
            S3RetryTestCase {
                description: "server errors retry the same key",
                failures: 3,
                status: 500,
                records: 200,
                expected_rows: 200,
            },
            S3RetryTestCase {
                description: "expired credentials are reloaded and the upload retried",
                failures: 1,
                status: 400,
                records: 200,
                expected_rows: 200,
            },
        ];
        for test_case in &test_cases {
            let stack = start_stack().await?;
            produce_records(
                &stack.redpanda.brokers,
                "orders.created",
                PARTITIONS,
                &order_values(0, test_case.records),
            )
            .await?;
            stack
                .fake
                .fail_next_puts(test_case.failures, test_case.status);
            let settings = S3Settings {
                extra: SHARED_TABLE,
                ..S3Settings::default()
            };
            let running = start_exporter(s3_test_config(&stack, "orders.*", settings)?);
            wait_for_rows(&stack.fake, "ORDERS", test_case.expected_rows).await?;
            stop_exporter(running).await?;
            let rows = stack.fake.rows("ORDERS");
            assert_eq!(
                rows.len(),
                test_case.expected_rows,
                "{}",
                test_case.description
            );
            assert_eq!(
                distinct_positions(&rows),
                test_case.expected_rows,
                "{}",
                test_case.description
            );
            let puts = stack.fake.puts();
            let attempted: BTreeSet<&String> = puts.iter().collect();
            let stored: BTreeSet<String> = stack.fake.object_keys().into_iter().collect();
            assert_eq!(
                puts.len(),
                stored.len() + test_case.failures,
                "{}: {puts:?}",
                test_case.description
            );
            assert_eq!(
                attempted.into_iter().cloned().collect::<BTreeSet<String>>(),
                stored,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_withheld_load_reports_when_exporting_to_s3_then_offsets_are_committed_only_once_loads_are_reported()
-> Result<(), String> {
    run_async(async {
        let test_cases = [S3LoadReportTestCase {
            description: "loaded files stay uncommitted until insertReport lists them",
            records: 200,
            withheld_for: Duration::from_secs(4),
            expected_committed: 200,
        }];
        for test_case in &test_cases {
            let stack = start_stack().await?;
            produce_records(
                &stack.redpanda.brokers,
                TOPICS[0],
                PARTITIONS,
                &order_values(0, test_case.records),
            )
            .await?;
            stack.fake.withhold_load_reports(true);
            let settings = S3Settings {
                extra: SHARED_TABLE,
                ..S3Settings::default()
            };
            let running = start_exporter(s3_test_config(&stack, "orders.*", settings)?);
            wait_for_rows(&stack.fake, "ORDERS", test_case.records).await?;
            tokio::time::sleep(test_case.withheld_for).await;
            let before = committed(&stack, TOPICS[0]).await?;
            stack.fake.withhold_load_reports(false);
            let after = wait_for_commits(
                &stack,
                ORDERS_GROUP,
                TOPICS[0],
                test_case.expected_committed,
            )
            .await;
            stop_exporter(running).await?;
            assert_eq!(before, 0, "{}", test_case.description);
            assert_eq!(
                after?, test_case.expected_committed,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_failed_or_lost_file_when_exporting_to_s3_with_halt_then_the_stream_stops_committing_only_loaded_batches()
-> Result<(), String> {
    run_async(async {
        let test_cases = [
            S3LoadFailureTestCase {
                description: "the first file fails to load, so no offset is committed",
                staging: "",
                script: &[LoadOutcome::Fail(LOAD_ERROR)],
                expected_error: LOAD_ERROR,
                expected_committed: 0..=0,
            },
            S3LoadFailureTestCase {
                description: "a batch that loaded before the failed one is committed",
                staging: "",
                script: &[LoadOutcome::Load, LoadOutcome::Fail(LOAD_ERROR)],
                expected_error: LOAD_ERROR,
                expected_committed: 1..=199,
            },
            S3LoadFailureTestCase {
                description: "an unreported file missing from the bucket fails",
                staging: "pending_check_after = \"1s\"",
                script: &[LoadOutcome::Lose],
                expected_error: "no longer in the stage",
                expected_committed: 0..=0,
            },
        ];
        for test_case in &test_cases {
            let stack = start_stack().await?;
            produce_records(
                &stack.redpanda.brokers,
                TOPICS[0],
                PARTITIONS,
                &order_values(0, 200),
            )
            .await?;
            stack.fake.script_loads(test_case.script);
            let settings = S3Settings {
                staging: test_case.staging,
                extra: SHARED_TABLE,
                ..S3Settings::default()
            };
            let running = start_exporter(s3_test_config(&stack, "orders.*", settings)?);
            let outcome = tokio::time::timeout(WAIT, running.task)
                .await
                .map_err(|_| format!("{}: the exporter kept running", test_case.description))?
                .map_err(|error| error.to_string())?;
            let error = outcome.expect_err(test_case.description);
            let committed = committed(&stack, TOPICS[0]).await?;
            assert!(
                error.contains(test_case.expected_error)
                    && error.contains("orders_")
                    && error.contains("COPY INTO"),
                "{}: {error}",
                test_case.description
            );
            assert!(
                test_case.expected_committed.contains(&committed),
                "{}: committed {committed}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_slow_or_unreported_file_when_exporting_to_s3_then_the_stream_keeps_waiting_and_commits_once_it_is_seen()
-> Result<(), String> {
    run_async(async {
        let test_cases = [
            S3SlowLoadTestCase {
                description: "a stalled file is sent once more, then waits through further checks",
                staging: "pending_check_after = \"1s\"\npending_recheck_every = \"2s\"",
                script: &[LoadOutcome::Stall],
                withhold_reports: false,
                held_for: Duration::from_secs(25),
                expected_sends: 2,
            },
            S3SlowLoadTestCase {
                description: "a load missing from insertReport is found in the load history",
                staging: "pending_check_after = \"1s\"",
                script: &[],
                withhold_reports: true,
                held_for: Duration::from_secs(0),
                expected_sends: 1,
            },
        ];
        for test_case in &test_cases {
            let stack = start_stack().await?;
            produce_records(
                &stack.redpanda.brokers,
                TOPICS[0],
                PARTITIONS,
                &order_values(0, 100),
            )
            .await?;
            stack.fake.script_loads(test_case.script);
            stack.fake.withhold_load_reports(test_case.withhold_reports);
            let settings = S3Settings {
                staging: test_case.staging,
                extra: SHARED_TABLE,
                ..S3Settings::default()
            };
            let running = start_exporter(s3_test_config(&stack, "orders.*", settings)?);
            tokio::time::sleep(test_case.held_for).await;
            let held = committed(&stack, TOPICS[0]).await?;
            let still_running = !running.task.is_finished();
            stack.fake.release_stalled();
            let after = wait_for_commits(&stack, ORDERS_GROUP, TOPICS[0], 100).await;
            let inserted = stack.fake.inserted();
            stop_exporter(running).await?;
            let first = inserted.first().cloned().unwrap_or_default();
            let sends = inserted.iter().filter(|path| **path == first).count();
            assert!(still_running, "{}", test_case.description);
            assert!(
                test_case.held_for.is_zero() || held == 0,
                "{}: committed {held} while the file was held",
                test_case.description
            );
            assert_eq!(
                sends, test_case.expected_sends,
                "{}: {inserted:?}",
                test_case.description
            );
            assert_eq!(after?, 100, "{}", test_case.description);
        }
        Ok(())
    })
}

#[test]
fn given_failing_history_scans_when_a_later_file_is_overdue_then_loaded_batches_are_still_committed()
-> Result<(), String> {
    run_async(async {
        let test_cases = [S3ScanFailureTestCase {
            description: "a 503 from loadHistoryScan does not hold back the loaded first batch",
            status: 503,
            records: 200,
            expected_committed: 1..=199,
        }];
        for test_case in &test_cases {
            let stack = start_stack().await?;
            produce_records(
                &stack.redpanda.brokers,
                TOPICS[0],
                PARTITIONS,
                &order_values(0, test_case.records),
            )
            .await?;
            stack
                .fake
                .script_loads(&[LoadOutcome::Load, LoadOutcome::Stall]);
            stack.fake.fail_next_history_scans(1_000, test_case.status);
            let settings = S3Settings {
                staging: "pending_check_after = \"1s\"",
                extra: SHARED_TABLE,
                ..S3Settings::default()
            };
            let running = start_exporter(s3_test_config(&stack, "orders.*", settings)?);
            let first = wait_for_commits(&stack, ORDERS_GROUP, TOPICS[0], 1).await;
            tokio::time::sleep(Duration::from_secs(12)).await;
            let scans = stack.fake.history_scans();
            let still_running = !running.task.is_finished();
            stop_exporter(running).await?;
            let first = first?;
            assert!(
                test_case.expected_committed.contains(&first),
                "{}: committed {first}",
                test_case.description
            );
            assert!(still_running, "{}", test_case.description);
            assert!(scans >= 1, "{}: {scans} scans", test_case.description);
        }
        Ok(())
    })
}

#[test]
fn given_skip_when_one_file_of_a_two_topic_batch_fails_then_every_offset_is_committed_and_counted()
-> Result<(), String> {
    run_async(async {
        let test_cases = [S3SkipTestCase {
            description: "the failed topic file is skipped and both topics are committed",
            records_per_topic: 150,
            expected_metric: "sqlload_skipped_files_total{stream=\"orders\"} 1",
        }];
        for test_case in &test_cases {
            let stack = start_stack().await?;
            for topic in TOPICS {
                produce_records(
                    &stack.redpanda.brokers,
                    topic,
                    PARTITIONS,
                    &order_values(0, test_case.records_per_topic),
                )
                .await?;
            }
            stack.fake.script_loads(&[LoadOutcome::Fail(LOAD_ERROR)]);
            let address = std::net::TcpListener::bind("127.0.0.1:0")
                .and_then(|listener| listener.local_addr())
                .map_err(|error| error.to_string())?;
            let extra = format!("{SHARED_TABLE}[metrics]\nlisten = \"{address}\"\n");
            let settings = S3Settings {
                staging: "on_load_error = \"skip\"",
                extra: &extra,
                ..S3Settings::default()
            };
            let running = start_exporter(s3_test_config(&stack, "orders.*", settings)?);
            let expected = test_case.records_per_topic as i64;
            let created = wait_for_commits(&stack, ORDERS_GROUP, TOPICS[0], expected).await;
            let cancelled = wait_for_commits(&stack, ORDERS_GROUP, TOPICS[1], expected).await;
            let metrics = read_metrics(address).await;
            stop_exporter(running).await?;
            assert_eq!(created?, expected, "{}", test_case.description);
            assert_eq!(cancelled?, expected, "{}", test_case.description);
            let metrics = metrics?;
            assert!(
                metrics.contains(test_case.expected_metric),
                "{}: {metrics}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_unreported_files_when_stopping_cleanly_then_only_loaded_batches_are_committed()
-> Result<(), String> {
    run_async(async {
        let test_cases = [S3ShutdownTestCase {
            description: "a stop while later files are stalled commits only the first batch",
            records: 200,
            expected_committed: 1..=199,
        }];
        for test_case in &test_cases {
            let stack = start_stack().await?;
            produce_records(
                &stack.redpanda.brokers,
                TOPICS[0],
                PARTITIONS,
                &order_values(0, test_case.records),
            )
            .await?;
            stack.fake.script_loads(&[
                LoadOutcome::Load,
                LoadOutcome::Stall,
                LoadOutcome::Stall,
                LoadOutcome::Stall,
                LoadOutcome::Stall,
            ]);
            let settings = S3Settings {
                extra: SHARED_TABLE,
                ..S3Settings::default()
            };
            let running = start_exporter(s3_test_config(&stack, "orders.*", settings)?);
            let first = wait_for_commits(&stack, ORDERS_GROUP, TOPICS[0], 1).await;
            let stopped = stop_exporter(running).await;
            let after = committed(&stack, TOPICS[0]).await?;
            stopped?;
            let first = first?;
            assert!(
                test_case.expected_committed.contains(&first),
                "{}: committed {first}",
                test_case.description
            );
            assert_eq!(after, first, "{}", test_case.description);
        }
        Ok(())
    })
}

#[test]
fn given_max_pending_files_reached_when_exporting_to_s3_then_the_stream_pauses_and_resumes_once_loads_are_reported()
-> Result<(), String> {
    run_async(async {
        let test_cases = [S3PauseTestCase {
            description: "one pending file pauses the stream until its report arrives",
            records: 300,
            held_for: Duration::from_secs(6),
            expected_uploads_while_held: 1,
        }];
        for test_case in &test_cases {
            let stack = start_stack().await?;
            produce_records(
                &stack.redpanda.brokers,
                TOPICS[0],
                PARTITIONS,
                &order_values(0, test_case.records),
            )
            .await?;
            stack.fake.withhold_load_reports(true);
            stack.fake.withhold_load_history(true);
            let settings = S3Settings {
                staging: "max_pending_files = 1",
                extra: SHARED_TABLE,
                ..S3Settings::default()
            };
            let running = start_exporter(s3_test_config(&stack, "orders.*", settings)?);
            wait_for_rows(&stack.fake, "ORDERS", 1).await?;
            tokio::time::sleep(test_case.held_for).await;
            let uploads = stack.fake.puts().len();
            stack.fake.withhold_load_reports(false);
            let expected = test_case.records as i64;
            let after = wait_for_commits(&stack, ORDERS_GROUP, TOPICS[0], expected).await;
            stop_exporter(running).await?;
            assert_eq!(
                uploads, test_case.expected_uploads_while_held,
                "{}",
                test_case.description
            );
            assert_eq!(after?, expected, "{}", test_case.description);
        }
        Ok(())
    })
}
