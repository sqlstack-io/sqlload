use std::collections::BTreeSet;

use sqlload::time_text::main::now_ms::now_ms;
use sqlload::time_text::main::utc_date::utc_date;
use sqlload_testkit::configs::models::S3Settings;
use sqlload_testkit::harness::main::start_exporter::start_exporter;
use sqlload_testkit::harness::main::start_stack::start_stack;
use sqlload_testkit::harness::main::stop_exporter::stop_exporter;
use sqlload_testkit::harness::main::wait_for_rows::wait_for_rows;
use sqlload_testkit::records::main::distinct_positions::distinct_positions;
use sqlload_testkit::records::main::order_values::order_values;
use sqlload_testkit::redpanda::main::produce_records::produce_records;

use crate::helpers::{PARTITIONS, run_async, s3_test_config};
use crate::test_types::{S3LayoutTestCase, S3RetryTestCase};

const SHARED_TABLE: &str = "[batching]\nmax_age = \"1s\"\nmax_rows = 100\n[[naming.rules]]\nmatch = \"orders.{kind}\"\ntable = \"ORDERS\"\n";
const TOPICS: [&str; 2] = ["orders.created", "orders.cancelled"];

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
                expected_rows: 300,
            },
            S3LayoutTestCase {
                description: "COPY loads table folders with hourly partitions",
                staging: "load = \"copy\"",
                s3: "path = \"{table}/date={date}/hour={hour}/\"",
                records_per_topic: 150,
                expected_folders: &["fake-bucket/exports/ORDERS/date="],
                expected_after_date: "/hour=",
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
                                        file.starts_with("orders_") && file.ends_with(".json.gz")
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
