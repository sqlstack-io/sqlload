use sqlload_testkit::harness::main::start_exporter::start_exporter;
use sqlload_testkit::harness::main::start_stack::start_stack;
use sqlload_testkit::harness::main::stop_exporter::stop_exporter;
use sqlload_testkit::harness::main::wait_for_rows::wait_for_rows;
use sqlload_testkit::records::main::distinct_positions::distinct_positions;
use sqlload_testkit::records::main::incompressible_values::incompressible_values;
use sqlload_testkit::records::main::order_values::order_values;
use sqlload_testkit::redpanda::main::produce_records::produce_records;

use crate::helpers::{PARTITIONS, config, run_async};

use crate::test_types::{DeliveryTestCase, DiscoveryTestCase};

#[test]
fn given_records_when_exporting_then_each_arrives_once_over_few_connections() -> Result<(), String>
{
    run_async(async {
        let test_cases = [
            DeliveryTestCase {
                description: "every record lands once with its envelope, over a handful of connections",
                topic: "orders.created",
                records: 600,
                oversized_records: 0,
                extra_config: "[batching]\nmax_age = \"1s\"\nmax_rows = 50\n",
                expected_table: "ORDERS__CREATED",
                expected_rows: 600,
                expected_max_connections: 4,
            },
            DeliveryTestCase {
                description: "rows over the request limit are skipped under the skip policy",
                topic: "orders.bulky",
                records: 50,
                oversized_records: 2,
                extra_config: "[batching]\nmax_age = \"1s\"\nmax_request_bytes = 4000\noversized = \"skip\"\n",
                expected_table: "ORDERS__BULKY",
                expected_rows: 50,
                expected_max_connections: 4,
            },
        ];
        for test_case in &test_cases {
            let stack = start_stack().await?;
            let mut values = order_values(0, test_case.records);
            values.extend(incompressible_values(test_case.oversized_records, 20_000));
            produce_records(
                &stack.redpanda.brokers,
                test_case.topic,
                PARTITIONS,
                &values,
            )
            .await?;
            let running = start_exporter(config(&stack, "orders.*", test_case.extra_config)?);
            wait_for_rows(
                &stack.fake,
                test_case.expected_table,
                test_case.expected_rows,
            )
            .await?;
            let summary = stop_exporter(running).await?;
            let rows = stack.fake.rows(test_case.expected_table);
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
            assert!(
                summary.connections_opened <= test_case.expected_max_connections,
                "{}: {} connections",
                test_case.description,
                summary.connections_opened
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_new_matching_topic_when_running_then_it_is_discovered_and_exported() -> Result<(), String>
{
    run_async(async {
        let test_cases = [DiscoveryTestCase {
            description: "a topic created after start is picked up by periodic discovery",
            first_topic: "orders.created",
            later_topic: "orders.returned",
            records: 40,
            expected_table: "ORDERS__RETURNED",
            expected_rows: 40,
        }];
        for test_case in &test_cases {
            let stack = start_stack().await?;
            produce_records(
                &stack.redpanda.brokers,
                test_case.first_topic,
                PARTITIONS,
                &order_values(0, 5),
            )
            .await?;
            let running = start_exporter(config(
                &stack,
                "orders.*",
                "[batching]\nmax_age = \"1s\"\n",
            )?);
            produce_records(
                &stack.redpanda.brokers,
                test_case.later_topic,
                PARTITIONS,
                &order_values(0, test_case.records),
            )
            .await?;
            wait_for_rows(
                &stack.fake,
                test_case.expected_table,
                test_case.expected_rows,
            )
            .await?;
            stop_exporter(running).await?;
            let rows = stack.fake.rows(test_case.expected_table);
            assert_eq!(
                rows.len(),
                test_case.expected_rows,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}
