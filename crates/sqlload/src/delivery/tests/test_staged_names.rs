use std::sync::Arc;

use crate::batching::models::{Batch, BatchRow};
use crate::config::main::parse_config::parse_config;
use crate::config::models::OversizedPolicy;
use crate::delivery::main::build_delivery::build_delivery;
use crate::delivery::models::DeliveryInputs;
use crate::delivery::tests::helpers::{copy_keys, stage_keys};
use crate::delivery::tests::test_types::StagedNamesTestCase;
use crate::exporter::main::build_clients::build_clients;
use crate::naming::models::Stream;
use crate::planning::main::load_path::load_path;

const SOURCE: &str = "[source]\nkind = \"kafka\"\nbrokers = \"b\"\ninclude = [\"shop.*\"]\n";
const SNOWFLAKE: &str = "[destination]\nkind = \"snowflake\"\naccount = \"myorg-myaccount\"\nuser = \"LOADER\"\nauth = { method = \"pat\", token = \"t\" }\ndatabase = \"MY_DB\"\nschema = \"RAW\"\n[staging]\nkind = \"internal\"\nload = \"copy\"\n";
const DATABRICKS: &str = "[destination]\nkind = \"databricks\"\nhost = \"dbc-1.cloud.databricks.com\"\nclient_id = \"id\"\nclient_secret = \"secret\"\nwarehouse_id = \"w1\"\ncatalog = \"main\"\nschema = \"raw\"\n[staging]\nkind = \"internal\"\n";

#[test]
fn given_the_same_offsets_when_a_new_delivery_stages_them_then_its_files_never_take_an_earlier_runs_names()
-> Result<(), String> {
    let test_cases = [
        StagedNamesTestCase {
            description: "Snowflake COPY retrying a batch in the same run reuses its files, which COPY skips once loaded",
            config: SNOWFLAKE,
            keys: stage_keys,
            restarted: false,
            expected_same_names: true,
        },
        StagedNamesTestCase {
            description: "Snowflake COPY after a restart (or a re-created table) names new files, so its load metadata can't skip them",
            config: SNOWFLAKE,
            keys: stage_keys,
            restarted: true,
            expected_same_names: false,
        },
        StagedNamesTestCase {
            description: "Databricks COPY INTO retrying a batch in the same run reuses its files",
            config: DATABRICKS,
            keys: copy_keys,
            restarted: false,
            expected_same_names: true,
        },
        StagedNamesTestCase {
            description: "Databricks COPY INTO after a restart names new files, so it can't skip them as loaded by path",
            config: DATABRICKS,
            keys: copy_keys,
            restarted: true,
            expected_same_names: false,
        },
    ];
    let stream = Stream {
        table: "shop__orders".to_owned(),
        name: "shop__orders".to_owned(),
        topics: vec!["shop.orders".to_owned()],
        consumer_group: "g".to_owned(),
        stage: "S".to_owned(),
        pipe: "P".to_owned(),
        streaming_pipe: "SP".to_owned(),
    };
    let batch = Batch {
        rows: (0..3)
            .map(|offset| BatchRow {
                topic: "shop.orders".to_owned(),
                partition: 0,
                offset,
                line: "{}".to_owned(),
            })
            .collect(),
        bytes: 6,
    };
    for test_case in &test_cases {
        let config = Arc::new(
            parse_config(&format!("{SOURCE}{}", test_case.config), &|_| None)
                .map_err(|error| format!("{}: {error}", test_case.description))?,
        );
        let clients = build_clients(&config).map_err(|error| error.to_string())?;
        let inputs = DeliveryInputs {
            http: clients.http.clone(),
            in_flight: clients.in_flight.clone(),
            warehouse: clients.warehouse.clone(),
            config: config.clone(),
            path: load_path(&config, false),
            oversized: OversizedPolicy::Fail,
        };
        let mut deliveries = [
            build_delivery(&inputs, &stream).map_err(|error| error.to_string())?,
            build_delivery(&inputs, &stream).map_err(|error| error.to_string())?,
        ];
        let named = (test_case.keys)(&mut deliveries[0], &batch)?;
        let renamed = (test_case.keys)(&mut deliveries[usize::from(test_case.restarted)], &batch)?;
        assert!(!named.is_empty(), "{}", test_case.description);
        assert_eq!(
            named == renamed,
            test_case.expected_same_names,
            "{}: {named:?} then {renamed:?}",
            test_case.description
        );
    }
    Ok(())
}
