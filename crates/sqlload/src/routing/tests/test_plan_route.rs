use crate::routing::main::plan_route::plan_route;
use crate::routing::models::{FileFormat, ReadVia, ReaderOffer, RouteOverrides, StagePreference};
use crate::routing::tests::helpers::{
    LAKE, clickhouse_destination, database_destination, files_source, internal_stage,
    local_destination, object_store, streaming_source, user_stage, warehouse_destination,
    warehouse_source,
};
use crate::routing::tests::test_types::{PlanRouteErrorTestCase, PlanRouteTestCase};

const HMAC: &str = "email masked with hmac, whose key must never appear in SQL";
const SMALL: Option<u64> = Some(1024 * 1024);
const BIG: Option<u64> = Some(10 * 1024 * 1024 * 1024);

#[test]
fn given_a_source_and_destination_when_planning_then_the_cheapest_route_is_chosen() {
    let test_cases = [
        PlanRouteTestCase {
            description: "database to database streams Arrow straight through",
            reader: streaming_source(),
            writer: database_destination(),
            overrides: RouteOverrides::default(),
            expected_steps: &["read as Arrow", "destination takes Arrow"],
        },
        PlanRouteTestCase {
            description: "database to warehouse stages Parquet on the external stage",
            reader: streaming_source(),
            writer: warehouse_destination(vec![object_store("s3://lake/sqlload/"), user_stage()]),
            overrides: RouteOverrides::default(),
            expected_steps: &[
                "read as Arrow",
                "write Parquet to s3://lake/sqlload/",
                "destination loads parquet files from s3://lake/sqlload/",
            ],
        },
        PlanRouteTestCase {
            description: "an internal stage preference stages on the internal stage",
            reader: streaming_source(),
            writer: warehouse_destination(vec![object_store("s3://lake/sqlload/"), user_stage()]),
            overrides: internal_stage(),
            expected_steps: &[
                "read as Arrow",
                "write Parquet to internal stage @~",
                "destination loads parquet files from internal stage @~",
            ],
        },
        PlanRouteTestCase {
            description: "without an external stage the internal stage is used",
            reader: streaming_source(),
            writer: warehouse_destination(vec![user_stage()]),
            overrides: RouteOverrides::default(),
            expected_steps: &[
                "read as Arrow",
                "write Parquet to internal stage @~",
                "destination loads parquet files from internal stage @~",
            ],
        },
        PlanRouteTestCase {
            description: "existing files the warehouse can reach are loaded in place",
            reader: files_source("s3://lake/scores/date=2026-11-04/", FileFormat::Parquet),
            writer: warehouse_destination(vec![object_store(LAKE)]),
            overrides: RouteOverrides::default(),
            expected_steps: &[
                "use parquet files in s3://lake/scores/date=2026-11-04/",
                "destination loads parquet files from s3://lake/scores/date=2026-11-04/",
            ],
        },
        PlanRouteTestCase {
            description: "existing files the warehouse cannot reach are read and restaged only for hmac",
            reader: ReaderOffer {
                needs_loader: Some(HMAC.to_owned()),
                ..files_source("s3://other/events/", FileFormat::Json)
            },
            writer: warehouse_destination(vec![object_store(LAKE)]),
            overrides: RouteOverrides::default(),
            expected_steps: &[
                "use json files in s3://other/events/",
                "read json into Arrow",
                "write Parquet to s3://lake/",
                "destination loads parquet files from s3://lake/",
            ],
        },
        PlanRouteTestCase {
            description: "files the warehouse reads itself, but hmac-masked, go through SQLLoad",
            reader: ReaderOffer {
                needs_loader: Some(HMAC.to_owned()),
                ..files_source("s3://lake/events/", FileFormat::Parquet)
            },
            writer: warehouse_destination(vec![object_store(LAKE)]),
            overrides: RouteOverrides::default(),
            expected_steps: &[
                "use parquet files in s3://lake/events/",
                "read parquet into Arrow",
                "write Parquet to s3://lake/",
                "destination loads parquet files from s3://lake/",
            ],
        },
        PlanRouteTestCase {
            description: "files a non-warehouse destination cannot read itself are read by SQLLoad",
            reader: files_source("s3://other/events/", FileFormat::Csv),
            writer: database_destination(),
            overrides: RouteOverrides::default(),
            expected_steps: &[
                "use csv files in s3://other/events/",
                "read csv into Arrow",
                "destination takes Arrow",
            ],
        },
        PlanRouteTestCase {
            description: "a small warehouse table is queried",
            reader: warehouse_source(SMALL),
            writer: database_destination(),
            overrides: RouteOverrides::default(),
            expected_steps: &["read as Arrow", "destination takes Arrow"],
        },
        PlanRouteTestCase {
            description: "a big warehouse table is unloaded and read back for a database",
            reader: warehouse_source(BIG),
            writer: database_destination(),
            overrides: RouteOverrides::default(),
            expected_steps: &[
                "unload Parquet to s3://lake/unload/",
                "read parquet into Arrow",
                "destination takes Arrow",
            ],
        },
        PlanRouteTestCase {
            description: "a warehouse table of unknown size is unloaded",
            reader: warehouse_source(None),
            writer: database_destination(),
            overrides: RouteOverrides::default(),
            expected_steps: &[
                "unload Parquet to s3://lake/unload/",
                "read parquet into Arrow",
                "destination takes Arrow",
            ],
        },
        PlanRouteTestCase {
            description: "read_via = query queries even a big table",
            reader: warehouse_source(BIG),
            writer: database_destination(),
            overrides: RouteOverrides {
                read_via: ReadVia::Query,
                ..RouteOverrides::default()
            },
            expected_steps: &["read as Arrow", "destination takes Arrow"],
        },
        PlanRouteTestCase {
            description: "ClickHouse reads an unload from the lake itself",
            reader: warehouse_source(BIG),
            writer: clickhouse_destination(),
            overrides: RouteOverrides::default(),
            expected_steps: &[
                "unload Parquet to s3://lake/unload/",
                "destination loads parquet files from s3://lake/unload/",
            ],
        },
        PlanRouteTestCase {
            description: "an unload the destination could read itself is read back when the table needs the loader",
            reader: ReaderOffer {
                needs_loader: Some(HMAC.to_owned()),
                ..warehouse_source(BIG)
            },
            writer: clickhouse_destination(),
            overrides: RouteOverrides::default(),
            expected_steps: &[
                "unload Parquet to s3://lake/unload/",
                "read parquet into Arrow",
                "destination takes Arrow",
            ],
        },
        PlanRouteTestCase {
            description: "a local directory downloads an unload from the lake",
            reader: warehouse_source(BIG),
            writer: local_destination(),
            overrides: RouteOverrides::default(),
            expected_steps: &[
                "unload Parquet to s3://lake/unload/",
                "destination loads parquet files from s3://lake/unload/",
            ],
        },
        PlanRouteTestCase {
            description: "preferring the internal stage, a local directory fetches from the internal stage",
            reader: warehouse_source(BIG),
            writer: local_destination(),
            overrides: internal_stage(),
            expected_steps: &[
                "unload Parquet to internal stage @~",
                "destination loads parquet files from internal stage @~",
            ],
        },
    ];
    for test_case in test_cases {
        let route = plan_route(&test_case.reader, &test_case.writer, test_case.overrides)
            .unwrap_or_else(|error| panic!("{}: {error}", test_case.description));
        let steps: Vec<String> = route.steps.iter().map(ToString::to_string).collect();
        assert_eq!(steps, test_case.expected_steps, "{}", test_case.description);
        assert!(!route.reason.is_empty(), "{}", test_case.description);
    }
}

#[test]
fn given_an_impossible_combination_when_planning_then_the_error_says_why() {
    let test_cases = [
        PlanRouteErrorTestCase {
            description: "stage = external without an external stage",
            reader: streaming_source(),
            writer: warehouse_destination(vec![user_stage()]),
            overrides: RouteOverrides {
                stage: StagePreference::External,
                ..RouteOverrides::default()
            },
            expected_message: "has no external stage",
        },
        PlanRouteErrorTestCase {
            description: "read_via = unload on a source that cannot unload",
            reader: streaming_source(),
            writer: database_destination(),
            overrides: RouteOverrides {
                read_via: ReadVia::Unload,
                ..RouteOverrides::default()
            },
            expected_message: "the source cannot unload",
        },
        PlanRouteErrorTestCase {
            description: "a warehouse destination without any stage",
            reader: streaming_source(),
            writer: warehouse_destination(Vec::new()),
            overrides: RouteOverrides::default(),
            expected_message: "neither Arrow nor staged Parquet",
        },
        PlanRouteErrorTestCase {
            description: "files no stage of the warehouse covers fail, never going through SQLLoad",
            reader: files_source("s3://other/events/", FileFormat::Parquet),
            writer: warehouse_destination(vec![object_store(LAKE), user_stage()]),
            overrides: RouteOverrides::default(),
            expected_message: "s3://other/events/ cannot be read by Snowflake: no external stage covers it. Fix: add a storage integration that allows this bucket (the fast path), or, if that isn't possible, set copy_to_internal_stage = true",
        },
        PlanRouteErrorTestCase {
            description: "files whose path a stage names but on another S3 endpoint fail as unreadable, never planned as read in place",
            reader: ReaderOffer {
                out_of_reach: true,
                ..files_source("s3://lake/events/", FileFormat::Parquet)
            },
            writer: warehouse_destination(vec![object_store(LAKE)]),
            overrides: RouteOverrides::default(),
            expected_message: "s3://lake/events/ cannot be read by Snowflake: no external stage covers it",
        },
        PlanRouteErrorTestCase {
            description: "files the warehouse reaches but cannot load exactly fail, never going through SQLLoad",
            reader: ReaderOffer {
                direct_blocked: Some("half is Float16 in the files".to_owned()),
                ..files_source("s3://lake/events/", FileFormat::Parquet)
            },
            writer: warehouse_destination(vec![object_store(LAKE)]),
            overrides: RouteOverrides::default(),
            expected_message: "the destination cannot load these files exactly by reading them itself: half is Float16 in the files",
        },
        PlanRouteErrorTestCase {
            description: "a source with nothing to offer",
            reader: crate::routing::models::ReaderOffer::default(),
            writer: database_destination(),
            overrides: RouteOverrides::default(),
            expected_message: "neither streams rows nor has files",
        },
    ];
    for test_case in test_cases {
        let error = plan_route(&test_case.reader, &test_case.writer, test_case.overrides)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(
            error.contains(test_case.expected_message),
            "{}: {error}",
            test_case.description
        );
    }
}
