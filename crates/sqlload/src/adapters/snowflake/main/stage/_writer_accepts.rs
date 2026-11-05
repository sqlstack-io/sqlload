//! What Snowflake takes on a stage route.

use crate::adapters::snowflake::constants::USER_STAGE;
use crate::adapters::snowflake::models::StageRoute;
use crate::routing::models::{FileFormat, Store, WriterAccepts};

/// What Snowflake takes over `stage`: Parquet files on that stage (and JSON on an external stage, read where it lies), or rows sent without files; the user stage when none was chosen.
pub(in crate::adapters) fn writer_accepts(stage: Option<&StageRoute>) -> WriterAccepts {
    let stored = |store: Store| WriterAccepts {
        arrow: false,
        pulls_from: vec![store.clone()],
        pull_formats: vec![FileFormat::Parquet],
        staging: vec![store],
        rows_into: None,
    };
    match stage {
        Some(StageRoute::External(stage)) => WriterAccepts {
            pull_formats: vec![FileFormat::Parquet, FileFormat::Json],
            ..stored(Store::ObjectStore(stage.location.to_string()))
        },
        Some(StageRoute::Internal { name }) => stored(Store::InternalStage(format!("@{name}"))),
        Some(StageRoute::TargetTable) => stored(Store::InternalStage("@%table".to_owned())),
        Some(StageRoute::StagingTable) => stored(Store::InternalStage(
            "the staging table's @%table".to_owned(),
        )),
        Some(StageRoute::Streaming { .. }) => WriterAccepts {
            rows_into: Some("Snowpipe Streaming into a transient staging table".to_owned()),
            ..WriterAccepts::default()
        },
        Some(StageRoute::Insert) => WriterAccepts {
            rows_into: Some("batched INSERT into a transient staging table".to_owned()),
            ..WriterAccepts::default()
        },
        _ => stored(Store::InternalStage(format!("@{USER_STAGE}"))),
    }
}
