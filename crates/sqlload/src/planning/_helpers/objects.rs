use crate::config::models::Config;
use crate::naming::main::qualify_stage::qualify_stage;
use crate::naming::models::Stream;
use crate::planning::_helpers::compatibility::column_problems;
use crate::planning::_helpers::ddl::{
    create_stage, create_stage_pipe, create_streaming_pipe, create_table, qualified,
};
use crate::planning::constants::DEFAULT_STREAMING_PIPE_SUFFIX;
use crate::planning::models::{
    ExistingObjects, LoadPath, ObjectKind, ObjectState, PlannedObject, StagePipe,
};

fn is_default_streaming_pipe(stream: &Stream) -> bool {
    stream.streaming_pipe == format!("{}{DEFAULT_STREAMING_PIPE_SUFFIX}", stream.table)
}

pub(crate) fn stream_objects(
    config: &Config,
    stream: &Stream,
    existing: &ExistingObjects,
    path: LoadPath,
) -> Vec<PlannedObject> {
    let error_logging = path == LoadPath::Streaming && config.envelope.error_logging;
    let mut objects = vec![table_object(config, stream, existing, error_logging)];
    match path {
        LoadPath::Streaming if !is_default_streaming_pipe(stream) => {
            let sql = create_streaming_pipe(config, &stream.streaming_pipe, &stream.table);
            objects.push(pipe_object(stream.streaming_pipe.clone(), sql, existing));
        }
        LoadPath::Streaming | LoadPath::S3Copy => {}
        LoadPath::StageCopy => objects.push(stage_object(config, stream, existing)),
        LoadPath::StageInsertFiles => {
            objects.push(stage_object(config, stream, existing));
            let source = format!("@{}", qualified(config, &stream.stage));
            let sql = create_stage_pipe(
                config,
                &StagePipe {
                    pipe: &stream.pipe,
                    table: &stream.table,
                    source: &source,
                    auto_ingest: false,
                },
            );
            objects.push(pipe_object(stream.pipe.clone(), sql, existing));
        }
        LoadPath::S3AutoIngest | LoadPath::S3InsertFiles => {
            let auto_ingest = path == LoadPath::S3AutoIngest;
            let sql = create_stage_pipe(
                config,
                &StagePipe {
                    pipe: &stream.pipe,
                    table: &stream.table,
                    source: &s3_source(config, stream, auto_ingest),
                    auto_ingest,
                },
            );
            objects.push(pipe_object(stream.pipe.clone(), sql, existing));
        }
    }
    objects
}

/// The pipe's stage location: the bucket prefix, narrowed to the table's folder for auto-ingest.
fn s3_source(config: &Config, stream: &Stream, auto_ingest: bool) -> String {
    let Some(s3) = config.staging.s3.as_ref() else {
        return String::new();
    };
    let stage = qualify_stage(
        &config.destination.database,
        &config.destination.schema,
        &s3.stage,
    );
    let mut location = format!("@{stage}/");
    let prefix = s3.prefix.trim_matches('/');
    if !prefix.is_empty() {
        location.push_str(prefix);
        location.push('/');
    }
    if auto_ingest {
        location.push_str(&stream.table);
        location.push('/');
    }
    location
}

fn table_object(
    config: &Config,
    stream: &Stream,
    existing: &ExistingObjects,
    error_logging: bool,
) -> PlannedObject {
    let state = match existing.tables.get(&stream.table) {
        None => ObjectState::Missing,
        Some(columns) => match column_problems(&config.envelope.columns, columns) {
            problems if problems.is_empty() => ObjectState::Exists,
            problems => ObjectState::Incompatible(problems),
        },
    };
    PlannedObject {
        kind: ObjectKind::Table,
        name: stream.table.clone(),
        state,
        create_sql: create_table(config, &stream.table, error_logging),
    }
}

fn stage_object(config: &Config, stream: &Stream, existing: &ExistingObjects) -> PlannedObject {
    PlannedObject {
        kind: ObjectKind::Stage,
        name: stream.stage.clone(),
        state: match existing.stages.contains(&stream.stage) {
            true => ObjectState::Exists,
            false => ObjectState::Missing,
        },
        create_sql: create_stage(config, &stream.stage),
    }
}

fn pipe_object(name: String, create_sql: String, existing: &ExistingObjects) -> PlannedObject {
    PlannedObject {
        kind: ObjectKind::Pipe,
        state: match existing.pipes.contains_key(&name) {
            true => ObjectState::Exists,
            false => ObjectState::Missing,
        },
        name,
        create_sql,
    }
}
