use std::time::Duration;

use sqlload::config::models::Config;
use sqlload::exporter::main::build_clients::build_clients;
use sqlload::exporter::main::run_exporter::run_exporter;
use sqlload::exporter::models::Clients;
use sqlload::kafka::main::list_topics::list_topics;
use sqlload::kafka::main::select_topics::select_topics;
use sqlload::naming::main::resolve_streams::resolve_streams;
use sqlload::naming::main::resolve_tables::resolve_tables;
use sqlload::naming::main::shard_of::shard_of;
use sqlload::naming::models::{NameSource, TopicTable};
use sqlload::planning::main::apply_plan::apply_plan;
use sqlload::planning::main::build_plan::build_plan;
use sqlload::planning::main::read_existing::read_existing;
use sqlload::planning::models::{ObjectState, Plan, PlannedObject, StreamPlan};
use sqlload::status::main::check_connections::check_connections;
use sqlload::status::main::collect_status::collect_status;
use sqlload::status::models::{PartitionStatus, StreamStatus};
use tokio_util::sync::CancellationToken;

use crate::command::_helpers::limits::raise_open_file_limit;
use crate::command::_helpers::signals::wait_for_signal;

fn clients(config: &Config) -> Result<Clients, String> {
    build_clients(config).map_err(|error| error.to_string())
}

pub(crate) async fn check(config: &Config) -> Result<Vec<String>, String> {
    let findings = check_connections(config, &clients(config)?).await?;
    Ok(findings
        .into_iter()
        .map(|line| format!("ok  {line}"))
        .collect())
}

async fn current_plan(config: &Config, clients: &Clients) -> Result<Plan, String> {
    let topics = list_topics(config).await?;
    let streams = resolve_streams(config, &topics).map_err(|problems| problems.join("\n"))?;
    let existing = read_existing(&clients.sql, config)
        .await
        .map_err(|error| error.to_string())?;
    Ok(build_plan(
        config,
        &streams,
        &existing,
        clients.auth.uses_key_pair(),
    ))
}

pub(crate) async fn plan(config: &Config) -> Result<Vec<String>, String> {
    let clients = clients(config)?;
    let topics = list_topics(config).await?;
    let tables = resolve_tables(&config.naming, &topics).map_err(|problems| problems.join("\n"))?;
    let plan = current_plan(config, &clients).await?;
    let mut lines: Vec<String> = vec![format!("Topics ({}):", tables.len())];
    lines.extend(tables.iter().map(topic_line));
    lines.push(String::new());
    lines.push("Objects:".to_owned());
    let shards = config.source.shards;
    lines.extend(
        plan.streams
            .iter()
            .flat_map(|stream| stream_lines(stream, shards)),
    );
    let problems = plan.problems();
    lines.push(String::new());
    lines.push(format!(
        "{} to create, {} incompatible",
        plan.missing().len(),
        problems.len()
    ));
    match problems.is_empty() {
        true => Ok(lines),
        false => Err(format!(
            "{}\nexisting objects do not fit the configuration",
            lines.join("\n")
        )),
    }
}

fn topic_line(entry: &TopicTable) -> String {
    let source = match &entry.source {
        NameSource::Map => "map".to_owned(),
        NameSource::Rule(template) => format!("rule {template}"),
        NameSource::Default => "default".to_owned(),
        NameSource::Fallback => "fallback".to_owned(),
    };
    format!("  {} -> {} ({source})", entry.topic, entry.table)
}

fn stream_lines(stream: &StreamPlan, shards: u32) -> Vec<String> {
    let shard = match shards {
        1 => String::new(),
        _ => format!(", shard {}", shard_of(&stream.stream.table, shards)),
    };
    let header = format!(
        "  {} [{:?}, group {}{shard}]",
        stream.stream.table, stream.load_path, stream.stream.consumer_group
    );
    std::iter::once(header)
        .chain(stream.objects.iter().map(object_line))
        .collect()
}

fn object_line(object: &PlannedObject) -> String {
    match &object.state {
        ObjectState::Exists => format!("    = {} {} exists", object.kind, object.name),
        ObjectState::Missing => format!("    + {}", object.create_sql),
        ObjectState::Incompatible(reasons) => {
            format!(
                "    ! {} {}: {}",
                object.kind,
                object.name,
                reasons.join("; ")
            )
        }
    }
}

pub(crate) async fn apply(config: &Config) -> Result<Vec<String>, String> {
    let clients = clients(config)?;
    let plan = current_plan(config, &clients).await?;
    let created = apply_plan(&clients.sql, &plan)
        .await
        .map_err(|error| error.to_string())?;
    let mut lines: Vec<String> = created
        .iter()
        .map(|object| format!("created {} {}", object.kind, object.name))
        .collect();
    lines.push(format!("{} created", created.len()));
    Ok(lines)
}

pub(crate) async fn tables(
    config: &Config,
    topics_file: Option<&std::path::Path>,
) -> Result<Vec<String>, String> {
    let topics = match topics_file {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
            select_topics(config, text.split_whitespace().map(str::to_owned).collect())?
        }
        None => list_topics(config).await?,
    };
    let streams = resolve_streams(config, &topics).map_err(|problems| problems.join("\n"))?;
    let mut by_topic: std::collections::BTreeMap<&str, &sqlload::naming::models::Stream> =
        std::collections::BTreeMap::new();
    for stream in &streams {
        for topic in &stream.topics {
            by_topic.insert(topic.as_str(), stream);
        }
    }
    Ok(topics
        .iter()
        .map(|topic| match by_topic.get(topic.as_str()) {
            Some(stream) => format!(
                "{topic}\t{}\t{}\t{}",
                stream.table, stream.name, stream.consumer_group
            ),
            None => format!("{topic}\t-"),
        })
        .collect())
}

pub(crate) async fn status(config: &Config) -> Result<Vec<String>, String> {
    let statuses = collect_status(config, &clients(config)?).await?;
    Ok(statuses.iter().flat_map(status_lines).collect())
}

fn status_lines(stream: &StreamStatus) -> Vec<String> {
    let header = format!(
        "{} -> {} (group {}, lag {})",
        stream.stream,
        stream.table,
        stream.consumer_group,
        stream.total_lag()
    );
    std::iter::once(header)
        .chain(stream.partitions.iter().map(partition_line))
        .collect()
}

fn partition_line(partition: &PartitionStatus) -> String {
    let channel = match partition.channel_committed {
        Some(offset) => format!(
            ", channel committed {offset}, rejected rows {}",
            partition.channel_error_rows.unwrap_or_default()
        ),
        None => String::new(),
    };
    let committed = match partition.kafka_committed {
        Some(offset) => offset.to_string(),
        None => "-".to_owned(),
    };
    format!(
        "  {}[{}] committed {committed} of {}, lag {}{channel}",
        partition.topic, partition.partition, partition.high_watermark, partition.lag
    )
}

pub(crate) async fn run_export(
    config: Config,
    max_duration: Option<Duration>,
) -> Result<Vec<String>, String> {
    raise_open_file_limit();
    let shutdown = CancellationToken::new();
    let signal = shutdown.clone();
    tokio::spawn(async move {
        wait_for_signal().await;
        tracing::info!("shutting down");
        signal.cancel();
    });
    let summary = run_exporter(config, max_duration, shutdown).await?;
    tracing::info!(
        streams = summary.streams.len(),
        rows = summary.rows,
        connections_opened = summary.connections_opened,
        elapsed_secs = summary.elapsed.as_secs(),
        "run finished"
    );
    Ok(Vec::new())
}
