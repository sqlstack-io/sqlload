use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::batching::models::BufferGauge;
use crate::config::main::stream_settings::stream_settings;
use crate::config::models::{Config, OversizedPolicy};
use crate::delivery::main::build_delivery::build_delivery;
use crate::delivery::models::{Delivery, DeliveryInputs, StageClients};
use crate::exporter::_helpers::stream_task::{StreamInputs, run_stream};
use crate::exporter::models::Clients;
use crate::kafka::main::list_topics::list_topics;
use crate::kafka::main::prefetch_kbytes::prefetch_kbytes;
use crate::metrics::models::Metrics;
use crate::naming::main::resolve_streams::resolve_streams;
use crate::naming::main::shard_of::shard_of;
use crate::naming::models::Stream;
use crate::planning::main::apply_plan::apply_plan;
use crate::planning::main::build_plan::build_plan;
use crate::planning::main::load_path::load_path;
use crate::planning::main::read_existing::read_existing;

/// How long a stream whose objects do not fit waits before its objects are checked again.
const INCOMPATIBLE_RECHECK: Duration = Duration::from_secs(3600);

/// How long a replaced stream may take to flush and commit before its successor starts.
const REPLACE_WAIT: Duration = Duration::from_secs(120);

struct Running {
    topics: Vec<String>,
    cancel: CancellationToken,
    finished: CancellationToken,
}

/// Discovers streams, creates their objects and keeps one task per stream running.
pub(crate) struct Supervisor {
    pub(crate) config: Arc<Config>,
    pub(crate) clients: Clients,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) gauge: BufferGauge,
    running: BTreeMap<String, Running>,
    tasks: JoinSet<(String, Result<(), String>)>,
    stream_count: usize,
    incompatible: BTreeMap<String, std::time::Instant>,
}

impl Supervisor {
    pub(crate) fn new(
        config: Arc<Config>,
        clients: Clients,
        metrics: Arc<Metrics>,
        gauge: BufferGauge,
    ) -> Self {
        Self {
            config,
            clients,
            metrics,
            gauge,
            running: BTreeMap::new(),
            tasks: JoinSet::new(),
            stream_count: 0,
            incompatible: BTreeMap::new(),
        }
    }

    /// Run until `cancel` fires or a stream fails.
    pub(crate) async fn run(&mut self, cancel: &CancellationToken) -> Result<Vec<String>, String> {
        self.discover(cancel).await?;
        let mut discovery = tokio::time::interval(self.config.source.discovery_interval);
        discovery.tick().await;
        let outcome = loop {
            tokio::select! {
                () = cancel.cancelled() => break Ok(()),
                _ = discovery.tick() => {
                    if let Err(error) = self.discover(cancel).await {
                        tracing::warn!(%error, "topic discovery failed; keeping the current streams");
                    }
                }
                Some(joined) = self.tasks.join_next() => {
                    if let Some(failure) = self.on_task_end(joined, cancel) {
                        break Err(failure);
                    }
                }
            }
        };
        self.stop_all().await;
        outcome.map(|()| self.running.keys().cloned().collect())
    }

    fn on_task_end(
        &mut self,
        joined: Result<(String, Result<(), String>), tokio::task::JoinError>,
        cancel: &CancellationToken,
    ) -> Option<String> {
        match joined {
            Ok((name, Err(error))) => Some(format!("stream {name} failed: {error}")),
            Err(error) => Some(format!("a stream task panicked: {error}")),
            Ok((name, Ok(()))) if !cancel.is_cancelled() => {
                let replaced = self
                    .running
                    .get(&name)
                    .is_some_and(|running| !running.cancel.is_cancelled());
                match replaced {
                    true => None,
                    false => {
                        tracing::debug!(stream = %name, "previous task for a replaced stream finished");
                        None
                    }
                }
            }
            Ok(_) => None,
        }
    }

    async fn discover(&mut self, cancel: &CancellationToken) -> Result<(), String> {
        let topics = list_topics(&self.config).await?;
        let source = &self.config.source;
        let streams: Vec<Stream> = resolve_streams(&self.config, &topics)
            .map_err(|problems| problems.join("; "))?
            .into_iter()
            .filter(|stream| shard_of(&stream.table, source.shards) == source.shard)
            .collect();
        self.stream_count = streams.len();
        let changed: Vec<Stream> = streams
            .into_iter()
            .filter(|stream| {
                self.running
                    .get(&stream.name)
                    .is_none_or(|running| running.topics != stream.topics)
            })
            .filter(|stream| {
                self.incompatible
                    .get(&stream.name)
                    .is_none_or(|checked| checked.elapsed() >= INCOMPATIBLE_RECHECK)
            })
            .collect();
        if changed.is_empty() {
            return Ok(());
        }
        let changed = self.apply(changed).await?;
        if changed.is_empty() {
            return Ok(());
        }
        let wave = match self.config.source.startup_batch {
            0 => changed.len(),
            size => size,
        };
        for (index, stream) in changed.into_iter().enumerate() {
            if index > 0 && index % wave == 0 {
                tokio::select! {
                    () = cancel.cancelled() => return Ok(()),
                    () = tokio::time::sleep(self.config.source.startup_interval) => {}
                }
            }
            self.start(stream, cancel).await?;
        }
        Ok(())
    }

    /// Create missing objects; streams whose existing objects do not fit are left out and logged, so they cannot stop the rest.
    async fn apply(&mut self, streams: Vec<Stream>) -> Result<Vec<Stream>, String> {
        let existing = read_existing(&self.clients.sql, &self.config)
            .await
            .map_err(|error| format!("cannot read existing objects: {error}"))?;
        let mut plan = build_plan(
            &self.config,
            &streams,
            &existing,
            self.clients.auth.uses_key_pair(),
        );
        let incompatible = &mut self.incompatible;
        plan.streams.retain(|stream_plan| {
            let problems: Vec<String> = stream_plan
                .objects
                .iter()
                .flat_map(|object| object.problems())
                .collect();
            let name = &stream_plan.stream.name;
            match problems.is_empty() {
                true => {
                    incompatible.remove(name);
                }
                false => {
                    tracing::error!(stream = %name, ?problems, "existing objects do not fit; not exporting this stream, checking again in an hour");
                    incompatible.insert(name.clone(), std::time::Instant::now());
                }
            }
            problems.is_empty()
        });
        let created = apply_plan(&self.clients.sql, &plan)
            .await
            .map_err(|error| error.to_string())?;
        for object in created {
            tracing::info!(kind = %object.kind, name = %object.name, "created");
        }
        Ok(plan
            .streams
            .into_iter()
            .map(|stream_plan| stream_plan.stream)
            .collect())
    }

    async fn start(&mut self, stream: Stream, cancel: &CancellationToken) -> Result<(), String> {
        if let Some(previous) = self.running.remove(&stream.name) {
            tracing::info!(stream = %stream.name, topics = ?stream.topics, "topics changed; restarting the stream");
            previous.cancel.cancel();
            let stopped = tokio::time::timeout(REPLACE_WAIT, previous.finished.cancelled()).await;
            if stopped.is_err() {
                return Err(format!(
                    "stream {} did not stop within {REPLACE_WAIT:?}",
                    stream.name
                ));
            }
        }
        let first_topic = stream.topics.first().cloned().unwrap_or_default();
        let settings = stream_settings(&self.config, &first_topic);
        let delivery = self.delivery(&stream, settings.oversized)?;
        let child = cancel.child_token();
        let inputs = StreamInputs {
            config: self.config.clone(),
            stream: stream.clone(),
            settings,
            delivery,
            metrics: self.metrics.clone(),
            gauge: self.gauge.clone(),
            prefetch_kbytes: prefetch_kbytes(
                self.config.source.prefetch_budget.0,
                self.stream_count,
            ),
        };
        let name = stream.name.clone();
        let token = child.clone();
        let finished = CancellationToken::new();
        let done = finished.clone().drop_guard();
        self.tasks.spawn(async move {
            let outcome = run_stream(inputs, token).await;
            drop(done);
            (name, outcome)
        });
        self.running.insert(
            stream.name.clone(),
            Running {
                topics: stream.topics,
                cancel: child,
                finished,
            },
        );
        Ok(())
    }

    fn delivery(&self, stream: &Stream, oversized: OversizedPolicy) -> Result<Delivery, String> {
        let inputs = DeliveryInputs {
            streaming: self.clients.streaming.clone(),
            stage: StageClients {
                http: self.clients.http.clone(),
                auth: self.clients.auth.clone(),
                sql: self.clients.sql.clone(),
                driver: self.clients.driver.clone(),
                account_url: self.clients.account_url.clone(),
            },
            config: self.config.clone(),
            path: load_path(&self.config, self.clients.auth.uses_key_pair()),
            oversized,
        };
        build_delivery(&inputs, stream).map_err(|error| error.to_string())
    }

    async fn stop_all(&mut self) {
        for running in self.running.values() {
            running.cancel.cancel();
        }
        let wait = tokio::time::timeout(Duration::from_secs(120), async {
            while let Some(joined) = self.tasks.join_next().await {
                if let Ok((name, Err(error))) = joined {
                    tracing::warn!(stream = %name, %error, "stream ended with an error during shutdown");
                }
            }
        });
        if wait.await.is_err() {
            tracing::warn!("streams did not stop within two minutes");
            self.tasks.abort_all();
        }
    }
}
