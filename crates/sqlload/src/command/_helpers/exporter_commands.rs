use std::path::Path;

use crate::command::_helpers::commands::{apply, check, plan, run_export, status, tables};
use crate::command::models::{Command, LoaderArgs};
use crate::config::main::load_config::load_config;

/// Run an exporter command against the configuration at `path`.
pub(crate) async fn run_exporter_command(
    path: &Path,
    command: Command,
) -> Result<Vec<String>, String> {
    let mut config = load_config(path).map_err(|error| error.to_string())?;
    if let Command::Plan(args) | Command::Apply(args) = &command
        && *args != LoaderArgs::default()
    {
        return Err("table selection and loader flags apply to loader projects; this file is an exporter configuration".to_owned());
    }
    match command {
        Command::Check => check(&config).await,
        Command::Plan(_) => plan(&config).await,
        Command::Tables { topics_file } => tables(&config, topics_file.as_deref()).await,
        Command::Apply(_) => apply(&config).await,
        Command::Load(_) => Err(
            "load runs loader projects; this file is an exporter configuration (use run)"
                .to_owned(),
        ),
        Command::Run {
            max_duration,
            shard,
        } => {
            config.source.shard = shard.unwrap_or(config.source.shard);
            match config.source.shard < config.source.shards {
                true => run_export(config, max_duration).await,
                false => Err(format!(
                    "--shard {} must be below source.shards {}",
                    config.source.shard, config.source.shards
                )),
            }
        }
        Command::Status => status(&config).await,
    }
}
