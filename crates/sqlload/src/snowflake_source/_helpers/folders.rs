use bytes::Bytes;

use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::read_file::read_file;
use crate::file_store::models::OpenFolder;
use crate::project::models::UnloadStage;
use crate::snowflake_source::constants::USER_STAGE_FOLDER;
use crate::snowflake_source::errors::SnowflakeSourceError;
use crate::snowflake_source::models::{SnowflakeReader, UnloadFolder};
use crate::sql_api::main::execute_sql::execute_sql;
use crate::stage_api::main::staged_file_bytes::staged_file_bytes;
use crate::stage_api::models::StageDownloads;
use crate::store_paths::main::resolve_location::resolve_location;
use crate::store_paths::models::{Location, StoreKind};

/// Where run `run` unloads: a folder of the user stage, of a named stage, or of an S3 URL written through a storage integration.
pub(crate) async fn unload_folder(
    reader: &SnowflakeReader,
    run: &str,
) -> Result<UnloadFolder, SnowflakeSourceError> {
    let invalid = |reason: String| SnowflakeSourceError::Invalid(reason);
    let Some(stage) = &reader.stage else {
        return Ok(UnloadFolder::Internal {
            stage_path: format!("{USER_STAGE_FOLDER}{run}/"),
        });
    };
    match stage {
        UnloadStage {
            url: Some(url),
            storage_integration: Some(integration),
            ..
        } => Ok(UnloadFolder::External {
            copy_target: format!("'{}{run}/'", with_slash(url).replace('\'', "''")),
            options: format!(" STORAGE_INTEGRATION = {integration}"),
            folder: run_folder(url, stage.region.clone(), run)?,
        }),
        UnloadStage {
            name: Some(name), ..
        } => {
            let (qualifier, stage_name) = name.rsplit_once('.').unwrap_or(("", name));
            let listed = execute_sql(
                &reader.sql,
                &format!(
                    "SHOW STAGES LIKE '{}' IN SCHEMA {qualifier}",
                    stage_name.replace('\'', "''")
                ),
            )
            .await?;
            let row = listed
                .rows
                .first()
                .ok_or_else(|| invalid(format!("stage {name} does not exist or is not visible")))?;
            let value = |column: &str| listed.value(row, column).unwrap_or_default().to_owned();
            match value("type").as_str() {
                "INTERNAL" => Ok(UnloadFolder::Internal {
                    stage_path: format!("@{name}/{run}/"),
                }),
                _ => {
                    let region = Some(value("region")).filter(|region| !region.is_empty());
                    Ok(UnloadFolder::External {
                        copy_target: format!("@{name}/{run}/"),
                        options: String::new(),
                        folder: run_folder(&value("url"), region, run)?,
                    })
                }
            }
        }
        _ => Err(invalid(
            "stage takes either url and storage_integration, or name".to_owned(),
        )),
    }
}

fn with_slash(url: &str) -> String {
    match url.ends_with('/') {
        true => url.to_owned(),
        false => format!("{url}/"),
    }
}

/// The folder `run` of S3 URL `url`, which SQLLoad reads with its own credentials.
fn run_folder(
    url: &str,
    region: Option<String>,
    run: &str,
) -> Result<Location, SnowflakeSourceError> {
    let location = resolve_location(None, &with_slash(url))
        .map_err(|error| SnowflakeSourceError::Invalid(format!("stage url {url:?}: {error}")))?;
    let StoreKind::S3 { bucket, .. } = location.store else {
        return Err(SnowflakeSourceError::Invalid(format!(
            "stage url {url:?} is not on S3, which is the only external stage SQLLoad reads"
        )));
    };
    Ok(Location {
        store: StoreKind::S3 {
            bucket,
            region,
            endpoint: None,
        },
        prefix: format!("{}{run}/", location.prefix),
    })
}

/// The unloaded files of one run, ready to fetch in any order.
pub(crate) enum Unloaded {
    /// Internal-stage files, with their S3 location, temporary credentials and keys.
    Internal(StageDownloads),
    /// Files in an S3 folder.
    External {
        folder: OpenFolder,
        files: Vec<String>,
    },
}

impl Unloaded {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Internal(downloads) => downloads.files.len(),
            Self::External { files, .. } => files.len(),
        }
    }

    /// File `index`'s name, for messages.
    pub(crate) fn name(&self, index: usize) -> String {
        match self {
            Self::Internal(downloads) => downloads.files.get(index).map(|file| file.key.clone()),
            Self::External { folder, files } => files
                .get(index)
                .map(|key| format!("{}{key}", folder.location)),
        }
        .unwrap_or_default()
    }

    /// File `index`'s Parquet bytes, decrypted when it comes from an internal stage.
    pub(crate) async fn fetch(
        &self,
        http: &reqwest::Client,
        index: usize,
    ) -> Result<Bytes, SnowflakeSourceError> {
        let failed = |reason: String| SnowflakeSourceError::File {
            file: self.name(index),
            reason,
        };
        match self {
            Self::Internal(downloads) => {
                let file = downloads
                    .files
                    .get(index)
                    .ok_or_else(|| failed("no such file".to_owned()))?;
                staged_file_bytes(http, &downloads.target, file)
                    .await
                    .map(Bytes::from)
                    .map_err(|error| failed(error.to_string()))
            }
            Self::External { folder, files } => {
                let key = files
                    .get(index)
                    .ok_or_else(|| failed("no such file".to_owned()))?;
                Ok(read_file(folder, key).await?)
            }
        }
    }
}

/// The files `folder` holds after the unload: from `GET` on an internal stage (its credentials and keys, without downloading), or listed in S3.
pub(crate) async fn unloaded_files(
    reader: &SnowflakeReader,
    folder: &UnloadFolder,
) -> Result<Unloaded, SnowflakeSourceError> {
    match folder {
        UnloadFolder::Internal { stage_path } => {
            let downloads = reader.driver.stage_downloads(stage_path).await?;
            if downloads.files.is_empty() {
                return Err(SnowflakeSourceError::Invalid(format!(
                    "GET found no files in {stage_path}, though COPY INTO unloaded rows there"
                )));
            }
            Ok(Unloaded::Internal(downloads))
        }
        UnloadFolder::External { folder, .. } => {
            let opened = open_folder(folder)?;
            let files: Vec<String> = list_files(&opened)
                .await?
                .into_iter()
                .map(|file| file.relative_key)
                .collect();
            Ok(Unloaded::External {
                folder: opened,
                files,
            })
        }
    }
}

/// Remove run folder `folder` and everything in it.
pub(crate) async fn remove_folder(
    reader: &SnowflakeReader,
    folder: &UnloadFolder,
) -> Result<(), SnowflakeSourceError> {
    match folder {
        UnloadFolder::Internal { stage_path } => {
            execute_sql(&reader.sql, &format!("REMOVE {stage_path}")).await?;
        }
        UnloadFolder::External { folder, .. } => {
            let opened = open_folder(folder)?;
            let keys: Vec<String> = list_files(&opened)
                .await?
                .into_iter()
                .map(|file| file.relative_key)
                .collect();
            delete_files(&opened, &keys).await?;
        }
    }
    Ok(())
}
