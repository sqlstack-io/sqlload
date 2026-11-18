//! A files batch loaded by BigQuery reading the source's own Parquet files in GCS.

use serde_json::Value;

use crate::adapters::bigquery::_helpers::write::files_query_job::files_query_job;
use crate::adapters::bigquery::_helpers::write::gcs_insert::{GcsBatch, gcs_insert_statement};
use crate::adapters::bigquery::main::plan::_direct_problem::direct_problem;
use crate::adapters::bigquery::models::{BigQuerySql, BigQueryWriter};
use crate::adapters::contract::main::_batch_direct_problem::batch_direct_problem;
use crate::adapters::contract::main::_file_constants::file_constants;
use crate::adapters::contract::main::_footer_rows::footer_rows;
use crate::adapters::contract::models::{DirectBatch, DirectLoaded};
use crate::adapters::contract::types::SqlAdapter;
use crate::bigquery_api::main::load_job::load_job;
use crate::bigquery_api::main::run_query::run_query;
use crate::files_source::constants::SOURCE_FILE_COLUMN;
use crate::load_naming::main::metadata_name::metadata_name;
use crate::load_naming::models::MetadataRole;
use crate::loader::errors::LoaderError;
use crate::store_paths::models::StoreKind;

impl BigQueryWriter {
    /// Load `batch` by having BigQuery read its Parquet files where they lie in GCS, as the temporary external table of one query job: one `INSERT` (one transaction) into the staging or build table the load publishes from, or into the table itself. A batch whose own footers hold a column a cast would not keep exactly fails, and so does one whose inserted rows, as the job reports them, differ from its footers'; an append into the table itself first takes back what it committed. Files never fall back to SQLLoad mid-load. `None` when the table's files aren't in GCS.
    pub(in crate::adapters) async fn copy_files(
        &mut self,
        batch: &DirectBatch<'_>,
    ) -> Result<Option<DirectLoaded>, LoaderError> {
        let Some(source) = batch.spec.files.as_ref() else {
            return Ok(None);
        };
        let StoreKind::Gcs { .. } = source.location.store else {
            return Ok(None);
        };
        if let Some(problem) =
            batch_direct_problem(batch, source, direct_problem(batch.spec, batch.read))
        {
            return Err(LoaderError::Invalid(format!(
                "BigQuery cannot load this batch's files exactly by reading them itself, and they never go through SQLLoad instead: {problem}"
            )));
        }
        let target = &batch.target;
        let (into, columns) = match (target.staging, target.builds) {
            (Some(staging), false) => (staging, target.columns),
            (staging, _) => (staging.unwrap_or(target.table), batch.write_columns),
        };
        let mut gcs = GcsBatch {
            files: Vec::with_capacity(batch.files.len()),
            in_files: Vec::new(),
        };
        for data in batch.files {
            gcs.files.push((
                format!("{}{}", source.location, data.file.relative_key),
                file_constants(batch, data, columns)?,
            ));
            let Some(footer) = batch.footers.parquet.get(&data.file.store_key()) else {
                continue;
            };
            for field in footer.schema().fields() {
                let loaded = batch.spec.source_table.loaded_as(field.name()).to_owned();
                if !gcs.in_files.iter().any(|(name, _)| *name == loaded) {
                    gcs.in_files.push((loaded, field.name().clone()));
                }
            }
        }
        let statement = gcs_insert_statement(
            (&BigQuerySql.render_qualified_name(into), columns),
            target,
            &gcs,
            batch.loaded_at,
        );
        let uris: Vec<String> = gcs.files.iter().map(|(uri, _)| uri.clone()).collect();
        let job_id = format!("sqlload_files_{}", uuid::Uuid::new_v4().simple());
        let job = files_query_job(
            (&job_id, self.destination.api.location()),
            &statement,
            &uris,
        );
        let resource = load_job(&self.destination.api, &job, None).await?;
        if let Some(error) = job_failure(&resource) {
            return Err(error);
        }
        let reported = &resource["statistics"]["query"]["numDmlAffectedRows"];
        let expected = footer_rows(batch.footers);
        if reported.as_str().and_then(|text| text.parse::<u64>().ok()) != Some(expected) {
            let error = LoaderError::Invalid(format!(
                "BigQuery reported inserting {reported} rows from this batch's files, but their footers hold {expected}"
            ));
            return Err(self.undo(batch, &uris, error).await);
        }
        Ok(Some(match target.staging {
            Some(_) => DirectLoaded::Staged(expected),
            None => DirectLoaded::Committed(expected),
        }))
    }

    /// After `error`, take back the rows a batch going straight into the table committed: its load's rows from its files `uris`, which a later run would otherwise skip as loaded. A batch landing in a staging table needs nothing, as the load drops it, and neither does a job that failed, as its `INSERT` committed nothing.
    async fn undo(
        &self,
        batch: &DirectBatch<'_>,
        uris: &[String],
        error: LoaderError,
    ) -> LoaderError {
        let target = &batch.target;
        if target.staging.is_some() {
            return error;
        }
        let literal = |text: &str| BigQuerySql.render_loader_value_literal(text);
        let paths: Vec<String> = uris.iter().map(|uri| literal(uri)).collect();
        let statement = format!(
            "DELETE FROM {} WHERE {} = {} AND {} IN ({})",
            BigQuerySql.render_qualified_name(target.table),
            BigQuerySql.render_identifier(metadata_name(target.compat, MetadataRole::LoadId)),
            literal(target.load_id),
            BigQuerySql.render_identifier(SOURCE_FILE_COLUMN),
            paths.join(", ")
        );
        match run_query(&self.destination.api, &statement).await {
            Ok(_) => error,
            Err(undo) => LoaderError::Invalid(format!(
                "{error}; taking back the rows it committed then failed too, so the table holds them: {undo}"
            )),
        }
    }
}

/// Why the finished query job `resource` loaded nothing, when it failed.
fn job_failure(resource: &Value) -> Option<LoaderError> {
    let error = &resource["status"]["errorResult"];
    (!error.is_null()).then(|| {
        LoaderError::Invalid(format!(
            "BigQuery failed to load this batch's files: {}: {}",
            error["reason"].as_str().unwrap_or_default(),
            error["message"].as_str().unwrap_or_default()
        ))
    })
}
