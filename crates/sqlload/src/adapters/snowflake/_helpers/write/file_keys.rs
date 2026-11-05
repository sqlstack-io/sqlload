//! The keys of one JSON batch's records, read by Snowflake from the stage.

use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::_helpers::dialect::json_values::{
    json_file_format, key_type, keys_query,
};
use crate::adapters::snowflake::_helpers::dialect::values::plain_key;
use crate::adapters::snowflake::constants::KEYS_FILE_FORMAT;
use crate::adapters::snowflake::main::stage::_stage_reference::stage_reference;
use crate::adapters::snowflake::models::{SnowflakeSql, SnowflakeWriter, StageRoute};
use crate::column_types::models::ColumnType;
use crate::files_source::models::{DataFile, SourceFormat};
use crate::loader::errors::LoaderError;
use crate::loader::models::TableSpec;
use crate::sql_api::main::execute_script_rows::execute_script_rows;

/// Every key the records of a JSON or NDJSON batch's `files` hold and the type its values suggest (`None` when all were NULL), read by Snowflake from the external stage the writer loads through, so a key the sample missed is found before the `COPY`, which would silently skip it; `None` when Snowflake does not read these files itself.
impl SnowflakeWriter {
    pub(in crate::adapters) async fn file_keys(
        &self,
        spec: &TableSpec,
        files: &[DataFile],
    ) -> Result<Option<Vec<(String, Option<ColumnType>)>>, LoaderError> {
        let (Some(StageRoute::External(stage)), Some(source)) = (&self.route, &spec.files) else {
            return Ok(None);
        };
        let json = matches!(
            source.encoding.format,
            SourceFormat::Json | SourceFormat::Ndjson
        );
        let Some(folder) = source
            .location
            .prefix
            .strip_prefix(&stage.location.prefix)
            .filter(|_| json && !files.is_empty())
        else {
            return Ok(None);
        };
        let keys: Vec<&str> = files
            .iter()
            .map(|data| data.file.relative_key.as_str())
            .collect();
        if !plain_key(&source.location.prefix) || !keys.iter().all(|key| plain_key(key)) {
            return Ok(None);
        }
        let alternatives: Vec<String> = keys.iter().map(|key| escaped(key)).collect();
        let pattern = format!(
            "{}({})",
            escaped(&source.location.prefix),
            alternatives.join("|")
        );
        let format = SnowflakeSql.render_qualified_name(&TableName {
            table: KEYS_FILE_FORMAT.to_owned(),
            ..stage.home.clone()
        });
        let script = [
            format!(
                "CREATE OR REPLACE TEMPORARY FILE FORMAT {format} {}",
                json_file_format(source.encoding.compression)
            ),
            keys_query(
                &format!("{}/{folder}", stage_reference(stage)),
                &format,
                &pattern,
            ),
        ];
        let results = execute_script_rows(&self.destination.sql, &script).await?;
        let rows = results
            .last()
            .map(|result| result.rows.as_slice())
            .unwrap_or_default();
        let mut found: Vec<(String, Option<ColumnType>)> = Vec::with_capacity(rows.len());
        for row in rows {
            let text = |index: usize| row.get(index).cloned().flatten().unwrap_or_default();
            let types: Vec<String> = serde_json::from_str(&text(1)).map_err(|error| {
                LoaderError::Invalid(format!("unexpected key types {}: {error}", text(1)))
            })?;
            let flag = |index: usize| text(index).eq_ignore_ascii_case("true");
            found.push((text(0), key_type(&types, (flag(2), flag(3)))));
        }
        Ok(Some(found))
    }
}

/// `key` as a regular expression matching itself: each symbol a stage path may hold in a class of its own.
fn escaped(key: &str) -> String {
    key.chars()
        .map(|character| match character.is_ascii_alphanumeric() {
            true => character.to_string(),
            false => format!("[{character}]"),
        })
        .collect()
}
