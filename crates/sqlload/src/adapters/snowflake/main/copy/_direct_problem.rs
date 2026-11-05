//! Decide whether Snowflake can load a files source's files itself.

use crate::adapters::snowflake::_helpers::dialect::values::{copyable_type, plain_key};
use crate::adapters::snowflake::main::copy::_stage_covers::stage_covers;
use crate::adapters::snowflake::models::{CopyValue, DirectCheck};
use crate::project::models::MaskMethod;

/// Why Snowflake cannot load the files exactly as SQLLoad would with a direct `COPY` from the stage (`stage_url`, or files copied to an internal stage when `None`), or `None` when it can.
pub(in crate::adapters) fn direct_problem(check: &DirectCheck<'_>) -> Option<String> {
    if let Some(stage_url) = check.stage_url
        && !stage_covers(stage_url, check.source)
    {
        return Some(format!("the files are outside the stage url {stage_url}"));
    }
    let odd_key = std::iter::once(check.source.prefix.as_str())
        .chain(
            check
                .files
                .iter()
                .map(|data| data.file.relative_key.as_str()),
        )
        .find(|key| !plain_key(key));
    if let Some(key) = odd_key {
        return Some(format!(
            "{key} has characters a stage path cannot name exactly"
        ));
    }
    for (name, method) in check.masks {
        let column = check.columns.iter().find(|column| column.name == *name);
        let text_file = column.is_some_and(|column| match &column.value {
            CopyValue::File { file_type, .. } => file_type.as_ref().is_none_or(|file_type| {
                *file_type == crate::column_types::models::ColumnType::Text
            }),
            _ => true,
        });
        match method {
            MaskMethod::Hmac => {
                return Some(format!(
                    "{name} is masked with hmac, which Snowflake cannot compute without its key in SQL"
                ));
            }
            MaskMethod::Hash
                if !text_file
                    || column.is_some_and(|column| {
                        column.column_type != crate::column_types::models::ColumnType::Text
                    }) =>
            {
                return Some(format!(
                    "{name} is hashed but is not text in the files; SQLLoad hashes its text"
                ));
            }
            MaskMethod::Hash => {}
        }
    }
    for (name, arrow_type) in check.refused {
        let read = check
            .columns
            .iter()
            .any(|column| column.name == *name && matches!(column.value, CopyValue::File { .. }));
        if read {
            return Some(format!(
                "{name} is {arrow_type} in the files, which the direct COPY does not load exactly"
            ));
        }
    }
    for column in check.columns {
        if matches!(
            column.value,
            CopyValue::SourceFile
                | CopyValue::SourceVersion
                | CopyValue::SourceRow
                | CopyValue::Literal(_)
        ) {
            continue;
        }
        if !copyable_type(&column.column_type) {
            return Some(format!(
                "{} is {}, which the direct COPY does not load exactly",
                column.name, column.column_type
            ));
        }
        if let CopyValue::File {
            file_type: Some(file_type),
            ..
        } = &column.value
            && *file_type != column.column_type
            && !(*file_type == crate::column_types::models::ColumnType::Text
                && column.column_type == crate::column_types::models::ColumnType::Json)
        {
            return Some(format!(
                "{} is {file_type} in the files but loads as {}; SQLLoad converts it",
                column.name, column.column_type
            ));
        }
    }
    None
}
