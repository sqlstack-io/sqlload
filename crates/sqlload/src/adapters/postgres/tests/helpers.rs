use crate::adapters::contract::models::TableName;
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

/// `"app"."orders"`'s name, in a database the session connects to.
pub(crate) fn orders() -> TableName {
    TableName {
        database: Some("mart".to_owned()),
        schema: "app".to_owned(),
        table: "orders".to_owned(),
    }
}

/// A planned column `name` of `column_type`, mapped from the source.
pub(crate) fn desired(name: &str, column_type: ColumnType) -> DesiredColumn {
    DesiredColumn {
        name: name.to_owned(),
        column_type,
        origin: ColumnOrigin::Source,
    }
}

/// Each column of `batch` as `name = value|value`, NULLs as `null`, columns joined by `; `.
pub(crate) fn shown(batch: &arrow_array::RecordBatch) -> Result<String, String> {
    let options = arrow_cast::display::FormatOptions::default().with_null("null");
    let mut shown: Vec<String> = Vec::new();
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        let formatter = arrow_cast::display::ArrayFormatter::try_new(array, &options)
            .map_err(|error| error.to_string())?;
        let values: Vec<String> = (0..array.len())
            .map(|row| formatter.value(row).to_string())
            .collect();
        shown.push(format!("{} = {}", field.name(), values.join("|")));
    }
    Ok(shown.join("; "))
}

/// `columns` (each read back as its wire type) as one batch, with a column outside them that the staging table lacks, encoded by binary `COPY` in pieces of `chunk_bytes` and decoded by the Postgres source's reader: `split: <whether the stream was cut>`, then the decoded columns as [`shown`] gives them; an error up to its first `: `.
pub(crate) fn round_trip(
    columns: &[(
        &str,
        ColumnType,
        crate::postgres_source::models::WireType,
        arrow_array::ArrayRef,
    )],
    chunk_bytes: usize,
) -> Result<String, String> {
    let extra: arrow_array::ArrayRef = std::sync::Arc::new(arrow_array::Int64Array::from(vec![
        7;
        columns
            .first()
            .map_or(0, |(_, _, _, array)| array.len())
    ]));
    let batch = arrow_array::RecordBatch::try_from_iter(
        columns
            .iter()
            .map(|(name, _, _, array)| (*name, array.clone()))
            .chain([("not_staged", extra)]),
    )
    .map_err(|error| error.to_string())?;
    let staged: Vec<DesiredColumn> = columns
        .iter()
        .map(|(name, column_type, _, _)| desired(name, column_type.clone()))
        .collect();
    let cut = |error: String| error.split(": ").next().unwrap_or_default().to_owned();
    let chunks =
        crate::adapters::postgres::_helpers::copy_rows::copy_chunks(&batch, &staged, chunk_bytes)
            .map_err(cut)?;
    let read = columns
        .iter()
        .map(
            |(name, column_type, wire, _)| crate::postgres_source::models::ReadColumn {
                name: (*name).to_owned(),
                expression: String::new(),
                wire: *wire,
                output: column_type.clone(),
            },
        )
        .collect();
    let batches = crate::postgres_source::main::decode_copy::decode_copy(
        "staging",
        read,
        chunks.iter().map(|chunk| chunk.as_ref()),
        (usize::MAX, usize::MAX),
    )
    .map_err(|error| cut(error.to_string()))?;
    let decoded = batches.first().ok_or("no batch decoded")?;
    Ok(format!("split: {}; {}", chunks.len() > 1, shown(decoded)?))
}

/// A `numeric` as Postgres sends it: digit count, weight, sign, display scale, then the base-10000 digits.
pub(crate) fn numeric_wire(digits: &[i16], weight: i16, sign: u16, scale: u8) -> Vec<u8> {
    let mut bytes: Vec<u8> = Vec::new();
    bytes.extend_from_slice(
        &i16::try_from(digits.len())
            .unwrap_or_default()
            .to_be_bytes(),
    );
    bytes.extend_from_slice(&weight.to_be_bytes());
    bytes.extend_from_slice(&sign.to_be_bytes());
    bytes.extend_from_slice(&u16::from(scale).to_be_bytes());
    for digit in digits {
        bytes.extend_from_slice(&digit.to_be_bytes());
    }
    bytes
}
