use crate::adapters::contract::models::TableName;

/// `` `p`.`raw`.`orders` ``'s name.
pub(crate) fn orders() -> TableName {
    TableName {
        database: Some("p".to_owned()),
        schema: "raw".to_owned(),
        table: "orders".to_owned(),
    }
}

/// A planned column `name` of `column_type`, mapped from the source.
pub(crate) fn desired(
    name: &str,
    column_type: crate::column_types::models::ColumnType,
) -> crate::schema_plan::models::DesiredColumn {
    crate::schema_plan::models::DesiredColumn {
        name: name.to_owned(),
        column_type,
        origin: crate::schema_plan::models::ColumnOrigin::Source,
    }
}

/// A batch of the named `columns`.
pub(crate) fn batch_of(
    columns: Vec<(&str, arrow_array::ArrayRef)>,
) -> Result<arrow_array::RecordBatch, String> {
    arrow_array::RecordBatch::try_from_iter(columns).map_err(|error| error.to_string())
}

/// Each column of `batch` as `name: type = value|value`, NULLs as `null`, columns joined by `; `.
pub(crate) fn shown(batch: &arrow_array::RecordBatch) -> Result<String, String> {
    let options = arrow_cast::display::FormatOptions::default().with_null("null");
    let mut shown: Vec<String> = Vec::new();
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        let formatter = arrow_cast::display::ArrayFormatter::try_new(array, &options)
            .map_err(|error| error.to_string())?;
        let values: Vec<String> = (0..array.len())
            .map(|row| formatter.value(row).to_string())
            .collect();
        shown.push(format!(
            "{}: {} = {}",
            field.name(),
            field.data_type(),
            values.join("|")
        ));
    }
    Ok(shown.join("; "))
}

/// Ids 0 to `rows - 1`, each with a note of `note_bytes` `x`s.
pub(crate) fn noted_ids(rows: i64, note_bytes: usize) -> Result<arrow_array::RecordBatch, String> {
    let note = "x".repeat(note_bytes);
    batch_of(vec![
        (
            "id",
            std::sync::Arc::new(arrow_array::Int64Array::from_iter_values(0..rows)),
        ),
        (
            "note",
            std::sync::Arc::new(arrow_array::StringArray::from_iter_values(
                (0..rows).map(|_| note.as_str()),
            )),
        ),
    ])
}

/// The ids `requests` decode back to, read as Arrow IPC from each one's schema and rows, as `first..=last (count)`.
pub(crate) fn decoded_ids(
    requests: &[(crate::bigquery_api::models::AppendRowsRequest, usize)],
) -> Result<String, String> {
    let mut ids: Vec<i64> = Vec::new();
    for (request, _) in requests {
        let data = request.arrow_rows.clone().unwrap_or_default();
        let mut bytes = data.writer_schema.unwrap_or_default().serialized_schema;
        bytes.extend(data.rows.unwrap_or_default().serialized_record_batch);
        let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(|error| error.to_string())?;
        for decoded in reader {
            let decoded = decoded.map_err(|error| error.to_string())?;
            let column = arrow_array::cast::AsArray::as_primitive::<arrow_array::types::Int64Type>(
                decoded.column(0),
            );
            ids.extend(column.values().iter().copied());
        }
    }
    Ok(format!(
        "{}..={} ({})",
        ids.first().copied().unwrap_or(-1),
        ids.last().copied().unwrap_or(-1),
        ids.len()
    ))
}

/// A list of `decimal(10,2)` values per row, given as hundredths; `None` is a NULL list or a NULL element.
pub(crate) fn decimal_lists(
    rows: Vec<Option<Vec<Option<i128>>>>,
) -> Result<arrow_array::ListArray, String> {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    let lists =
        arrow_array::ListArray::from_iter_primitive::<arrow_array::types::Decimal128Type, _, _>(
            rows,
        );
    let values = lists
        .values()
        .as_primitive::<arrow_array::types::Decimal128Type>()
        .clone()
        .with_precision_and_scale(10, 2)
        .map_err(|error| error.to_string())?;
    arrow_array::ListArray::try_new(
        std::sync::Arc::new(arrow_schema::Field::new(
            "item",
            values.data_type().clone(),
            true,
        )),
        lists.offsets().clone(),
        std::sync::Arc::new(values),
        lists.nulls().cloned(),
    )
    .map_err(|error| error.to_string())
}
