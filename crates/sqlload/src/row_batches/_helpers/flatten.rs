//! Expanding one listed object column of a batch, and marking which columns flattening made.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema};

use crate::column_types::constants::ARROW_TYPE_METADATA_KEY;
use crate::column_types::models::ColumnType;
use crate::load_naming::main::loaded_name::loaded_name;
use crate::load_naming::models::Naming;
use crate::row_batches::_helpers::arrow_json::normalise_column;
use crate::row_batches::constants::{
    DATA_KEYS_SHARE, FLATTEN_SEPARATOR, FLATTENED_METADATA_KEY, FLATTENED_SEPARATOR, MANY_KEYS,
};
use crate::row_batches::main::columns_batch::columns_batch;
use crate::row_batches::main::json_rows::json_rows;
use crate::row_batches::models::{Flatten, FlattenObjects, JsonLayout, JsonRead};
use crate::row_batches::types::FlattenedColumns;

/// `columns` as a batch of `rows` rows whose schema lists `flattened`, the columns flattening made.
pub(crate) fn rebuilt(
    columns: Vec<(Arc<Field>, ArrayRef)>,
    rows: usize,
    flattened: &[String],
) -> Result<RecordBatch, String> {
    let (fields, arrays): (Vec<Arc<Field>>, Vec<ArrayRef>) = columns.into_iter().unzip();
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    let schema = marked(Schema::new(fields), flattened);
    RecordBatch::try_new_with_options(Arc::new(schema), arrays, &options)
        .map_err(|error| error.to_string())
}

/// `schema` listing `flattened` in its metadata (none when empty).
pub(crate) fn marked(schema: Schema, flattened: &[String]) -> Schema {
    match flattened.is_empty() {
        true => schema,
        false => {
            let mut metadata = schema.metadata().clone();
            metadata.insert(
                FLATTENED_METADATA_KEY.to_owned(),
                flattened.join(FLATTENED_SEPARATOR),
            );
            schema.with_metadata(metadata)
        }
    }
}

/// The columns `schema`'s metadata says flattening made.
pub(crate) fn flattened_names(schema: &Schema) -> Vec<String> {
    schema
        .metadata()
        .get(FLATTENED_METADATA_KEY)
        .map(|names| {
            names
                .split(FLATTENED_SEPARATOR)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// Which object each flattened column came from, and the batch's other columns, so no name comes from two places.
pub(crate) struct Names<'a> {
    pub(crate) plain: &'a Schema,
    pub(crate) made: HashMap<String, String>,
}

impl Names<'_> {
    /// Claim `<object>__<key>` for `object`; an error when another column or object already gives it.
    pub(crate) fn claim(&mut self, object: &str, key: &str) -> Result<String, String> {
        let name = format!("{object}{FLATTEN_SEPARATOR}{key}");
        let field = self
            .plain
            .fields()
            .iter()
            .any(|field| *field.name() == name);
        let other = self.made.get(&name).filter(|from| from.as_str() != object);
        let clash = match (field, other) {
            (true, _) => Some("which the data already has".to_owned()),
            (false, Some(from)) => Some(format!("which flattening {from} also gives")),
            (false, None) => None,
        };
        match clash {
            Some(clash) => Err(format!(
                "flatten {object}: the key {key} gives column {name}, {clash}; rename one of them or leave {object} unflattened"
            )),
            None => {
                self.made.insert(name.clone(), object.to_owned());
                Ok(name)
            }
        }
    }
}

/// `batch`'s columns with each column holding objects `flatten` flattens replaced by its keys' columns, recursively, and every column flattening has made in it (sorted), or `None` when nothing flattened. A column an earlier flattening made sits at the path its name spells (`customer__address` below `customer`).
pub(crate) fn expanded_columns(
    batch: &RecordBatch,
    flatten: &Flatten,
) -> Result<Option<FlattenedColumns>, String> {
    let schema = batch.schema();
    let earlier = flattened_names(&schema);
    let mut names = Names {
        plain: &schema,
        made: HashMap::new(),
    };
    let mut columns: Vec<(Arc<Field>, ArrayRef)> = Vec::with_capacity(schema.fields().len());
    let mut expanded = false;
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        let path: Vec<&str> = match earlier.contains(field.name()) {
            true => field.name().split(FLATTEN_SEPARATOR).collect(),
            false => vec![field.name().as_str()],
        };
        expanded |= expand((field, array), &path, flatten, (&mut names, &mut columns))?;
    }
    if !expanded {
        return Ok(None);
    }
    let made: Vec<&str> = columns
        .iter()
        .map(|(field, _)| field.name().as_str())
        .filter(|name| names.made.contains_key(*name))
        .collect();
    let mut flattened: Vec<String> = earlier;
    flattened.extend(made.into_iter().map(str::to_owned));
    flattened.sort();
    flattened.dedup();
    Ok(Some((columns, flattened)))
}

/// Push the column at `path` onto `columns`, or, when it holds objects `flatten` flattens (a listed path, or under `"all"` a column of objects), its keys' columns in its place, each named by `names` and flattened in turn; whether it flattened.
fn expand(
    (field, array): (&Arc<Field>, &ArrayRef),
    path: &[&str],
    flatten: &Flatten,
    (names, columns): (&mut Names<'_>, &mut Vec<(Arc<Field>, ArrayRef)>),
) -> Result<bool, String> {
    let object = flatten.lists(path)
        || (flatten.objects == FlattenObjects::All && holds_objects(field, array));
    if !(object && flatten.flattens(path)?) {
        columns.push((field.clone(), array.clone()));
        return Ok(false);
    }
    let parent = path.join(FLATTEN_SEPARATOR);
    for (key, values) in key_columns(field, array)? {
        let name = names.claim(&parent, key.name())?;
        let below: Vec<&str> = path.iter().copied().chain([key.name().as_str()]).collect();
        let named = Arc::new(key.as_ref().clone().with_name(name));
        expand((&named, &values), &below, flatten, (names, columns))?;
    }
    Ok(true)
}

/// Whether every value of the column is an object (a struct, a map, or a JSON column of objects), and some is.
fn holds_objects(field: &Field, array: &ArrayRef) -> bool {
    if array.logical_null_count() == array.len() {
        return false;
    }
    match array.data_type() {
        DataType::Struct(_) | DataType::Map(..) => true,
        DataType::Utf8 if is_json(field) => {
            let texts = array.as_string::<i32>();
            texts
                .iter()
                .flatten()
                .all(|text| text.trim_start().starts_with('{'))
        }
        _ => false,
    }
}

/// Whether the column holds JSON text.
fn is_json(field: &Field) -> bool {
    field
        .metadata()
        .get(ARROW_TYPE_METADATA_KEY)
        .is_some_and(|kind| *kind == ColumnType::Json.to_string())
}

/// The column's objects as one column per key, each named by its key: a struct's children as they are, JSON objects typed by inference; nothing for a column of NULLs; an error naming the row of a value that is not an object.
pub(crate) fn key_columns(
    field: &Field,
    array: &ArrayRef,
) -> Result<Vec<(Arc<Field>, ArrayRef)>, String> {
    match (array.data_type(), is_json(field)) {
        _ if array.logical_null_count() == array.len() => Ok(Vec::new()),
        (DataType::Struct(_), _) => struct_children(array),
        (DataType::Utf8, true) => json_keys(field, array),
        (DataType::Map(..), _) => {
            let (json_field, json_array) = normalise_column(field.name(), array)?;
            json_keys(&json_field, &json_array)
        }
        (DataType::Utf8, false) => {
            let (row, shape) = text_shape(array);
            Err(shape_error(field.name(), row, shape))
        }
        (data_type, _) => Err(shape_error(
            field.name(),
            first_value(array),
            &shape(data_type),
        )),
    }
}

/// A struct column's children, NULL wherever the struct is.
fn struct_children(array: &ArrayRef) -> Result<Vec<(Arc<Field>, ArrayRef)>, String> {
    let structs = array.as_struct();
    let missing: BooleanArray = (0..structs.len())
        .map(|row| Some(structs.is_null(row)))
        .collect();
    let fields = structs.fields().iter().cloned();
    fields
        .zip(structs.columns())
        .map(|(child_field, child)| {
            let child =
                arrow_select::nullif::nullif(child, &missing).map_err(|error| error.to_string())?;
            Ok((
                Arc::new(child_field.as_ref().clone().with_nullable(true)),
                child,
            ))
        })
        .collect()
}

/// A JSON column's objects read back as typed columns, one per key.
fn json_keys(field: &Field, array: &ArrayRef) -> Result<Vec<(Arc<Field>, ArrayRef)>, String> {
    let document = object_lines(field, array)?;
    let read = JsonRead {
        layout: JsonLayout::Document,
        path: None,
        batch_rows: array.len().max(1),
        projection: None,
    };
    let mut keys: Vec<(Arc<Field>, ArrayRef)> = Vec::new();
    json_rows(document.as_bytes(), &read, |columns| {
        let batch = columns_batch(columns)?;
        keys.extend(
            batch
                .schema()
                .fields()
                .iter()
                .cloned()
                .zip(batch.columns().iter().cloned()),
        );
        Ok(())
    })
    .map_err(|error| format!("flatten {}: {error}", field.name()))?;
    Ok(keys)
}

/// The JSON column as one object per line (`{}` for NULL), or an error naming the first row that holds something else.
fn object_lines(field: &Field, array: &ArrayRef) -> Result<String, String> {
    let texts = array.as_string::<i32>();
    let mut document = String::new();
    for row in 0..array.len() {
        let text = match texts.is_valid(row) {
            true => texts.value(row).trim(),
            false => "null",
        };
        let line = match (text, text.starts_with('{'), text.starts_with('[')) {
            ("null", _, _) => "{}",
            (object, true, _) => object,
            (_, false, true) => return Err(shape_error(field.name(), row, "a list")),
            (_, false, false) => return Err(shape_error(field.name(), row, "a scalar")),
        };
        document.push_str(line);
        document.push('\n');
    }
    Ok(document)
}

/// The first row of a plain text column that does not hold an object's JSON, and its shape: text, or, when other rows hold objects (a column mixing objects with numbers or booleans became text), a scalar.
fn text_shape(array: &ArrayRef) -> (usize, &'static str) {
    let texts = array.as_string::<i32>();
    let object = |row: usize| texts.value(row).trim_start().starts_with('{');
    let valid = (0..array.len()).filter(|row| texts.is_valid(*row));
    let mixed = valid.clone().any(object);
    let row = valid
        .clone()
        .find(|row| !object(*row))
        .unwrap_or_else(|| first_value(array));
    match mixed {
        true => (row, "a scalar (other rows hold objects)"),
        false => (row, "a text value"),
    }
}

fn first_value(array: &ArrayRef) -> usize {
    (0..array.len())
        .find(|row| array.is_valid(*row))
        .unwrap_or_default()
}

/// A value's shape in words.
pub(crate) fn shape(data_type: &DataType) -> String {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "a text value".to_owned(),
        DataType::Boolean => "a boolean".to_owned(),
        DataType::Date32 | DataType::Date64 => "a date".to_owned(),
        DataType::Timestamp(..) => "a timestamp".to_owned(),
        DataType::Decimal128(..) | DataType::Decimal256(..) => "a decimal".to_owned(),
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..) => {
            "a list".to_owned()
        }
        DataType::Map(..) => "a map".to_owned(),
        numeric if numeric.is_numeric() => "a number".to_owned(),
        other => format!("a {other} value"),
    }
}

/// The error for a listed column whose row `row` (0-based, in its batch) holds `shape` instead of an object.
pub(crate) fn shape_error(object: &str, row: usize, shape: &str) -> String {
    format!(
        "flatten {object}: row {} of a batch holds {shape}, not an object; the shape changed, so leave {object} unflattened or fix the source",
        row + 1
    )
}

/// The keys below `object` that load as columns (not objects themselves), as the data spells them: the sampled `columns` named `<object>__…`.
pub(crate) fn leaf_keys(object: &str, (objects, columns): (&[String], &[String])) -> Vec<String> {
    let prefix = format!("{object}{FLATTEN_SEPARATOR}");
    columns
        .iter()
        .filter_map(|column| column.strip_prefix(&prefix))
        .filter(|key| !objects.contains(&format!("{prefix}{key}")))
        .map(str::to_owned)
        .collect()
}

/// Whether an object's keys look like data rather than field names: more than [`MANY_KEYS`] of them, or more than [`DATA_KEYS_SHARE`] percent of them holding a digit or not a plain name (snake_case changes more than its case).
pub(crate) fn keys_are_data(keys: &[String]) -> bool {
    let data = keys.iter().filter(|key| is_data_key(key)).count();
    keys.len() > MANY_KEYS || (!keys.is_empty() && data * 100 > keys.len() * DATA_KEYS_SHARE)
}

/// Whether `key` looks like data rather than a field name: it holds a digit, or snake_case changes more than its case.
pub(crate) fn is_data_key(key: &str) -> bool {
    key.bytes().any(|byte| byte.is_ascii_digit())
        || loaded_name(Naming::SnakeCase, key, None) != key.to_lowercase()
}
