//! Splitting list values into child tables' rows, level by level.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, Field, Schema};
use serde::de::{Deserialize, Deserializer, MapAccess, Visitor};
use serde_json::value::RawValue;

use crate::column_types::constants::ARROW_TYPE_METADATA_KEY;
use crate::column_types::models::ColumnType;
use crate::load_naming::constants::VALUE_COLUMN;
use crate::load_naming::main::child_table_name::child_table_name;
use crate::load_naming::main::loaded_name::loaded_name;
use crate::load_naming::main::metadata_name::metadata_name;
use crate::load_naming::models::{MetadataRole, Naming};
use crate::row_batches::_helpers::arrow_json::normalise_column;
use crate::row_batches::_helpers::json_seed::raw_cell;
use crate::row_batches::constants::FLATTEN_SEPARATOR;
use crate::row_batches::main::columns_batch::columns_batch;
use crate::row_batches::models::{ChildBatch, RowColumns, Unnest, UnnestRequest};
use crate::row_batches::types::ListChoices;
use crate::row_ids::main::child_row_id::child_row_id;

/// The key dlt gives a list that is itself an item of a list, so its items land a level down (`items__list`).
const NESTED_LIST_KEY: &str = "list";

/// One child table's rows as they are gathered: the items' data cells, and each row's parent, position, own id and root.
pub(crate) struct Level {
    path: String,
    table: String,
    rows: RowColumns,
    parents: Vec<String>,
    positions: Vec<i64>,
    ids: Vec<String>,
    roots: Vec<String>,
    root_rows: Vec<usize>,
}

/// What every level of one split shares: which fields unnest, how deep, and how names are made.
pub(crate) struct Split<'a> {
    pub(crate) unnest: &'a Unnest,
    pub(crate) max_depth: u32,
    pub(crate) naming: Naming,
    /// The longest name dlt's naming gives on the destination.
    pub(crate) name_limit: Option<usize>,
    /// Whether an object inside an item flattens into `<key>__<inner>` fields.
    pub(crate) flatten_objects: bool,
    /// Under `"all"`, the fields chosen as list fields or not so far, chosen further as values decide.
    pub(crate) choices: &'a mut ListChoices,
    pub(crate) levels: Vec<Level>,
}

/// What a field's values say about it being a list field: whether some value is a list with items, and whether some value is neither NULL nor a list.
#[derive(Default)]
struct Seen {
    list: bool,
    other: bool,
}

/// Where one list's items go: the list field's dotted path and child table, its depth (1 for a field of the loaded table), and the row it belongs to with that row's root.
struct ListPlace<'a> {
    path: &'a str,
    table: &'a str,
    depth: u32,
    parent: &'a str,
    root: Option<&'a str>,
    /// The root's row in the split batch.
    root_row: usize,
}

/// An object's keys and raw values in the order the JSON writes them.
struct OrderedObject(Vec<(String, Box<RawValue>)>);

impl<'de> Deserialize<'de> for OrderedObject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(OrderedVisitor)
    }
}

/// Reads an object's entries in order.
struct OrderedVisitor;

impl<'de> Visitor<'de> for OrderedVisitor {
    type Value = OrderedObject;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<OrderedObject, A::Error> {
        let mut entries: Vec<(String, Box<RawValue>)> = Vec::new();
        while let Some(entry) = map.next_entry::<String, Box<RawValue>>()? {
            entries.push(entry);
        }
        Ok(OrderedObject(entries))
    }
}

impl Split<'_> {
    /// Move the items of the column `field` to child rows when it is a list field (any column holding a list under `"all"`, a listed one always), each row's items under its id from `request`; whether it moved.
    pub(crate) fn take_lists(
        &mut self,
        (field, column): (&Field, &ArrayRef),
        request: &UnnestRequest<'_>,
    ) -> Result<bool, String> {
        if !self.unnests(field.name(), 1) {
            return Ok(false);
        }
        let Some(texts) = self.choose_column((field, column))? else {
            return Ok(false);
        };
        let table = child_table_name((self.naming, self.name_limit), request.table, field.name());
        for (row, text) in texts.iter().enumerate() {
            let (Some(text), Some(parent)) = (text, request.ids.get(row)) else {
                continue;
            };
            let place = ListPlace {
                path: field.name(),
                table: &table,
                depth: 1,
                parent,
                root: request
                    .roots
                    .and_then(|roots| roots.get(row))
                    .map(String::as_str),
                root_row: row,
            };
            self.add_list(text, &place)?;
        }
        Ok(true)
    }

    /// The JSON texts of the column `field` when it moves to a child table (see [`Self::chooses`]), choosing it from its values when that is still open.
    pub(crate) fn choose_column(
        &mut self,
        (field, column): (&Field, &ArrayRef),
    ) -> Result<Option<Vec<Option<String>>>, String> {
        let texts = json_texts(field, column)?;
        let seen = texts
            .iter()
            .flatten()
            .fold(Seen::default(), |seen, text| seen.and(text));
        Ok(self.chooses(field.name(), &seen)?.then_some(texts))
    }

    /// Whether the field at `path` moves to a child table given what its values in hand show: always for a listed path; under `"all"` when it is chosen as a list field, or this is its first deciding value and that is a list (a NULL or empty list decides nothing). A field chosen as a list field with a value that is not a list fails, naming it, as storing that value or dropping it would both lose it.
    fn chooses(&mut self, path: &str, seen: &Seen) -> Result<bool, String> {
        if matches!(self.unnest, Unnest::Paths(_)) {
            return Ok(true);
        }
        match (self.choices.get(path).copied(), seen) {
            (Some(true), Seen { other: true, .. }) => Err(format!(
                "{path}: unnest = \"all\" took {path} as a list field from its values so far, and a row holds a value that is not a list there; list the paths to unnest without {path}, so it stays a JSON column, or fix the row"
            )),
            (Some(chosen), _) => Ok(chosen),
            (None, Seen { other: true, .. }) => {
                self.choices.insert(path.to_owned(), false);
                Ok(false)
            }
            (None, Seen { list: true, .. }) => {
                self.choices.insert(path.to_owned(), true);
                Ok(true)
            }
            (None, _) => Ok(false),
        }
    }

    /// Whether the list field at `path`, `depth` levels down, becomes a child table: listed in `unnest` (or any list under `"all"`), no deeper than `max_depth`.
    fn unnests(&self, path: &str, depth: u32) -> bool {
        depth <= self.max_depth
            && match self.unnest {
                Unnest::None => false,
                Unnest::All => true,
                Unnest::Paths(paths) => paths.iter().any(|listed| listed == path),
            }
    }

    /// Add the items of `text`, the JSON value of a list field (a value that is not a list is a list of itself), as rows of the child table at `place`.
    fn add_list(&mut self, text: &str, place: &ListPlace<'_>) -> Result<(), String> {
        let items: Vec<Box<RawValue>> = match text.trim_start().as_bytes().first() {
            Some(b'n') => return Ok(()),
            Some(b'[') => {
                serde_json::from_str(text).map_err(|error| format!("{}: {error}", place.path))?
            }
            _ => vec![
                RawValue::from_string(text.to_owned())
                    .map_err(|error| format!("{}: {error}", place.path))?,
            ],
        };
        for (position, item) in items.iter().enumerate() {
            self.add_item(item.get(), place, position)?;
        }
        Ok(())
    }

    /// Add `item`, at `position` in its list, as one row of the child table at `place`, then its own list fields as rows a level down.
    fn add_item(
        &mut self,
        item: &str,
        place: &ListPlace<'_>,
        position: usize,
    ) -> Result<(), String> {
        let id = child_row_id(place.parent, place.table, position);
        let entries: Vec<(String, Box<RawValue>)> = match item.trim_start().as_bytes().first() {
            Some(b'{') => {
                let object = serde_json::from_str::<OrderedObject>(item)
                    .map_err(|error| format!("{}: {error}", place.path))?
                    .0;
                let entries = match self.flatten_objects {
                    true => flatten_entries(object, ("", place.path))?,
                    false => object,
                };
                self.loaded_keys(entries, place.path)?
            }
            Some(b'[') => vec![(NESTED_LIST_KEY.to_owned(), raw(item, place.path)?)],
            _ => vec![(VALUE_COLUMN.to_owned(), raw(item, place.path)?)],
        };
        let mut lists: Vec<(String, Box<RawValue>)> = Vec::new();
        let mut cells: Vec<(String, Box<RawValue>)> = Vec::new();
        for (key, value) in entries {
            let nested = format!("{}.{key}", place.path);
            let taken = self.unnests(&nested, place.depth + 1)
                && self.chooses(&nested, &Seen::default().and(value.get()))?;
            match taken {
                true => lists.push((key, value)),
                false => cells.push((key, value)),
            }
        }
        let level = self.level(place);
        for (key, value) in &cells {
            level.rows.push(
                key,
                raw_cell(value.get()).map_err(|error| format!("{}.{key}: {error}", place.path))?,
            );
        }
        level.rows.end_row();
        level.parents.push(place.parent.to_owned());
        level
            .positions
            .push(i64::try_from(position).unwrap_or(i64::MAX));
        level.ids.push(id.clone());
        level.roots.extend(place.root.map(str::to_owned));
        level.root_rows.push(place.root_row);
        for (key, value) in &lists {
            let path = format!("{}.{key}", place.path);
            let table = child_table_name((self.naming, self.name_limit), place.table, key);
            let below = ListPlace {
                path: &path,
                table: &table,
                depth: place.depth + 1,
                parent: &id,
                root: place.root,
                root_row: place.root_row,
            };
            self.add_list(value.get(), &below)?;
        }
        Ok(())
    }

    /// An item's `entries` with each key as it loads under the split's naming (dlt's normalises it); two keys of one item at `path` loading as one name is an error naming both.
    fn loaded_keys(
        &self,
        entries: Vec<(String, Box<RawValue>)>,
        path: &str,
    ) -> Result<Vec<(String, Box<RawValue>)>, String> {
        if self.naming == Naming::Sqlload {
            return Ok(entries);
        }
        let mut loaded: Vec<(String, Box<RawValue>)> = Vec::with_capacity(entries.len());
        let mut spelled: Vec<String> = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let name = loaded_name(self.naming, &key, self.name_limit);
            if let Some(other) = loaded.iter().position(|(taken, _)| *taken == name) {
                return Err(format!(
                    "{path}: keys {:?} and {key:?} both load as {name} under naming = \"dlt\"",
                    spelled[other]
                ));
            }
            loaded.push((name, value));
            spelled.push(key);
        }
        Ok(loaded)
    }

    /// The level gathering `place`'s rows, made when its first row arrives, after its parent's.
    fn level(&mut self, place: &ListPlace<'_>) -> &mut Level {
        let found = self
            .levels
            .iter()
            .position(|level| level.path == place.path);
        let index = found.unwrap_or_else(|| {
            self.levels.push(Level {
                path: place.path.to_owned(),
                table: place.table.to_owned(),
                rows: RowColumns::new(),
                parents: Vec::new(),
                positions: Vec::new(),
                ids: Vec::new(),
                roots: Vec::new(),
                root_rows: Vec::new(),
            });
            self.levels.len() - 1
        });
        &mut self.levels[index]
    }
}

/// `entries`, named below `prefix` when an object holds them, with each object's keys in its place as `<key>__<inner>`, recursively, as dlt flattens an item's objects (an empty object, or a NULL inside an object, leaves nothing); an error names the item's `path`.
fn flatten_entries(
    entries: Vec<(String, Box<RawValue>)>,
    (prefix, path): (&str, &str),
) -> Result<Vec<(String, Box<RawValue>)>, String> {
    let mut flat: Vec<(String, Box<RawValue>)> = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        let name = match prefix.is_empty() {
            true => key,
            false => format!("{prefix}{FLATTEN_SEPARATOR}{key}"),
        };
        match value.get().trim_start().as_bytes().first() {
            Some(b'{') => {
                let object = serde_json::from_str::<OrderedObject>(value.get())
                    .map_err(|error| format!("{path}.{name}: {error}"))?;
                flat.extend(flatten_entries(object.0, (&name, path))?);
            }
            Some(b'n') if !prefix.is_empty() => {}
            _ => flat.push((name, value)),
        }
    }
    Ok(flat)
}

/// `text` as a raw JSON value, an error naming `path` when it is not one.
fn raw(text: &str, path: &str) -> Result<Box<RawValue>, String> {
    RawValue::from_string(text.to_owned()).map_err(|error| format!("{path}: {error}"))
}

/// `level`'s rows as a batch: the items' data columns, then, in dlt's order, the root's id when it has them, the parent's id, the position and the row's own id, named under `naming`; a data column taking one of those names is an error.
fn level_batch(level: Level, naming: Naming) -> Result<ChildBatch, String> {
    let data = columns_batch(level.rows)?;
    let schema = data.schema();
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    let mut columns: Vec<ArrayRef> = data.columns().to_vec();
    let mut linkage: Vec<(MetadataRole, ArrayRef)> = Vec::with_capacity(4);
    if !level.roots.is_empty() {
        linkage.push((
            MetadataRole::RootId,
            Arc::new(StringArray::from(level.roots)),
        ));
    }
    linkage.push((
        MetadataRole::ParentId,
        Arc::new(StringArray::from(level.parents)),
    ));
    linkage.push((
        MetadataRole::ListIndex,
        Arc::new(Int64Array::from(level.positions)),
    ));
    linkage.push((MetadataRole::RowId, Arc::new(StringArray::from(level.ids))));
    for (role, column) in linkage {
        let name = metadata_name(naming, role);
        if schema.fields().iter().any(|field| field.name() == name) {
            return Err(format!(
                "{}: its items have a field {name}, the name of the column linking {} to its parent; rename it before loading",
                level.path, level.table
            ));
        }
        fields.push(Arc::new(Field::new(name, column.data_type().clone(), true)));
        columns.push(column);
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| error.to_string())?;
    Ok(ChildBatch {
        path: level.path,
        table: level.table,
        batch,
        root_rows: level.root_rows,
    })
}

/// Column `field` of a batch as each row's JSON text (`None` for NULL): JSON text as it is, a list as its JSON, text as a JSON string, and numbers and booleans as written.
fn json_texts(field: &Field, array: &ArrayRef) -> Result<Vec<Option<String>>, String> {
    let marked = |field: &Field| {
        field
            .metadata()
            .get(ARROW_TYPE_METADATA_KEY)
            .is_some_and(|kind| *kind == ColumnType::Json.to_string())
    };
    let (normal_field, normal) = normalise_column(field.name(), array)?;
    let is_json = marked(field) || marked(&normal_field);
    match (is_json, normal.data_type()) {
        (_, DataType::Utf8) => {
            let mut texts: Vec<Option<String>> = Vec::with_capacity(normal.len());
            for text in normal.as_string::<i32>() {
                texts.push(text.map(|text| quoted(text, is_json)));
            }
            Ok(texts)
        }
        _ => {
            let formatter = ArrayFormatter::try_new(normal.as_ref(), &FormatOptions::default())
                .map_err(|error| error.to_string())?;
            Ok((0..normal.len())
                .map(|row| (!normal.is_null(row)).then(|| formatter.value(row).to_string()))
                .collect())
        }
    }
}

/// `text` as JSON: itself when it already is JSON, otherwise a JSON string.
fn quoted(text: &str, is_json: bool) -> String {
    match is_json {
        true => text.to_owned(),
        false => serde_json::Value::String(text.to_owned()).to_string(),
    }
}

impl Seen {
    /// What is seen once the JSON value `text` is too.
    fn and(self, text: &str) -> Self {
        let text = text.trim_start();
        match text.as_bytes().first() {
            None | Some(b'n') => self,
            Some(b'[') => Self {
                list: self.list || !text[1..].trim_start().starts_with(']'),
                ..self
            },
            Some(_) => Self {
                other: true,
                ..self
            },
        }
    }
}

/// The parent's rows from the `kept` columns, with `schema`'s metadata.
pub(crate) fn parent_batch(
    schema: &Schema,
    kept: Vec<(Arc<Field>, ArrayRef)>,
) -> Result<RecordBatch, String> {
    let (fields, columns): (Vec<Arc<Field>>, Vec<ArrayRef>) = kept.into_iter().unzip();
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
    )
    .map_err(|error| error.to_string())
}

/// Every level `split` gathered as its child table's batch, parents before children.
pub(crate) fn child_batches(split: Split<'_>) -> Result<Vec<ChildBatch>, String> {
    let naming = split.naming;
    split
        .levels
        .into_iter()
        .map(|level| level_batch(level, naming))
        .collect()
}
