use std::collections::HashMap;

use crate::row_batches::types::ListChoices;

use arrow_array::RecordBatch;

use crate::column_parse::models::{BadValue, ColumnParse};
use crate::load_naming::main::loaded_name::loaded_name;
use crate::load_naming::models::{Compat, Naming};
use crate::row_batches::constants::FLATTEN_SEPARATOR;

/// One value of a yielded row, before its column's type is known.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Null,
    Bool(bool),
    /// A 64-bit integer.
    Int(i64),
    /// A finite 64-bit float.
    Float(f64),
    /// A number that is neither a 64-bit integer nor a finite float, as its text; it makes a `text` column.
    Number(String),
    Text(String),
    /// A nested object or list, as compact JSON text.
    Json(String),
    /// A Python `date`, as days since 1970-01-01.
    Date(i32),
    /// A naive Python `datetime`, as microseconds since 1970-01-01 00:00 on the wall clock.
    TimestampNtz(i64),
    /// A finite Python `Decimal`, as fixed-point text (`-12.50`).
    Decimal(String),
}

/// Yielded rows gathered column by column: column names in first-seen order and one cell per row in each.
#[derive(Debug, Default)]
pub struct RowColumns {
    names: Vec<String>,
    positions: HashMap<String, usize>,
    cells: Vec<Vec<Cell>>,
    rows: usize,
    /// The column the next key is likely to name: rows usually repeat the previous row's key order.
    guess: usize,
    /// The columns that flattening a listed object made.
    flattened: Vec<String>,
}

impl RowColumns {
    /// An empty set of rows.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set column `name` of the current row; a repeated key keeps its last value.
    pub fn push(&mut self, name: &str, cell: Cell) {
        let position = match self.names.get(self.guess) {
            Some(guessed) if guessed == name => self.guess,
            _ => self.position(name),
        };
        self.guess = position + 1;
        let column = &mut self.cells[position];
        column.resize(self.rows, Cell::Null);
        column.push(cell);
    }

    /// Finish the current row; columns it did not set are NULL.
    pub fn end_row(&mut self) {
        self.rows += 1;
        self.guess = 0;
    }

    /// About how many bytes the cells hold.
    pub fn approximate_bytes(&self) -> usize {
        self.cells
            .iter()
            .flatten()
            .map(|cell| {
                std::mem::size_of::<Cell>()
                    + match cell {
                        Cell::Number(text)
                        | Cell::Text(text)
                        | Cell::Json(text)
                        | Cell::Decimal(text) => text.len(),
                        Cell::Null
                        | Cell::Bool(_)
                        | Cell::Int(_)
                        | Cell::Float(_)
                        | Cell::Date(_)
                        | Cell::TimestampNtz(_) => 0,
                    }
            })
            .sum()
    }

    /// Record the columns that flattening a listed object made.
    pub fn set_flattened(&mut self, names: Vec<String>) {
        self.flattened = names;
    }

    /// The columns that flattening a listed object made.
    pub fn flattened(&self) -> &[String] {
        &self.flattened
    }

    /// The finished rows.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// The row count and each column's name and cells, one per row.
    pub fn into_columns(self) -> (usize, Vec<(String, Vec<Cell>)>) {
        let rows = self.rows;
        let columns: Vec<(String, Vec<Cell>)> = self
            .names
            .into_iter()
            .zip(self.cells)
            .map(|(name, mut cells)| {
                cells.resize(rows, Cell::Null);
                (name, cells)
            })
            .collect();
        (rows, columns)
    }

    fn position(&mut self, name: &str) -> usize {
        match self.positions.get(name) {
            Some(position) => *position,
            None => {
                self.names.push(name.to_owned());
                self.positions.insert(name.to_owned(), self.names.len() - 1);
                self.cells.push(Vec::new());
                self.names.len() - 1
            }
        }
    }
}

/// How JSON bytes hold their rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonLayout {
    /// One JSON document: an array of objects, or objects one after another.
    Document,
    /// One object per line (NDJSON).
    Lines,
}

/// What to read from JSON bytes, and how many rows go in each batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonRead {
    pub layout: JsonLayout,
    /// A dotted path to the array of rows inside a document, such as `data.items`.
    pub path: Option<String>,
    pub batch_rows: usize,
    /// The columns each row object loads as, when only some of its values load, renamed.
    pub projection: Option<Projection>,
}

/// Which values of each row object load, under which column names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projection {
    pub fields: Vec<ProjectedField>,
    /// The zone a parsed time without an offset is in when its field has no `tz`: the table's `timezone`, which the crossing sets; `None` is UTC.
    pub zone: Option<chrono_tz::Tz>,
    /// A column holding the whole row object as JSON.
    pub keep_record: Option<String>,
    /// The fields' paths as a tree, so each row walks every object on them once.
    pub tree: PathNode,
}

/// One key of the projection's paths: the keys below it, and the fields whose path ends or passes here, by position in `fields`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PathNode {
    pub children: HashMap<String, PathNode>,
    pub ends: Vec<usize>,
    pub under: Vec<usize>,
}

/// What a table asks of its projected rows: the cursor column must load, and masked columns must not hide in `keep_record`.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProjectionRules<'a> {
    pub cursor_column: Option<&'a str>,
    /// The objects expanded into `<column>__<key>` columns after projection, when the table flattens any.
    pub flatten: Option<&'a Flatten>,
    pub masked: &'a [String],
    /// How the table's columns load, and the destination's name limit: a field loads the cursor when its loaded name is the cursor's.
    pub naming: (Naming, Option<usize>),
}

/// One column of `fields=` as a table function names it (`sl.field(...)`): its path, such as `$.address.city`, whether every row needs a value there, and how that value converts.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct FieldSpec {
    pub name: String,
    pub path: String,
    #[serde(default)]
    pub required: bool,
    /// `sl.field(parse=...)` as written, as a table's `parse` entry is (see [`column_parse`](crate::column_parse::main::column_parse::column_parse)).
    #[serde(default)]
    pub parse: Option<serde_json::Value>,
    #[serde(default)]
    pub on_error: BadValue,
    /// What `path` starts from: the row, or, in an `sl.http` job, an object around it or the request's key.
    #[serde(default)]
    pub from: FieldFrom,
}

/// Where a field's path starts: the row (`sl.field`), the `levels`th object around it that `select` passes through, nearest first (`sl.parent`, the response document outermost), or the key field `name` of the fan-out request that read it (`sl.key`).
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FieldFrom {
    #[default]
    Row,
    Parent {
        levels: usize,
    },
    Key {
        name: String,
    },
}

/// One column a projection loads: the value at `path` (keys from the row object down), or NULL when it is missing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedField {
    pub name: String,
    pub path: Vec<String>,
    /// The path as written, such as `$.address.city`, for errors.
    pub text: String,
    /// A row without a value at `path` fails the read rather than loading NULL.
    pub required: bool,
    /// How its value converts, with what a value that doesn't convert does.
    pub parse: Option<ColumnParse>,
}

/// Builds compact JSON text for nested values, escaping strings as serde_json does.
#[derive(Debug, Default)]
pub struct JsonWriter {
    text: String,
}

impl JsonWriter {
    /// An empty writer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append JSON punctuation, a literal or a number as it is.
    pub fn raw(&mut self, text: &str) {
        self.text.push_str(text);
    }

    /// Append `text` as a JSON string.
    pub fn string(&mut self, text: &str) {
        self.text.push('"');
        let mut start = 0;
        for (position, byte) in text.bytes().enumerate() {
            let escape = match byte {
                b'"' => "\\\"",
                b'\\' => "\\\\",
                b'\n' => "\\n",
                b'\r' => "\\r",
                b'\t' => "\\t",
                0x08 => "\\b",
                0x0c => "\\f",
                0x00..=0x1f => "",
                _ => continue,
            };
            self.text.push_str(&text[start..position]);
            match escape.is_empty() {
                true => self.text.push_str(&format!("\\u{byte:04x}")),
                false => self.text.push_str(escape),
            }
            start = position + 1;
        }
        self.text.push_str(&text[start..]);
        self.text.push('"');
    }

    /// Append a float as the shortest number text that reads back the same; NaN and infinities are `null`.
    pub fn float(&mut self, value: f64) {
        match serde_json::Number::from_f64(value) {
            Some(number) => self.text.push_str(&number.to_string()),
            None => self.text.push_str("null"),
        }
    }

    /// The text written so far, leaving the writer empty.
    pub fn take(&mut self) -> String {
        std::mem::take(&mut self.text)
    }
}

/// Which list fields of a table's rows become child tables.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Unnest {
    /// None: lists stay JSON columns.
    #[default]
    None,
    /// Every list field, at every level down to `max_depth`: a field whose values are lists, and never one with another value (an object or a scalar stays a column).
    All,
    /// The list fields at these dotted paths.
    Paths(Vec<String>),
}

impl Unnest {
    /// Whether the loaded table's own column `name` is a list field its child tables take, so the table itself has no such column: a listed one, or under `"all"` one `choices` (the plan's) chose.
    pub fn takes(&self, name: &str, choices: &ListChoices) -> bool {
        match self {
            Self::Paths(paths) => paths.iter().any(|path| path == name),
            Self::All => choices.get(name) == Some(&true),
            Self::None => false,
        }
    }
}

/// Which objects of a table's rows flatten into `<object>__<key>` columns.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum FlattenObjects {
    /// None: objects stay JSON columns.
    #[default]
    None,
    /// Every object, at every level down to `max_depth`.
    All,
    /// The objects at these dotted paths (`customer`, `customer.address`, `items.dimensions` inside an unnested list's items).
    Paths(Vec<String>),
}

/// How a table flattens the objects of its rows: which, the ones kept whole, and how deep.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Flatten {
    pub objects: FlattenObjects,
    /// Objects kept whole as one column, by the name each would load as (`line_totals`, `customer__address`): those pinned `json`, and those masked, so the mask applies to the whole object.
    pub keep: Vec<String>,
    /// Columns pinned as another type than `json`, by name, with the type: flattening one of them fails rather than leave the pin unused.
    pub pinned: Vec<(String, String)>,
    /// How many levels down objects flatten at most (a field of the row is level 1); deeper ones stay JSON.
    pub max_depth: u32,
    /// How the table names its columns: under snake_case, a path or name matches a key as written or as both load (`LineTotals` is `line_totals`); under `source`, only as written.
    pub naming: Naming,
}

impl Flatten {
    /// Whether no object flattens.
    pub fn is_none(&self) -> bool {
        self.objects == FlattenObjects::None
    }

    /// Whether the object at `path` (keys from the row down, as the data spells them) flattens: listed, or any under `"all"`, no deeper than `max_depth`, and not kept whole; an error when a pin of another type than `json` names it.
    pub fn flattens(&self, path: &[&str]) -> Result<bool, String> {
        let chosen = match &self.objects {
            FlattenObjects::None => false,
            FlattenObjects::All => true,
            FlattenObjects::Paths(_) => self.lists(path),
        };
        if !chosen || path.len() > self.max_depth as usize || self.keeps(path) {
            return Ok(false);
        }
        let name = path.join(FLATTEN_SEPARATOR);
        match self
            .pinned
            .iter()
            .find(|(pinned, _)| self.same_parts(pinned.split(FLATTEN_SEPARATOR), path))
        {
            Some((pinned, column_type)) => Err(format!(
                "columns {pinned} = {column_type:?}: flatten replaces {name} with its keys' columns; pin it \"json\" to keep it whole"
            )),
            None => Ok(true),
        }
    }

    /// Whether `path` is one of the listed paths (never under `"all"`), whose values must be objects.
    pub fn lists(&self, path: &[&str]) -> bool {
        match &self.objects {
            FlattenObjects::Paths(paths) => paths
                .iter()
                .any(|listed| self.same_parts(listed.split('.'), path)),
            FlattenObjects::None | FlattenObjects::All => false,
        }
    }

    /// Whether column `name` may be one flattening made: under `"all"` any holding `__`, else one below a listed path.
    pub fn may_make(&self, name: &str) -> bool {
        match &self.objects {
            FlattenObjects::None => false,
            FlattenObjects::All => name.contains(FLATTEN_SEPARATOR),
            FlattenObjects::Paths(paths) => paths.iter().any(|listed| {
                let prefix = format!(
                    "{}{FLATTEN_SEPARATOR}",
                    listed.replace('.', FLATTEN_SEPARATOR)
                );
                name.get(..prefix.len())
                    .is_some_and(|start| start.eq_ignore_ascii_case(&prefix))
            }),
        }
    }

    /// Whether the object at `path` is kept whole: `keep` names it, its parts joined by `__`.
    pub fn keeps(&self, path: &[&str]) -> bool {
        self.keep
            .iter()
            .any(|name| self.same_parts(name.split(FLATTEN_SEPARATOR), path))
    }

    /// Whether `parts` and `path` name the same keys, each as written or, under snake_case, as both load (`LineTotals` is `line_totals`).
    fn same_parts<'a>(&self, parts: impl Iterator<Item = &'a str>, path: &[&str]) -> bool {
        let parts: Vec<&str> = parts.collect();
        parts.len() == path.len()
            && parts.iter().zip(path).all(|(part, key)| {
                part == key
                    || (self.naming == Naming::SnakeCase
                        && loaded_name(self.naming, part, None)
                            == loaded_name(self.naming, key, None))
            })
    }
}

/// How [`unnest_batch`](crate::row_batches::main::unnest_batch::unnest_batch) splits a batch's list fields into child tables.
#[derive(Debug, Clone, Copy)]
pub struct UnnestRequest<'a> {
    /// The table the batch loads, whose name its child tables' names start with.
    pub table: &'a str,
    pub unnest: &'a Unnest,
    /// How many levels of child tables to make at most.
    pub max_depth: u32,
    pub naming: Naming,
    /// Whose names the child rows' id, parent and position columns take.
    pub compat: Compat,
    /// The longest name the destination takes.
    pub name_limit: Option<usize>,
    /// Which objects inside an item flatten into `<key>__<inner>` fields, by their path from the table's row (`items.dimensions`).
    pub flatten: &'a Flatten,
    /// Under `unnest = "all"`, the child tables the destination already has, as it stores their names (compared in any case): a field whose child table is among them is a list field, as an earlier load chose it.
    pub live: &'a [String],
    /// Each row's id, which its child rows name as their parent's.
    pub ids: &'a [String],
    /// Each row's root row's id, which child rows carry under a merge; `None` otherwise.
    pub roots: Option<&'a [String]>,
}

/// A batch split by `unnest`: the parent's rows without their unnested list fields, and every child table's rows, parents before their children.
#[derive(Debug)]
pub struct Unnested {
    pub parent: RecordBatch,
    pub children: Vec<ChildBatch>,
}

/// The rows of one child table.
#[derive(Debug)]
pub struct ChildBatch {
    /// The dotted path of the list field the rows come from, such as `items.parts`.
    pub path: String,
    /// The child table's name, such as `orders__items__parts`.
    pub table: String,
    /// The items' data columns, then the parent's id, the position in the list, the row's own id and, under a merge, the root row's id.
    pub batch: RecordBatch,
    /// Each row's root: its row in the batch split.
    pub root_rows: Vec<usize>,
}
