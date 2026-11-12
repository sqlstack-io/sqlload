use std::collections::HashMap;

use arrow_array::RecordBatch;

use crate::load_naming::models::Naming;

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
    /// Columns expanded into `<column>__<key>` after projection.
    pub flatten: &'a [String],
    pub masked: &'a [String],
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
    /// Every list field, at every level down to `max_depth`.
    All,
    /// The list fields at these dotted paths.
    Paths(Vec<String>),
}

impl Unnest {
    /// Whether the loaded table's own column `name` is a list field its child tables take, so the table itself has no such column.
    pub fn takes(&self, name: &str) -> bool {
        match self {
            Self::Paths(paths) => paths.iter().any(|path| path == name),
            Self::None | Self::All => false,
        }
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
}
