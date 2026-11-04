use pyo3::exceptions::PyTypeError;
use pyo3::types::{
    PyAnyMethods, PyBool, PyBoolMethods, PyDict, PyDictMethods, PyFloat, PyFloatMethods, PyInt,
    PyList, PyListMethods, PyString, PyStringMethods, PyTuple, PyType, PyTypeMethods,
};
use pyo3::{Bound, PyAny, PyErr, PyResult, Python};
use sqlload::row_batches::models::{Cell, JsonWriter, RowColumns};

use crate::crossing::constants::{NUMPY_ARRAY, NUMPY_MODULE};

/// Reads Python values by orjson's rules (`OPT_SERIALIZE_NUMPY`, `default=str`), so walked and serialised rows type alike.
struct Walker<'py> {
    writer: JsonWriter,
    datetime: Bound<'py, PyType>,
    date: Bound<'py, PyType>,
    time: Bound<'py, PyType>,
    enumeration: Bound<'py, PyType>,
}

/// One Python value as orjson sees it.
enum Plain<'py> {
    Null,
    Bool(bool),
    Int(Bound<'py, PyInt>),
    Float(f64),
    Text(String),
    /// A dict's items, or a dataclass's public fields.
    Object(Vec<(Bound<'py, PyAny>, Bound<'py, PyAny>)>),
    /// A list, tuple or numeric numpy array.
    Array(Bound<'py, PyAny>),
}

/// Read `rows`, a list of dicts, column by column, holding the GIL.
pub(crate) fn walk_rows(rows: &Bound<'_, PyList>) -> PyResult<RowColumns> {
    let mut walker = Walker::new(rows.py())?;
    let mut columns = RowColumns::new();
    for (index, row) in rows.iter().enumerate() {
        let row = row.cast::<PyDict>().map_err(|_| {
            PyTypeError::new_err(format!(
                "row {index} of a yielded batch is a {}, not a dict",
                type_name(&row)
            ))
        })?;
        for (key, value) in row.iter() {
            let key = key.cast::<PyString>().map_err(|_| key_error(&key))?;
            columns.push(key.to_str()?, walker.cell(&value)?);
        }
        columns.end_row();
    }
    Ok(columns)
}

impl<'py> Walker<'py> {
    fn new(py: Python<'py>) -> PyResult<Self> {
        let datetime = py.import("datetime")?;
        Ok(Self {
            writer: JsonWriter::new(),
            datetime: datetime.getattr("datetime")?.cast_into()?,
            date: datetime.getattr("date")?.cast_into()?,
            time: datetime.getattr("time")?.cast_into()?,
            enumeration: py.import("enum")?.getattr("Enum")?.cast_into()?,
        })
    }

    /// The cell for one Python value: as orjson would write it, then read back (NaN is NULL; dates are ISO text; unknown types are `str()`).
    fn cell(&mut self, value: &Bound<'py, PyAny>) -> PyResult<Cell> {
        Ok(match self.plain(value)? {
            Plain::Null => Cell::Null,
            Plain::Bool(flag) => Cell::Bool(flag),
            Plain::Int(number) => match number.extract::<i64>() {
                Ok(number) => Cell::Int(number),
                Err(_) => Cell::Number(number.str()?.to_str()?.to_owned()),
            },
            Plain::Float(number) => match number.is_finite() {
                true => Cell::Float(number),
                false => Cell::Null,
            },
            Plain::Text(text) => Cell::Text(text),
            nested @ (Plain::Object(_) | Plain::Array(_)) => {
                self.write(nested)?;
                Cell::Json(self.writer.take())
            }
        })
    }

    /// Append `value` to the JSON text, keys in dict order, as orjson writes it.
    fn value(&mut self, value: &Bound<'py, PyAny>) -> PyResult<()> {
        let plain = self.plain(value)?;
        self.write(plain)
    }

    fn write(&mut self, plain: Plain<'py>) -> PyResult<()> {
        match plain {
            Plain::Null => self.writer.raw("null"),
            Plain::Bool(flag) => self.writer.raw(if flag { "true" } else { "false" }),
            Plain::Int(number) => match number.extract::<i64>() {
                Ok(number) => self.writer.raw(&number.to_string()),
                Err(_) => self.writer.raw(number.str()?.to_str()?),
            },
            Plain::Float(number) => self.writer.float(number),
            Plain::Text(text) => self.writer.string(&text),
            Plain::Object(items) => {
                self.writer.raw("{");
                for (position, (key, item)) in items.iter().enumerate() {
                    let key = key.cast::<PyString>().map_err(|_| key_error(key))?;
                    if position > 0 {
                        self.writer.raw(",");
                    }
                    self.writer.string(key.to_str()?);
                    self.writer.raw(":");
                    self.value(item)?;
                }
                self.writer.raw("}");
            }
            Plain::Array(items) => {
                self.writer.raw("[");
                for (position, item) in items.try_iter()?.enumerate() {
                    if position > 0 {
                        self.writer.raw(",");
                    }
                    self.value(&item?)?;
                }
                self.writer.raw("]");
            }
        }
        Ok(())
    }

    /// What orjson makes of `value`: str, int, dict and list subclasses as their base type; exact floats, tuples and dates; enums as their values; dataclasses as objects; numpy numbers; anything else as `str()`.
    fn plain(&self, value: &Bound<'py, PyAny>) -> PyResult<Plain<'py>> {
        if value.is_none() {
            return Ok(Plain::Null);
        }
        if let Ok(text) = value.cast::<PyString>() {
            return Ok(Plain::Text(text.to_str()?.to_owned()));
        }
        if let Ok(flag) = value.cast_exact::<PyBool>() {
            return Ok(Plain::Bool(flag.is_true()));
        }
        if let Ok(number) = value.cast::<PyInt>() {
            return Ok(Plain::Int(number.clone()));
        }
        if let Ok(number) = value.cast_exact::<PyFloat>() {
            return Ok(Plain::Float(number.value()));
        }
        if let Ok(object) = value.cast::<PyDict>() {
            return Ok(Plain::Object(object.iter().collect()));
        }
        if value.is_instance_of::<PyList>() || value.is_exact_instance_of::<PyTuple>() {
            return Ok(Plain::Array(value.clone()));
        }
        let kind = value.get_type();
        if kind.is(&self.datetime) || kind.is(&self.date) || kind.is(&self.time) {
            return Ok(Plain::Text(
                value.call_method0("isoformat")?.str()?.to_str()?.to_owned(),
            ));
        }
        if value.is_instance(&self.enumeration)? {
            return self.plain(&value.getattr("value")?);
        }
        if kind.hasattr("__dataclass_fields__")? {
            return dataclass_fields(value).map(Plain::Object);
        }
        if kind.module()?.to_str()? == NUMPY_MODULE {
            return numpy_plain(value);
        }
        Ok(Plain::Text(value.str()?.to_str()?.to_owned()))
    }
}

/// A dataclass's attributes as orjson writes them: `__dict__`, or the fields of a slotted one, without names starting `_`.
fn dataclass_fields<'py>(
    value: &Bound<'py, PyAny>,
) -> PyResult<Vec<(Bound<'py, PyAny>, Bound<'py, PyAny>)>> {
    let attributes: Vec<(Bound<'py, PyAny>, Bound<'py, PyAny>)> = match value.getattr("__dict__") {
        Ok(attributes) => attributes.cast_into::<PyDict>()?.iter().collect(),
        Err(_) => value
            .py()
            .import("dataclasses")?
            .call_method1("fields", (value,))?
            .try_iter()?
            .map(|field| {
                let name = field?.getattr("name")?.cast_into::<PyString>()?;
                let item = value.getattr(&name)?;
                Ok((name.into_any(), item))
            })
            .collect::<PyResult<_>>()?,
    };
    let mut fields = Vec::with_capacity(attributes.len());
    for (name, item) in attributes {
        let public = match name.cast::<PyString>() {
            Ok(text) => !text.to_str()?.starts_with('_'),
            Err(_) => true,
        };
        if public {
            fields.push((name, item));
        }
    }
    Ok(fields)
}

/// A numpy scalar or array as orjson's `OPT_SERIALIZE_NUMPY` writes it: numbers and booleans as JSON ones, datetimes as ISO text, numeric arrays as lists.
fn numpy_plain<'py>(value: &Bound<'py, PyAny>) -> PyResult<Plain<'py>> {
    let dtype = value.getattr("dtype")?;
    let kind = dtype.getattr("kind")?.str()?.to_str()?.to_owned();
    if value.get_type().name()?.to_str()? == NUMPY_ARRAY {
        let numeric = matches!(kind.as_str(), "b" | "i" | "u" | "f");
        return Ok(
            match numeric && value.getattr("ndim")?.extract::<usize>()? > 0 {
                true => Plain::Array(value.clone()),
                false => Plain::Text(value.str()?.to_str()?.to_owned()),
            },
        );
    }
    let size = dtype.getattr("itemsize")?.extract::<usize>()?;
    Ok(match (kind.as_str(), size) {
        ("b", _) => Plain::Bool(value.call_method0("item")?.is_truthy()?),
        ("i" | "u", _) => Plain::Int(value.call_method0("item")?.cast_into::<PyInt>()?),
        ("f", 8) => Plain::Float(value.call_method0("item")?.extract::<f64>()?),
        ("f", 2 | 4) => {
            let single = value.call_method1("astype", ("float32",))?.str()?;
            Plain::Float(single.to_str()?.parse::<f64>().unwrap_or(f64::NAN))
        }
        ("M", _) => {
            let micros = value
                .call_method1("astype", ("datetime64[us]",))?
                .call_method0("item")?;
            match micros.is_none() {
                true => Plain::Null,
                false => Plain::Text(
                    micros
                        .call_method0("isoformat")?
                        .str()?
                        .to_str()?
                        .to_owned(),
                ),
            }
        }
        _ => Plain::Text(value.str()?.to_str()?.to_owned()),
    })
}

fn key_error(key: &Bound<'_, PyAny>) -> PyErr {
    PyTypeError::new_err(format!("dict keys must be str, not {}", type_name(key)))
}

fn type_name(value: &Bound<'_, PyAny>) -> String {
    value
        .get_type()
        .name()
        .map_or_else(|_| "object".to_owned(), |name| name.to_string())
}
