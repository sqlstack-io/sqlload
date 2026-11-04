use pyo3::exceptions::PyTypeError;
use pyo3::types::{
    PyAnyMethods, PyBool, PyBoolMethods, PyDict, PyDictMethods, PyFloat, PyFloatMethods, PyInt,
    PyList, PyListMethods, PyString, PyStringMethods, PyTuple, PyTypeMethods,
};
use pyo3::{Bound, PyAny, PyErr, PyResult};
use sqlload::row_batches::models::{Cell, JsonWriter, RowColumns};

/// Read `rows`, a list of dicts, column by column, holding the GIL.
pub(crate) fn walk_rows(rows: &Bound<'_, PyList>) -> PyResult<RowColumns> {
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
            columns.push(key.to_str()?, cell(&value)?);
        }
        columns.end_row();
    }
    Ok(columns)
}

/// The cell for one Python value: as orjson would write it, then read back (NaN is NULL; dates are ISO text; unknown types are `str()`).
fn cell(value: &Bound<'_, PyAny>) -> PyResult<Cell> {
    if value.is_none() {
        return Ok(Cell::Null);
    }
    if let Ok(text) = value.cast_exact::<PyString>() {
        return Ok(Cell::Text(text.to_str()?.to_owned()));
    }
    if let Ok(flag) = value.cast::<PyBool>() {
        return Ok(Cell::Bool(flag.is_true()));
    }
    if let Ok(number) = value.cast::<PyInt>() {
        return Ok(match number.extract::<i64>() {
            Ok(number) => Cell::Int(number),
            Err(_) => Cell::Number(number.str()?.to_str()?.to_owned()),
        });
    }
    if let Ok(number) = value.cast::<PyFloat>() {
        let number = number.value();
        return Ok(match number.is_finite() {
            true => Cell::Float(number),
            false => Cell::Null,
        });
    }
    if value.is_instance_of::<PyDict>()
        || value.is_instance_of::<PyList>()
        || value.is_instance_of::<PyTuple>()
    {
        return Ok(Cell::Json(PythonJson::default().text(value)?));
    }
    Ok(Cell::Text(text_of(value)?))
}

/// Writes nested Python values as compact JSON, keys in dict order, as orjson writes them.
#[derive(Debug, Default)]
struct PythonJson {
    writer: JsonWriter,
}

impl PythonJson {
    /// The JSON text of `value`.
    fn text(&mut self, value: &Bound<'_, PyAny>) -> PyResult<String> {
        self.value(value)?;
        Ok(self.writer.take())
    }

    fn value(&mut self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        if value.is_none() {
            self.writer.raw("null");
        } else if let Ok(text) = value.cast::<PyString>() {
            self.writer.string(text.to_str()?);
        } else if let Ok(flag) = value.cast::<PyBool>() {
            self.writer
                .raw(if flag.is_true() { "true" } else { "false" });
        } else if let Ok(number) = value.cast::<PyInt>() {
            match number.extract::<i64>() {
                Ok(number) => self.writer.raw(&number.to_string()),
                Err(_) => self.writer.raw(number.str()?.to_str()?),
            }
        } else if let Ok(number) = value.cast::<PyFloat>() {
            self.writer.float(number.value());
        } else if let Ok(object) = value.cast::<PyDict>() {
            self.writer.raw("{");
            for (position, (key, item)) in object.iter().enumerate() {
                let key = key.cast::<PyString>().map_err(|_| key_error(&key))?;
                if position > 0 {
                    self.writer.raw(",");
                }
                self.writer.string(key.to_str()?);
                self.writer.raw(":");
                self.value(&item)?;
            }
            self.writer.raw("}");
        } else if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
            self.writer.raw("[");
            for (position, item) in value.try_iter()?.enumerate() {
                if position > 0 {
                    self.writer.raw(",");
                }
                self.value(&item?)?;
            }
            self.writer.raw("]");
        } else {
            self.writer.string(&text_of(value)?);
        }
        Ok(())
    }
}

/// ISO text for dates and times, `str()` for anything else.
fn text_of(value: &Bound<'_, PyAny>) -> PyResult<String> {
    let text = match value.hasattr("isoformat")? {
        true => value.call_method0("isoformat")?.str()?,
        false => value.str()?,
    };
    Ok(text.to_str()?.to_owned())
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
