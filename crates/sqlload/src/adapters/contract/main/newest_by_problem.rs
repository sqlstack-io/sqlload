//! Say whether a `newest_by` column can tell which row is newer.

use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::DesiredColumn;

/// Why `newest_by` column `name` can't say which row is newer on `adapter`'s warehouse when the table writes `columns`: it isn't among them, it is JSON or an array, or it is a native type the warehouse documents it can't order.
pub fn newest_by_problem(
    adapter: &dyn SqlAdapter,
    columns: &[DesiredColumn],
    name: &str,
) -> Option<String> {
    let written = columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(name));
    match written.map(|column| &column.column_type) {
        None => Some(format!(
            "newest_by column {name} is not written to the table (not a column of the source, excluded, not in the enforced contract, or ignored by on_schema_change)"
        )),
        Some(column_type @ (ColumnType::Json | ColumnType::Array(_))) => Some(format!(
            "newest_by column {name} is {column_type}, which has no order to say which row is newer; name a column of another type"
        )),
        Some(column_type @ ColumnType::Native(native)) => {
            let (family, parameterised) = native_family(native);
            (!adapter.orders_native(&family, parameterised)).then(|| {
                format!(
                    "newest_by column {name} is {column_type}, which {} can't order to say which row is newer; pin a portable type for it in columns, or name another column",
                    adapter.warehouse()
                )
            })
        }
        Some(_) => None,
    }
}

/// A native type's name, upper case and without ClickHouse's `Nullable(...)` and `LowCardinality(...)` wrappers, cut at its parameters, and whether it has any (`(` or `<` follows the name).
fn native_family(native: &str) -> (String, bool) {
    let mut upper = native.trim().to_ascii_uppercase();
    while let Some(inner) = ["NULLABLE(", "LOWCARDINALITY("]
        .iter()
        .find_map(|wrapper| upper.strip_prefix(wrapper))
    {
        upper = inner.strip_suffix(')').unwrap_or(inner).trim().to_owned();
    }
    let end = upper
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(upper.len());
    let parameterised = upper[end..].trim_start().starts_with(['(', '<']);
    (upper[..end].to_owned(), parameterised)
}
