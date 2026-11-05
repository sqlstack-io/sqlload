//! The statements comparing two tables' rows.

use crate::parity::constants::TEXT_COMPARED_TYPES;
use crate::parity::models::{ComparedColumn, ComparisonSql};

/// Typed `MINUS` both ways on `columns` of two tables (qualified and quoted), so values compare at their full precision; only VARIANT-like columns, which `MINUS` cannot compare, go through `TO_VARCHAR`. `MINUS` keeps distinct rows, so the row counts are compared too.
pub fn comparison_sql(
    (left, right): (&str, &str),
    columns: &[ComparedColumn<'_>],
) -> ComparisonSql {
    let selected: Vec<String> = columns
        .iter()
        .map(|column| {
            let name = format!("\"{}\"", column.name);
            match TEXT_COMPARED_TYPES.contains(&column.data_type) {
                true => format!("TO_VARCHAR({name}) AS {name}"),
                false => name,
            }
        })
        .collect();
    let select = selected.join(", ");
    let minus = |from: &str, without: &str| {
        format!("SELECT {select} FROM {from} MINUS SELECT {select} FROM {without}")
    };
    let (left_minus, right_minus) = (minus(left, right), minus(right, left));
    let sample = |minus: &str| {
        format!("SELECT TO_VARCHAR(OBJECT_CONSTRUCT_KEEP_NULL(*)) FROM ({minus}) LIMIT 5")
    };
    ComparisonSql {
        counts: format!(
            "SELECT (SELECT COUNT(*) FROM ({left_minus})) || ' ' || (SELECT COUNT(*) FROM ({right_minus})) || ' ' || (SELECT COUNT(*) FROM {left}) || ' ' || (SELECT COUNT(*) FROM {right})"
        ),
        left_only: sample(&left_minus),
        right_only: sample(&right_minus),
    }
}
