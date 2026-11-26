//! Type names each warehouse reads its own way: accepted when every destination of the table reads them alike.

use std::collections::BTreeSet;

use crate::column_types::_helpers::names::{decimal, precision_only};
use crate::column_types::_helpers::syntax::TypeSyntax;
use crate::column_types::constants::{BIGNUMERIC_NAME, TIMESTAMP_NAME, TIMESTAMP_PORTABLE};
use crate::column_types::errors::TypeError;
use crate::column_types::models::{ColumnType, Warehouse};

/// How one warehouse reads a type name: as a portable type, or not as any one type, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Reading {
    Means(ColumnType),
    /// Fits the readings text, as in `follows its TIMESTAMP_TYPE_MAPPING in Snowflake`.
    Refused(&'static str),
}

/// The warehouses that read a name (by name, as `Warehouse` displays them), each with what it reads, and the portable name to write instead.
struct NameReadings {
    readings: Vec<(&'static str, Reading)>,
    portable: String,
}

/// What a warehouse that does not list a name reads it as.
const NO_TYPE: Reading = Reading::Refused("names no type");

/// The type `syntax` names for a table loading into `destinations`, when it is a name whose meaning depends on the warehouse (`Ok(None)` for any other name): the type every destination reads it as, or why they don't read it alike. With no destinations, every warehouse that reads it must read it alike.
pub(crate) fn destination_type(
    syntax: &TypeSyntax,
    written: &str,
    destinations: &[Warehouse],
) -> Result<Option<ColumnType>, TypeError> {
    let Some(NameReadings {
        readings: listed,
        portable,
    }) = readings(syntax, written)?
    else {
        return Ok(None);
    };
    let read = |warehouse: &String| {
        listed
            .iter()
            .find(|(listed, _)| listed == warehouse)
            .map_or(NO_TYPE, |(_, reading)| reading.clone())
    };
    let chosen: BTreeSet<String> = match destinations.is_empty() {
        true => listed
            .iter()
            .map(|(warehouse, _)| (*warehouse).to_owned())
            .collect(),
        false => destinations.iter().map(ToString::to_string).collect(),
    };
    let mut groups: Vec<(Reading, Vec<String>)> = Vec::new();
    for warehouse in chosen {
        let reading = read(&warehouse);
        match groups.iter_mut().find(|(each, _)| *each == reading) {
            Some((_, warehouses)) => warehouses.push(warehouse),
            None => groups.push((reading, vec![warehouse])),
        }
    }
    match groups.as_slice() {
        [(Reading::Means(column_type), _)] => Ok(Some(column_type.clone())),
        _ => Err(TypeError::DependsOnDestination {
            written: written.to_owned(),
            readings: readings_text(&groups),
            portable,
        }),
    }
}

/// Each warehouse that reads the name, with what it reads, and the portable name to write instead; `None` for a name every warehouse reads alike.
fn readings(syntax: &TypeSyntax, written: &str) -> Result<Option<NameReadings>, TypeError> {
    let args = syntax.args.as_deref().unwrap_or_default();
    match syntax.name.as_str() {
        TIMESTAMP_NAME => {
            precision_only(args, written, ColumnType::TimestampNtz)?;
            let readings = vec![
                ("DuckDB", Reading::Means(ColumnType::TimestampNtz)),
                ("Postgres", Reading::Means(ColumnType::TimestampNtz)),
                ("BigQuery", Reading::Means(ColumnType::TimestampTz)),
                ("Databricks", Reading::Means(ColumnType::TimestampTz)),
                (
                    "Snowflake",
                    Reading::Refused("follows the account's TIMESTAMP_TYPE_MAPPING"),
                ),
            ];
            Ok(Some(NameReadings {
                readings,
                portable: TIMESTAMP_PORTABLE.to_owned(),
            }))
        }
        BIGNUMERIC_NAME => {
            let (reading, portable) = match args {
                [] => (
                    Reading::Refused("is BIGNUMERIC(76.76,38), more digits than decimal's 38,"),
                    "\"decimal(p,s)\" with the precision and scale".to_owned(),
                ),
                _ => {
                    let column_type = decimal(args, written)?;
                    let portable = format!("{:?}", column_type.to_string());
                    (Reading::Means(column_type), portable)
                }
            };
            Ok(Some(NameReadings {
                readings: vec![("BigQuery", reading)],
                portable,
            }))
        }
        _ => Ok(None),
    }
}

/// `groups` (each reading with the warehouses that read it) as one phrase, such as `is timestamp_tz in BigQuery, but timestamp_ntz in DuckDB and Postgres`: a type after another type drops the `is` they share.
fn readings_text(groups: &[(Reading, Vec<String>)]) -> String {
    let parts: Vec<String> = groups
        .iter()
        .enumerate()
        .map(|(index, (reading, warehouses))| {
            let after_type = index > 0 && matches!(groups[index - 1].0, Reading::Means(_));
            let what = match reading {
                Reading::Means(column_type) if after_type => column_type.to_string(),
                Reading::Means(column_type) => format!("is {column_type}"),
                Reading::Refused(why) => (*why).to_owned(),
            };
            format!("{what} in {}", listed(warehouses))
        })
        .collect();
    match parts.split_last() {
        Some((last, [])) => last.clone(),
        Some((last, rest)) => format!("{}, but {last}", rest.join(", ")),
        None => String::new(),
    }
}

/// `warehouses` by name, as `DuckDB`, `DuckDB and Postgres` or `DuckDB, Postgres and BigQuery`.
fn listed(names: &[String]) -> String {
    match names.split_last() {
        Some((last, [])) => last.clone(),
        Some((last, rest)) => format!("{} and {last}", rest.join(", ")),
        None => String::new(),
    }
}
