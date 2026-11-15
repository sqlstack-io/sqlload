//! `on_duplicate_names`: numbering the columns that load under one name, keeping the numbers earlier loads gave.

use std::collections::BTreeSet;

use crate::load_naming::constants::PAIR;
use crate::load_naming::models::{
    LoadedColumn, NameChange, NameRules, NumberedName, OnDuplicateNames,
};

/// A group's members: each column's name numbering gives it (empty until given), with its index in the table's columns.
type Members = Vec<(NumberedName, usize)>;

/// `loaded` with each group of columns that load under one name (ignoring case, as warehouses fold it) numbered, and every numbered group's names, which later loads keep; or why a group can't be numbered.
pub(crate) fn numbered_columns(
    loaded: Vec<LoadedColumn>,
    rules: &NameRules,
) -> Result<(Vec<LoadedColumn>, Vec<NumberedName>), Vec<String>> {
    let groups = clash_groups(&loaded);
    let mut taken: BTreeSet<String> = groups.iter().map(|(key, _)| key.clone()).collect();
    let mut numbered: Members = Vec::new();
    let mut problems: Vec<String> = Vec::new();
    for (key, indexes) in &groups {
        match group_numbers(&loaded, (key, indexes), rules, &taken) {
            Ok(members) => {
                taken.extend(members.iter().map(|(name, _)| name.column.to_lowercase()));
                numbered.extend(members);
            }
            Err(found) => problems.extend(found),
        }
    }
    match problems.is_empty() {
        true => Ok((
            numbered_loaded(loaded, &numbered),
            numbered.into_iter().map(|(name, _)| name).collect(),
        )),
        false => Err(problems),
    }
}

/// The indexes of `loaded`'s columns by the name they load as, in lower case, in the order the names first come.
fn clash_groups(loaded: &[LoadedColumn]) -> Vec<(String, Vec<usize>)> {
    let mut groups: Vec<(String, Vec<usize>)> = Vec::new();
    for (index, column) in loaded.iter().enumerate() {
        let key = column.loaded.to_lowercase();
        match groups.iter_mut().find(|(name, _)| *name == key) {
            Some((_, indexes)) => indexes.push(index),
            None => groups.push((key, vec![index])),
        }
    }
    groups
}

/// `loaded` with each numbered column under its number.
fn numbered_loaded(mut loaded: Vec<LoadedColumn>, numbered: &Members) -> Vec<LoadedColumn> {
    for (name, index) in numbered {
        let column = &mut loaded[*index];
        if column.loaded != name.column {
            column.loaded.clone_from(&name.column);
            column.change = Some(NameChange::Numbered);
        }
    }
    loaded
}

/// The names numbering gives the columns of one group, the columns at `indexes` that load as `key`, none when it needs none; or why it can't: `on_duplicate_names = "fail"`, a clash on a column the table has that no earlier load recorded, or a number given before to a source name that has gone, which another would take.
fn group_numbers(
    loaded: &[LoadedColumn],
    (key, indexes): (&str, &[usize]),
    rules: &NameRules,
    taken: &BTreeSet<String>,
) -> Result<Members, Vec<String>> {
    let base = &loaded[indexes[0]].loaded;
    let recorded: Vec<&NumberedName> = rules
        .kept
        .iter()
        .filter(|kept| numbered_from(&kept.column, base))
        .collect();
    let (renamed, plain): (Vec<usize>, Vec<usize>) = indexes
        .iter()
        .partition(|index| loaded[**index].change == Some(NameChange::Renamed));
    if indexes.len() < PAIR && recorded.is_empty() {
        return Ok(Vec::new());
    }
    let members = with_kept(sorted_members(loaded, &plain), &recorded);
    let refused = refused_clash(loaded, &plain, (rules.on_duplicate_names, base))
        .or_else(|| unrecorded_clash(loaded, &members, (&recorded, base), &rules.existing));
    if let Some(problem) = refused {
        return Err(vec![problem]);
    }
    let held: BTreeSet<String> = renamed
        .iter()
        .map(|index| loaded[*index].loaded.to_lowercase())
        .chain(members.iter().map(|(name, _)| name.column.to_lowercase()))
        .filter(|name| !name.is_empty())
        .collect();
    given_numbers(
        loaded,
        members,
        (&recorded, rules.max_length),
        (held, taken, key),
    )
}

/// The columns at `plain` as members with their source names, in source-name order, repeats of one header in their order.
fn sorted_members(loaded: &[LoadedColumn], plain: &[usize]) -> Members {
    let mut members: Members = plain
        .iter()
        .map(|index| {
            let column = &loaded[*index];
            let name = NumberedName {
                column: String::new(),
                source: column.source.clone(),
                repeat: repeat_of(column, loaded),
            };
            (name, *index)
        })
        .collect();
    members.sort_by(|(left, _), (right, _)| {
        (&left.source, left.repeat).cmp(&(&right.source, right.repeat))
    });
    members
}

/// `members`, each under the name `recorded` gave its source name in an earlier load, if any.
fn with_kept(members: Members, recorded: &[&NumberedName]) -> Members {
    members
        .into_iter()
        .map(|(name, index)| (kept_name(name, recorded), index))
        .collect()
}

/// `name` under the column `recorded` gave its source name, if any.
fn kept_name(mut name: NumberedName, recorded: &[&NumberedName]) -> NumberedName {
    let kept = recorded
        .iter()
        .find(|kept| kept.source == name.source && kept.repeat == name.repeat);
    if let Some(kept) = kept {
        name.column.clone_from(&kept.column);
    }
    name
}

/// The problem under `on_duplicate_names = "fail"` when the columns at `plain` meet under `base`.
fn refused_clash(
    loaded: &[LoadedColumn],
    plain: &[usize],
    (on_duplicate_names, base): (OnDuplicateNames, &str),
) -> Option<String> {
    if on_duplicate_names != OnDuplicateNames::Fail || plain.len() < PAIR {
        return None;
    }
    let names: Vec<String> = plain
        .iter()
        .map(|index| shown(&loaded[*index], loaded))
        .collect();
    Some(format!(
        "columns {} {} load as {base}; give them names of their own with rename, or set on_duplicate_names = \"number\"",
        names.join(" and "),
        both_or_all(names.len())
    ))
}

/// The problem when different source names meet under `base`, a column the table has, which no earlier load recorded as numbered: which of them it holds is unknown.
fn unrecorded_clash(
    loaded: &[LoadedColumn],
    members: &Members,
    (recorded, base): (&[&NumberedName], &str),
    existing: &[String],
) -> Option<String> {
    let sources: BTreeSet<&str> = members
        .iter()
        .map(|(name, _)| name.source.as_str())
        .collect();
    let unrecorded = !recorded
        .iter()
        .any(|kept| kept.column.eq_ignore_ascii_case(base));
    let live = existing
        .iter()
        .any(|column| column.eq_ignore_ascii_case(base));
    if sources.len() < PAIR || !live || !unrecorded {
        return None;
    }
    let names: Vec<String> = members
        .iter()
        .map(|(_, index)| shown(&loaded[*index], loaded))
        .collect();
    Some(format!(
        "columns {} now {} load as {base}, which the table already has, and no earlier load recorded which of them it holds; give all but one a name of its own with rename",
        names.join(" and "),
        both_or_all(names.len())
    ))
}

/// `members` each under a name: the one an earlier load gave it, else its own name or the next number past every name `held` by the group or `taken` by another; or the problems when a number recorded for a source name that has gone would go to another.
fn given_numbers(
    loaded: &[LoadedColumn],
    members: Members,
    (recorded, max_length): (&[&NumberedName], Option<usize>),
    (mut held, taken, key): (BTreeSet<String>, &BTreeSet<String>, &str),
) -> Result<Members, Vec<String>> {
    let mut number = 0;
    let mut problems: Vec<String> = Vec::new();
    let mut named: Members = Vec::new();
    for (mut name, index) in members {
        if name.column.is_empty() {
            let own = &loaded[index].loaded;
            let (column, next) = free_name(own, number, (&held, taken, key), max_length);
            number = next;
            problems.extend(gone_problem(loaded, recorded, (&column, index)));
            held.insert(column.to_lowercase());
            name.column = column;
        }
        named.push((name, index));
    }
    match problems.is_empty() {
        true => Ok(named),
        false => Err(problems),
    }
}

/// The first of `own`, `own_1`, `own_2`… counting from `number` that the group doesn't hold and no other group takes (`key`, the group's own name, never counts as taken), with the number after it.
fn free_name(
    own: &str,
    number: usize,
    (held, taken, key): (&BTreeSet<String>, &BTreeSet<String>, &str),
    max_length: Option<usize>,
) -> (String, usize) {
    (number..)
        .map(|number| match number {
            0 => (own.to_owned(), 1),
            _ => (numbered_name(own, number, max_length), number + 1),
        })
        .find(|(candidate, _)| {
            let lower = candidate.to_lowercase();
            !held.contains(&lower) && (lower == key || !taken.contains(&lower))
        })
        .unwrap_or_default()
}

/// The problem when `column`, given to the column at `index`, is a name an earlier load gave a source name `loaded` no longer has.
fn gone_problem(
    loaded: &[LoadedColumn],
    recorded: &[&NumberedName],
    (column, index): (&str, usize),
) -> Option<String> {
    let gone = recorded
        .iter()
        .find(|kept| kept.column.eq_ignore_ascii_case(column))?;
    (!has_source(loaded, &gone.source)).then(|| {
        format!(
            "column {column} holds {:?} from earlier loads, which the source no longer has, and {} would now load into it; give {:?} a name of its own with rename, or rename it to {column} if it is the same column under a new name",
            gone.source,
            shown(&loaded[index], loaded),
            loaded[index].source
        )
    })
}

/// Whether a column of `loaded` has the source name `source`.
fn has_source(loaded: &[LoadedColumn], source: &str) -> bool {
    loaded.iter().any(|column| column.source == source)
}

/// `both` for two names, `all` for more.
fn both_or_all(count: usize) -> &'static str {
    match count > PAIR {
        true => "all",
        false => "both",
    }
}

/// Whether `column` is `base`, or `base` numbered (`base_2`), ignoring case.
fn numbered_from(column: &str, base: &str) -> bool {
    let column = column.to_lowercase();
    let base = base.to_lowercase();
    if column == base {
        return true;
    }
    let Some(digits) = column
        .strip_prefix(&base)
        .and_then(|rest| rest.strip_prefix('_'))
    else {
        return false;
    };
    !digits.is_empty() && digits.bytes().all(|digit| digit.is_ascii_digit())
}

/// `base` with `_number` after it, `base` cut short so the whole fits `max_length`.
fn numbered_name(base: &str, number: usize, max_length: Option<usize>) -> String {
    let suffix = format!("_{number}");
    let kept = max_length.map_or(usize::MAX, |max| max.saturating_sub(suffix.chars().count()));
    let head: String = base.chars().take(kept).collect();
    format!("{head}{suffix}")
}

/// How many columns of `loaded` before `column` have its source name: 0 for the first of a repeated header.
pub(crate) fn repeat_of(column: &LoadedColumn, loaded: &[LoadedColumn]) -> usize {
    loaded
        .iter()
        .filter(|earlier| earlier.position < column.position && earlier.source == column.source)
        .count()
}

/// Whether another column of `loaded` has `column`'s source name: a repeated header.
pub(crate) fn repeated(column: &LoadedColumn, loaded: &[LoadedColumn]) -> bool {
    loaded
        .iter()
        .any(|other| other.position != column.position && other.source == column.source)
}

/// `column` as a problem names it: its source name, and its place when another column has that name too.
pub(crate) fn shown(column: &LoadedColumn, loaded: &[LoadedColumn]) -> String {
    match repeated(column, loaded) {
        true => format!("{:?} (column {})", column.source, column.position),
        false => format!("{:?}", column.source),
    }
}

/// Whether another column of `columns` loads as `name`'s column or as it numbered (`x` beside `x_1`), so `name` belongs to a numbered group rather than only holding the name an earlier batch gave its field.
pub(crate) fn clashes(name: &NumberedName, columns: &[LoadedColumn]) -> bool {
    columns.iter().any(|column| {
        column.source != name.source
            && (numbered_from(&column.loaded, &name.column)
                || numbered_from(&name.column, &column.loaded))
    })
}
