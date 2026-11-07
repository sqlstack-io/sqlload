//! Folding a table's changes in a batch: one row per key for `merge`, every change for a changelog.

use std::collections::BTreeMap;

use crate::cdc::constants::FULL_IDENTITY;
use crate::cdc::models::{Change, ChangeOp, Relation, TupleValue};

/// The positions of `key` columns in `relation`, or why they cannot match changes: a column missing, or one outside the replica identity, whose old value a delete does not carry (replica identity full carries them all).
pub(crate) fn key_positions(relation: &Relation, key: &[String]) -> Result<Vec<usize>, String> {
    let full = relation.replica_identity == FULL_IDENTITY;
    let mut positions: Vec<usize> = Vec::with_capacity(key.len());
    for name in key {
        let Some(position) = relation
            .columns
            .iter()
            .position(|column| column.name == *name)
        else {
            return Err(format!(
                "{}.{} has no key column {name} on the stream",
                relation.schema, relation.table
            ));
        };
        let identified = relation
            .columns
            .get(position)
            .is_some_and(|column| column.key);
        if !identified && !full {
            return Err(format!(
                "{}.{}: unique_key column {name} is not in the table's replica identity, so deletes cannot find their rows; use the primary key, or set REPLICA IDENTITY FULL",
                relation.schema, relation.table
            ));
        }
        positions.push(position);
    }
    Ok(positions)
}

fn key_of(values: &[TupleValue], positions: &[usize]) -> Vec<TupleValue> {
    positions
        .iter()
        .map(|position| values.get(*position).cloned().unwrap_or(TupleValue::Null))
        .collect()
}

/// Each key's last change in stream order, keys in first-seen order: an update's unchanged TOAST values are taken from the key's earlier change in the batch where it had them, and an update that moves a row to a new key deletes the old key first. The moved row is new to the table, so a value it left out must come from its old row (replica identity full sends it); otherwise the batch fails rather than write NULL.
pub(crate) fn fold_changes(changes: &[Change], positions: &[usize]) -> Result<Vec<Change>, String> {
    let mut order: Vec<Vec<TupleValue>> = Vec::new();
    let mut last: Folded = BTreeMap::new();
    for change in changes {
        let key = key_of(&change.values, positions);
        let moved = change
            .old_key
            .as_ref()
            .filter(|old| key_of(old, positions) != key);
        if let Some(old) = moved {
            let mut deleted = change.clone();
            deleted.op = ChangeOp::Delete;
            deleted.values = old.clone();
            deleted.old_key = None;
            put((&mut order, &mut last), key_of(old, positions), deleted);
        }
        let mut change = change.clone();
        if let Some(earlier) = last
            .get(&key)
            .filter(|earlier| earlier.op != ChangeOp::Delete)
        {
            for (value, before) in change.values.iter_mut().zip(&earlier.values) {
                if *value == TupleValue::Unchanged && *before != TupleValue::Unchanged {
                    *value = before.clone();
                }
            }
        }
        if let Some(old) = moved {
            fill_from_old(&mut change.values, old)?;
        }
        put((&mut order, &mut last), key, change);
    }
    Ok(order
        .into_iter()
        .filter_map(|key| last.remove(&key))
        .collect())
}

/// A moved row's values left out as unchanged, taken from its old row; an error when the old row lacks them.
fn fill_from_old(values: &mut [TupleValue], old: &[TupleValue]) -> Result<(), String> {
    for (value, before) in values.iter_mut().zip(old) {
        if *value == TupleValue::Unchanged && matches!(before, TupleValue::Text(_)) {
            *value = before.clone();
        }
    }
    match values.contains(&TupleValue::Unchanged) {
        true => Err("an update changed a row's key and left a large (TOASTed) value out, which the new row needs; set REPLICA IDENTITY FULL on the table so the old row comes with it".to_owned()),
        false => Ok(()),
    }
}

type Folded = BTreeMap<Vec<TupleValue>, Change>;

/// Record `change` as `key`'s last, noting a key seen for the first time.
fn put(
    (order, last): (&mut Vec<Vec<TupleValue>>, &mut Folded),
    key: Vec<TupleValue>,
    change: Change,
) {
    if !last.contains_key(&key) {
        order.push(key.clone());
    }
    last.insert(key, change);
}
