//! Folding a table's changes in a batch: one row per key for `merge`, every change for a changelog.

use std::collections::BTreeMap;

use crate::cdc::models::{Change, ChangeOp, Relation, TupleValue};

/// The positions of `key` columns in `relation`, or the column that is missing.
pub(crate) fn key_positions(relation: &Relation, key: &[String]) -> Result<Vec<usize>, String> {
    key.iter()
        .map(|name| {
            relation
                .columns
                .iter()
                .position(|column| column.name == *name)
                .ok_or_else(|| {
                    format!(
                        "{}.{} has no key column {name} on the stream",
                        relation.schema, relation.table
                    )
                })
        })
        .collect()
}

fn key_of(values: &[TupleValue], positions: &[usize]) -> Vec<TupleValue> {
    positions
        .iter()
        .map(|position| values.get(*position).cloned().unwrap_or(TupleValue::Null))
        .collect()
}

/// Each key's last change in stream order, keys in first-seen order: an update's unchanged TOAST values are taken from the key's earlier change in the batch where it had them, and an update that moves a row to a new key deletes the old key first.
pub(crate) fn fold_changes(changes: &[Change], positions: &[usize]) -> Vec<Change> {
    let mut order: Vec<Vec<TupleValue>> = Vec::new();
    let mut last: Folded = BTreeMap::new();
    for change in changes {
        let key = key_of(&change.values, positions);
        if let Some(old) = &change.old_key {
            let old_key = key_of(old, positions);
            if old_key != key {
                let mut deleted = change.clone();
                deleted.op = ChangeOp::Delete;
                deleted.values = old.clone();
                deleted.old_key = None;
                put((&mut order, &mut last), old_key, deleted);
            }
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
        put((&mut order, &mut last), key, change);
    }
    order
        .into_iter()
        .filter_map(|key| last.remove(&key))
        .collect()
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
