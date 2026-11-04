//! Decide whether a live column can take a desired type, and what change it needs if not.

use crate::column_types::_helpers::fit::{holds, widening};
use crate::column_types::main::warehouse_type::warehouse_type;
use crate::column_types::models::{ColumnType, TypeFit, Warehouse};

/// Whether `live` holds every `desired` value on `warehouse`, can be widened to, or cannot.
pub fn type_fit(desired: &ColumnType, live: &ColumnType, warehouse: Warehouse) -> TypeFit {
    if desired == live
        || warehouse_type(desired, warehouse).sql == warehouse_type(live, warehouse).sql
        || holds(live, desired)
    {
        return TypeFit::Fits;
    }
    widening(desired, live, warehouse)
}
