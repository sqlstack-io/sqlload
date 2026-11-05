use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::main::holds_values::holds_values;
use crate::column_types::main::widened_type::widened_type;
use crate::column_types::models::{ColumnType, TypeFit};

/// Whether `live` holds every `desired` value on the adapter's warehouse, can be widened to, or cannot.
pub(crate) fn type_fit<A: SqlAdapter + ?Sized>(
    adapter: &A,
    desired: &ColumnType,
    live: &ColumnType,
) -> TypeFit {
    let sql = |column_type: &ColumnType| adapter.render_loader_logical_type(column_type).sql;
    if desired == live || sql(desired) == sql(live) || holds_values(live, desired) {
        return TypeFit::Fits;
    }
    let Some(target) = widened_type(desired, live) else {
        return TypeFit::Incompatible;
    };
    match adapter.widens_in_place(live, &target) {
        true => TypeFit::WidenInPlace(sql(&target)),
        false => TypeFit::NeedsRebuild(sql(&target)),
    }
}
