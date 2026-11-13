use crate::load_naming::main::_dlt_child_table::dlt_child_table;

/// The child table of `orders` for `field`.
pub(crate) fn child_of_orders(field: &str, max_length: Option<usize>) -> String {
    dlt_child_table("orders", field, max_length)
}
