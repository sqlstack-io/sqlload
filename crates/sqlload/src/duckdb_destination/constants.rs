/// Columns of one table, in table order.
pub const DESCRIBE_TABLE_SQL: &str = "SELECT column_name, data_type, numeric_precision, numeric_scale, character_maximum_length FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
