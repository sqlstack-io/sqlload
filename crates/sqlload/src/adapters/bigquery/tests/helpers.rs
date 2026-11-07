use crate::adapters::contract::models::TableName;

/// `` `p`.`raw`.`orders` ``'s name.
pub(crate) fn orders() -> TableName {
    TableName {
        database: Some("p".to_owned()),
        schema: "raw".to_owned(),
        table: "orders".to_owned(),
    }
}
