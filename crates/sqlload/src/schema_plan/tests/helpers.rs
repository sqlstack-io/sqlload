use crate::column_types::models::{ColumnType, Warehouse};
use crate::project::models::{Contract, OnSchemaChange, Policy};
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn, LiveColumn, SchemaRules};

pub(crate) fn source(name: &str, column_type: ColumnType) -> DesiredColumn {
    DesiredColumn {
        name: name.to_owned(),
        column_type,
        origin: ColumnOrigin::Source,
    }
}

pub(crate) fn pinned(name: &str, column_type: ColumnType) -> DesiredColumn {
    DesiredColumn {
        name: name.to_owned(),
        column_type,
        origin: ColumnOrigin::Pinned,
    }
}

pub(crate) fn live(name: &str, column_type: ColumnType) -> LiveColumn {
    LiveColumn {
        name: name.to_owned(),
        column_type,
        fraction_digits: None,
    }
}

/// Snowflake with every default policy.
pub(crate) fn snowflake_rules() -> SchemaRules {
    SchemaRules {
        warehouse: Warehouse::Snowflake,
        contract: Contract::None,
        on_schema_change: OnSchemaChange::AppendNewColumns,
        type_change_rebuild: Policy::RequireConfirmation,
        rebuild_allowed: false,
    }
}
