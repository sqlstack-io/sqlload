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

pub(crate) fn inferred(name: &str, column_type: ColumnType) -> DesiredColumn {
    DesiredColumn {
        name: name.to_owned(),
        column_type,
        origin: ColumnOrigin::Inferred,
    }
}

pub(crate) fn constant(name: &str, column_type: ColumnType) -> DesiredColumn {
    DesiredColumn {
        name: name.to_owned(),
        column_type,
        origin: ColumnOrigin::Constant,
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
        naming: crate::load_naming::models::Naming::Sqlload,
        auto_widen: false,
    }
}

/// Postgres, a warehouse that keeps tables SQLLoad did not create as they are, with every default policy.
pub(crate) fn postgres_rules() -> SchemaRules {
    SchemaRules {
        warehouse: Warehouse::Postgres,
        ..snowflake_rules()
    }
}
