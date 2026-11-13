/// One column of a batch: its field and values.
pub type NamedColumn = (std::sync::Arc<arrow_schema::Field>, arrow_array::ArrayRef);

/// Under `unnest = "all"`, whether the field at each dotted path is a list field: chosen from the sampled values at plan, else by the first values that decide (a NULL or an empty list decides nothing), and kept for the rest of the load.
pub type ListChoices = std::collections::BTreeMap<String, bool>;
