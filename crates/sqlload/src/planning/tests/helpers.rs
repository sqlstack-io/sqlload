use crate::config::main::parse_config::parse_config;
use crate::naming::main::resolve_streams::resolve_streams;
use crate::planning::main::build_plan::build_plan;
use crate::planning::models::{ExistingObjects, Plan};

const BASE: &str = "[source]\nkind = \"kafka\"\nbrokers = \"b\"\ninclude = [\"*\"]\n[destination]\nkind = \"snowflake\"\naccount = \"a\"\nuser = \"u\"\nauth = { method = \"pat\", token = \"t\" }\ndatabase = \"MY_DB\"\nschema = \"RAW\"\n";

/// The plan for topic `shop.orders` with `staging` appended to a base configuration.
pub(crate) fn plan_for(
    staging: &str,
    columns: &[(&str, &str)],
    key_pair: bool,
) -> Result<Plan, String> {
    let config =
        parse_config(&format!("{BASE}{staging}"), &|_| None).map_err(|error| error.to_string())?;
    let streams = resolve_streams(&config, &["shop.orders".to_owned()])
        .map_err(|problems| problems.join("; "))?;
    let mut existing = ExistingObjects::default();
    let table = columns
        .iter()
        .map(|(name, data_type)| ((*name).to_owned(), (*data_type).to_owned()))
        .collect();
    existing
        .tables
        .extend(Some(("SHOP__ORDERS".to_owned(), table)).filter(|_| !columns.is_empty()));
    Ok(build_plan(&config, &streams, &existing, key_pair))
}

/// `<marker><kind> <name>` for each object of the plan's streams.
pub(crate) fn object_summary(plan: &Plan) -> Vec<String> {
    plan.streams
        .iter()
        .flat_map(|stream| stream.objects.iter())
        .map(|object| format!("{}{} {}", object.state.marker(), object.kind, object.name))
        .collect()
}
