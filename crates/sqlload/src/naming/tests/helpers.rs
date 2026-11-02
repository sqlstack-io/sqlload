use crate::config::main::parse_config::parse_config;
use crate::config::models::Config;

/// A configuration with `naming` as its `[naming]` section.
pub(crate) fn config_with_naming(naming: &str) -> Result<Config, String> {
    let text = format!(
        "[source]\nkind = \"kafka\"\nbrokers = \"b\"\ninclude = [\"*\"]\n[destination]\nkind = \"snowflake\"\naccount = \"a\"\nuser = \"u\"\nauth = {{ method = \"pat\", token = \"t\" }}\ndatabase = \"D\"\nschema = \"S\"\n[staging]\nkind = \"internal\"\n[naming]\n{naming}\n"
    );
    parse_config(&text, &|_| None).map_err(|error| error.to_string())
}

/// Owned topic names.
pub(crate) fn topics(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}
