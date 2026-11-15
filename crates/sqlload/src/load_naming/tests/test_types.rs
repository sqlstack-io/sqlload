pub(crate) struct DltNameTestCase {
    pub(crate) description: &'static str,
    /// `dlt_identifier`, `dlt_path` or a child table of one parent.
    pub(crate) normalise: fn(&str, Option<usize>) -> String,
    pub(crate) name: &'static str,
    pub(crate) max_length: Option<usize>,
    pub(crate) expected_name: &'static str,
}

pub(crate) struct LoadedColumnsTestCase {
    pub(crate) description: &'static str,
    /// Each column's source name, in order (a repeated header repeats it).
    pub(crate) columns: &'static [&'static str],
    pub(crate) naming: crate::load_naming::models::Naming,
    pub(crate) max_length: Option<usize>,
    /// `rename` as a map.
    pub(crate) renamed: &'static [(&'static str, &'static str)],
    /// What a rename function returns, by position.
    pub(crate) returned: &'static [(usize, &'static str)],
    pub(crate) on_duplicate_names: crate::load_naming::models::OnDuplicateNames,
    /// The names earlier loads' numbering gave, as `(column, source, repeat)`.
    pub(crate) kept: &'static [(&'static str, &'static str, usize)],
    /// The columns the table already has, when it exists.
    pub(crate) existing: &'static [&'static str],
    /// Each column's loaded name, joined by `,`; or the problems, joined by `; `.
    pub(crate) expected_names: Result<&'static str, &'static str>,
}

pub(crate) struct NumberedHeaderTestCase {
    pub(crate) description: &'static str,
    pub(crate) header: &'static [&'static str],
    /// Each read name, with `<` and the header it repeats when it is a repeat, joined by `,`.
    pub(crate) expected_read: &'static str,
}

pub(crate) struct NamedBatchTestCase {
    pub(crate) description: &'static str,
    /// Each batch's field names, in the order the batches arrive.
    pub(crate) batches: &'static [&'static [&'static str]],
    /// The table's `exclude_columns` patterns.
    pub(crate) excluded: &'static [&'static str],
    /// Each batch's fields as they load, joined by `,`; or the first problem.
    pub(crate) expected_names: Result<&'static [&'static str], &'static str>,
    /// The names numbering gave once every batch arrived, as `column<source`, joined by `,`.
    pub(crate) expected_numbered: &'static str,
}
