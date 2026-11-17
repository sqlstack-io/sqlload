use crate::playground::models::BundledFile;

/// The project's folder when `sqlload playground` is given none.
pub const DEFAULT_NAME: &str = "shop";

/// How the playground's sqlload.toml starts; `--change` runs only in a project that starts so.
pub const PROJECT_HEADER: &str = "# Written by `sqlload playground`";

/// The playground's sqlload.toml.
pub const PROJECT_TOML: &str = include_str!("_helpers/shop/sqlload.toml");

/// The customers the project starts with.
pub const CUSTOMERS_CSV: &str = include_str!("_helpers/shop/data/customers/customers.csv");

/// The orders the project starts with; the changes are described against them.
pub const ORDERS_CSV: &str = include_str!("_helpers/shop/data/orders/orders_2026-11-01.csv");

/// The files `sqlload playground` writes.
pub const PROJECT_FILES: &[BundledFile] = &[
    BundledFile {
        path: "sqlload.toml",
        text: PROJECT_TOML,
    },
    BundledFile {
        path: ".gitignore",
        text: include_str!("_helpers/shop/gitignore"),
    },
    BundledFile {
        path: "data/customers/customers.csv",
        text: CUSTOMERS_CSV,
    },
    BundledFile {
        path: "data/orders/orders_2026-11-01.csv",
        text: ORDERS_CSV,
    },
];

/// The files of changed orders `--change` adds, one per run, in this order.
pub const CHANGES: &[BundledFile] = &[BundledFile {
    path: "data/orders/orders_2026-11-02.csv",
    text: include_str!("_helpers/shop/changes/orders_2026-11-02.csv"),
}];

/// What a changed row is called in `--change`'s lines, before its key.
pub const ROW_NOUN: &str = "order";

/// Where the playground's DuckDB file is, below the project, and its schema.
pub const DATABASE: &str = "shop.duckdb";

/// The schema the playground's tables load into.
pub const SCHEMA: &str = "raw";

/// The commands `sqlload playground` suggests, after `cd` into the project.
pub const NEXT_COMMANDS: &[&str] = &[
    "sqlload debug",
    "sqlload plan",
    "sqlload apply",
    "sqlload load",
    "sqlload playground --change",
    "sqlload load",
];
