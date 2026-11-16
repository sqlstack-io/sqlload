//! The pieces of the plan's lines.

use crate::loader::models::{PlanDetail, PlanItem, PlanView, TablePlan};

/// A table's line under its source: its name, its write strategy and key, where its read starts (empty when it reads everything), and its child lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    pub name: String,
    pub action: String,
    pub reads: String,
    pub details: Vec<PlanDetail>,
}

/// A source's line in `Loads`, and its tables' rows, sorted by name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceGroup {
    pub line: String,
    pub rows: Vec<TableRow>,
}

/// Skipped tables that share a reason, as `up to date` and `estimates (3 files)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedGroup {
    pub reason: String,
    pub tables: Vec<String>,
}

/// A table `load` loads, as the loader worked it out for `-v`: its plan, its name as it lands, its item with the details a plan of several tables shows (its action the write strategy in full), and where its read starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedTable<'a> {
    pub table: &'a TablePlan,
    pub name: String,
    pub item: PlanItem,
    pub reads: Option<String>,
}

/// What the default plan is printed from: the plan's sections as `-v` shows them, its tables to load, and its `Errors` and `Warnings` lines, which it prints as `-v` does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanParts<'a> {
    pub view: &'a PlanView,
    pub tables: Vec<LoadedTable<'a>>,
    pub problems: Vec<String>,
}
