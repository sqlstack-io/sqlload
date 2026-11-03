use std::collections::{BTreeMap, BTreeSet};

use crate::naming::models::Stream;

/// How a stream's rows reach its table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadPath {
    Streaming,
    StageInsertFiles,
    StageCopy,
    S3AutoIngest,
    S3InsertFiles,
    S3Copy,
}

/// The warehouse object types sqlload creates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObjectKind {
    Table,
    Stage,
    Pipe,
}

impl std::fmt::Display for ObjectKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Table => "table",
            Self::Stage => "stage",
            Self::Pipe => "pipe",
        })
    }
}

/// Whether an object exists and fits the configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectState {
    Exists,
    Missing,
    Incompatible(Vec<String>),
}

impl ObjectState {
    /// `=` exists, `+` will be created, `!` incompatible.
    pub fn marker(&self) -> &'static str {
        match self {
            Self::Exists => "=",
            Self::Missing => "+",
            Self::Incompatible(_) => "!",
        }
    }
}

/// One object a stream needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedObject {
    pub kind: ObjectKind,
    pub name: String,
    pub state: ObjectState,
    pub create_sql: String,
}

impl PlannedObject {
    /// Why this object does not fit, each prefixed with its kind and name.
    pub fn problems(&self) -> Vec<String> {
        let ObjectState::Incompatible(reasons) = &self.state else {
            return Vec::new();
        };
        let prefix = format!("{} {}", self.kind, self.name);
        reasons
            .iter()
            .map(|reason| format!("{prefix}: {reason}"))
            .collect()
    }
}

/// One stream and the objects it needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamPlan {
    pub stream: Stream,
    pub load_path: LoadPath,
    pub objects: Vec<PlannedObject>,
}

/// Every stream's plan.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    pub streams: Vec<StreamPlan>,
}

impl Plan {
    /// Every reason the plan cannot be applied.
    pub fn problems(&self) -> Vec<String> {
        self.objects().flat_map(PlannedObject::problems).collect()
    }

    /// The objects that would be created.
    pub fn missing(&self) -> Vec<&PlannedObject> {
        self.objects()
            .filter(|object| object.state == ObjectState::Missing)
            .collect()
    }

    fn objects(&self) -> impl Iterator<Item = &PlannedObject> {
        self.streams.iter().flat_map(|stream| stream.objects.iter())
    }
}

/// What already exists in the destination schema.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExistingObjects {
    pub tables: BTreeMap<String, BTreeMap<String, String>>,
    pub stages: BTreeSet<String>,
    pub pipes: BTreeMap<String, Option<String>>,
}

/// The names a stage-loading pipe joins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagePipe<'a> {
    pub pipe: &'a str,
    pub table: &'a str,
    pub source: &'a str,
    pub auto_ingest: bool,
}
