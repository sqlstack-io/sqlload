/// The stage loads write their files to: the user's own stage, which needs no grants.
pub const USER_STAGE: &str = "~";

/// Folder on the stage for one load's files.
pub const STAGE_FOLDER: &str = "sqlload";

/// The schema sessions start in; every statement names its objects fully.
pub const SESSION_SCHEMA: &str = "PUBLIC";
