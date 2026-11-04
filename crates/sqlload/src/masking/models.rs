use std::collections::BTreeMap;

use crate::auth::models::Secret;
use crate::project::models::MaskMethod;

/// The columns of one table to mask, and the key `hmac` uses.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MaskRules {
    pub columns: BTreeMap<String, MaskMethod>,
    pub key: Option<Secret>,
}
