//! Shared types used across modules

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A block message containing height and full block JSON
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockMsg {
    pub height: u64,
    pub block: Value,
}
