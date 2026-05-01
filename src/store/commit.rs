use super::ObjectId;
use crate::vcs::change::ChangedPath;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitObject {
    pub id: ObjectId,
    pub tree: ObjectId,
    pub parent: Option<ObjectId>,
    pub timestamp: u64,
    pub message: String,
    pub author: String,
    pub changed_paths: Vec<ChangedPath>,
}

impl CommitObject {
    pub fn serialize(&self) -> Vec<u8> {
        crate::codec::serialize(self).expect("commit serialization should not fail")
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, crate::codec::DecodeError> {
        crate::codec::deserialize(data)
    }
}
