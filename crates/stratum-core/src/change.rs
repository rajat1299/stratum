use crate::ObjectId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeKind {
    Added,
    Modified,
    Deleted,
    TypeChanged,
    MetadataChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PathKind {
    File,
    Directory,
    Symlink,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathRecord {
    pub path: String,
    pub kind: PathKind,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub content_id: Option<ObjectId>,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub custom_attrs: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangedPath {
    pub path: String,
    pub kind: ChangeKind,
    pub before: Option<PathRecord>,
    pub after: Option<PathRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusSummary {
    pub head: Option<ObjectId>,
    pub object_count: usize,
    pub file_count: u64,
    pub total_size: u64,
    pub changes: Vec<ChangedPath>,
}

impl StatusSummary {
    pub fn is_clean(&self) -> bool {
        self.changes.is_empty()
    }
}
