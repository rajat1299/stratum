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

#[cfg(test)]
mod tests {
    use super::{ChangeKind, ChangedPath, PathKind, PathRecord, StatusSummary};

    #[test]
    fn changed_path_round_trips_through_json_without_dropping_metadata() {
        let change = ChangedPath {
            path: "/docs/report.md".to_string(),
            kind: ChangeKind::Modified,
            before: Some(PathRecord {
                path: "/docs/report.md".to_string(),
                kind: PathKind::File,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                size: 10,
                content_id: None,
                mime_type: Some("text/markdown".to_string()),
                custom_attrs: std::collections::BTreeMap::from([(
                    "reviewed".to_string(),
                    "false".to_string(),
                )]),
            }),
            after: Some(PathRecord {
                path: "/docs/report.md".to_string(),
                kind: PathKind::File,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                size: 20,
                content_id: None,
                mime_type: Some("text/markdown".to_string()),
                custom_attrs: std::collections::BTreeMap::from([(
                    "reviewed".to_string(),
                    "true".to_string(),
                )]),
            }),
        };

        let encoded = serde_json::to_string(&change).expect("change should serialize");
        let decoded: ChangedPath =
            serde_json::from_str(&encoded).expect("change should deserialize");

        assert_eq!(decoded, change);
    }

    #[test]
    fn status_summary_clean_state_depends_only_on_changes() {
        let summary = StatusSummary {
            head: None,
            object_count: 1,
            file_count: 1,
            total_size: 12,
            changes: Vec::new(),
        };

        assert!(summary.is_clean());
    }
}
