use super::ObjectId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeEntry {
    pub name: String,
    pub kind: TreeEntryKind,
    pub id: ObjectId,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub custom_attrs: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TreeEntryKind {
    Blob,
    Tree,
    Symlink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeObject {
    pub entries: Vec<TreeEntry>,
}

#[derive(Debug, Clone, Deserialize)]
struct TreeEntryV1 {
    name: String,
    kind: TreeEntryKind,
    id: ObjectId,
    mode: u16,
    uid: u32,
    gid: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct TreeObjectV1 {
    entries: Vec<TreeEntryV1>,
}

impl TreeObject {
    pub fn serialize(&self) -> Vec<u8> {
        crate::codec::serialize(self).expect("tree serialization should not fail")
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, crate::codec::DecodeError> {
        match crate::codec::deserialize(data) {
            Ok(tree) => Ok(tree),
            Err(_) => {
                let legacy: TreeObjectV1 = crate::codec::deserialize(data)?;
                let entries = legacy
                    .entries
                    .into_iter()
                    .map(|entry| TreeEntry {
                        name: entry.name,
                        kind: entry.kind,
                        id: entry.id,
                        mode: entry.mode,
                        uid: entry.uid,
                        gid: entry.gid,
                        mime_type: None,
                        custom_attrs: BTreeMap::new(),
                    })
                    .collect();
                Ok(Self { entries })
            }
        }
    }
}
