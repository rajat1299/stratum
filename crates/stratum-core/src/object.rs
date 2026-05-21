use crate::VfsError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId([u8; 32]);

impl ObjectId {
    pub fn from_bytes(data: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        let mut id = [0u8; 32];
        id.copy_from_slice(&result);
        ObjectId(id)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    pub fn to_hex(&self) -> String {
        self.hex()
    }

    pub fn short_hex(&self) -> String {
        self.hex()[..8].to_string()
    }

    pub fn from_raw(bytes: [u8; 32]) -> Self {
        ObjectId(bytes)
    }

    pub fn from_hex(hex: &str) -> Result<Self, VfsError> {
        if hex.len() != 64 {
            return Err(VfsError::InvalidArgs {
                message: format!("invalid object ID: {hex}"),
            });
        }
        let mut bytes = [0u8; 32];
        for i in 0..32 {
            bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|_| {
                VfsError::InvalidArgs {
                    message: format!("invalid hex: {hex}"),
                }
            })?;
        }
        Ok(ObjectId(bytes))
    }
}

impl std::fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObjectId({})", self.short_hex())
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.hex())
    }
}

impl Serialize for ObjectId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        if bytes.len() != 32 {
            return Err(serde::de::Error::custom("expected 32 bytes for ObjectId"));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(ObjectId(arr))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectKind {
    Blob,
    Tree,
    Commit,
}

#[cfg(test)]
mod tests {
    use super::{ObjectId, ObjectKind};

    #[test]
    fn object_id_round_trips_through_serde_bytes() {
        let id = ObjectId::from_bytes(b"stratum object");
        let encoded = serde_json::to_vec(&id).expect("object id should serialize");
        let decoded: ObjectId =
            serde_json::from_slice(&encoded).expect("object id should deserialize");

        assert_eq!(decoded, id);
    }

    #[test]
    fn object_kind_round_trips_through_serde_name() {
        let encoded = serde_json::to_string(&ObjectKind::Tree).expect("kind should serialize");
        let decoded: ObjectKind = serde_json::from_str(&encoded).expect("kind should deserialize");

        assert_eq!(decoded, ObjectKind::Tree);
    }
}
