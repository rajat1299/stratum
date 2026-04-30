use async_trait::async_trait;
use axum::http::HeaderValue;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::error::VfsError;

const IDEMPOTENCY_STORE_VERSION: u32 = 1;

pub type SharedIdempotencyStore = Arc<dyn IdempotencyStore>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyKey {
    key_hash: String,
}

impl IdempotencyKey {
    pub fn parse_header_value(value: &HeaderValue) -> Result<Self, VfsError> {
        let bytes = value.as_bytes();
        if bytes.is_empty() {
            return Err(VfsError::InvalidArgs {
                message: "Idempotency-Key must not be empty".to_string(),
            });
        }
        if bytes.len() > 255 {
            return Err(VfsError::InvalidArgs {
                message: "Idempotency-Key must be at most 255 bytes".to_string(),
            });
        }
        if !bytes.iter().all(|byte| matches!(byte, 0x21..=0x7e)) {
            return Err(VfsError::InvalidArgs {
                message: "Idempotency-Key must contain visible ASCII only".to_string(),
            });
        }

        Ok(Self {
            key_hash: sha256_hex(bytes),
        })
    }

    pub fn key_hash(&self) -> &str {
        &self.key_hash
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
struct IdempotencyStoreKey {
    scope: String,
    key_hash: String,
}

impl IdempotencyStoreKey {
    fn new(scope: &str, key: &IdempotencyKey) -> Self {
        Self {
            scope: scope.to_string(),
            key_hash: key.key_hash.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    request_fingerprint: String,
    pub status_code: u16,
    pub response_body: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyReservation {
    key: IdempotencyStoreKey,
    request_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdempotencyBegin {
    Execute(IdempotencyReservation),
    Replay(IdempotencyRecord),
    Conflict,
    InProgress,
}

#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    async fn begin(
        &self,
        scope: &str,
        key: &IdempotencyKey,
        request_fingerprint: &str,
    ) -> Result<IdempotencyBegin, VfsError>;

    async fn complete(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
    ) -> Result<(), VfsError>;

    async fn abort(&self, reservation: &IdempotencyReservation);
}

#[derive(Debug, Clone, Default)]
struct IdempotencyState {
    completed: BTreeMap<IdempotencyStoreKey, IdempotencyRecord>,
    pending: BTreeMap<IdempotencyStoreKey, String>,
}

#[derive(Debug, Default)]
pub struct InMemoryIdempotencyStore {
    inner: RwLock<IdempotencyState>,
}

impl InMemoryIdempotencyStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl IdempotencyStore for InMemoryIdempotencyStore {
    async fn begin(
        &self,
        scope: &str,
        key: &IdempotencyKey,
        request_fingerprint: &str,
    ) -> Result<IdempotencyBegin, VfsError> {
        let mut guard = self.inner.write().await;
        begin_locked(&mut guard, scope, key, request_fingerprint)
    }

    async fn complete(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
    ) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        ensure_pending_matches(&guard, reservation)?;
        guard.completed.insert(
            reservation.key.clone(),
            IdempotencyRecord {
                request_fingerprint: reservation.request_fingerprint.clone(),
                status_code,
                response_body,
            },
        );
        guard.pending.remove(&reservation.key);
        Ok(())
    }

    async fn abort(&self, reservation: &IdempotencyReservation) {
        let mut guard = self.inner.write().await;
        if guard
            .pending
            .get(&reservation.key)
            .is_some_and(|pending| pending == &reservation.request_fingerprint)
        {
            guard.pending.remove(&reservation.key);
        }
    }
}

#[derive(Debug)]
pub struct LocalIdempotencyStore {
    path: PathBuf,
    inner: RwLock<IdempotencyState>,
}

#[derive(Serialize, Deserialize)]
struct PersistedIdempotencyStore {
    version: u32,
    records: Vec<PersistedIdempotencyRecord>,
}

#[derive(Serialize, Deserialize)]
struct PersistedIdempotencyRecord {
    scope: String,
    key_hash: String,
    request_fingerprint: String,
    status_code: u16,
    response_body_json: Vec<u8>,
}

impl LocalIdempotencyStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, VfsError> {
        let path = path.as_ref().to_path_buf();
        let completed = match std::fs::read(&path) {
            Ok(bytes) => Self::decode(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => return Err(e.into()),
        };

        Ok(Self {
            path,
            inner: RwLock::new(IdempotencyState {
                completed,
                pending: BTreeMap::new(),
            }),
        })
    }

    fn decode(bytes: &[u8]) -> Result<BTreeMap<IdempotencyStoreKey, IdempotencyRecord>, VfsError> {
        let persisted: PersistedIdempotencyStore =
            bincode::deserialize(bytes).map_err(|e| VfsError::CorruptStore {
                message: format!("idempotency store decode failed: {e}"),
            })?;
        if persisted.version != IDEMPOTENCY_STORE_VERSION {
            return Err(VfsError::CorruptStore {
                message: format!(
                    "unsupported idempotency store version {}",
                    persisted.version
                ),
            });
        }

        let mut completed = BTreeMap::new();
        for record in persisted.records {
            let response_body =
                serde_json::from_slice(&record.response_body_json).map_err(|e| {
                    VfsError::CorruptStore {
                        message: format!("idempotency response JSON decode failed: {e}"),
                    }
                })?;
            let key = IdempotencyStoreKey {
                scope: record.scope,
                key_hash: record.key_hash,
            };
            if completed
                .insert(
                    key.clone(),
                    IdempotencyRecord {
                        request_fingerprint: record.request_fingerprint,
                        status_code: record.status_code,
                        response_body,
                    },
                )
                .is_some()
            {
                return Err(VfsError::CorruptStore {
                    message: format!(
                        "duplicate idempotency record for scope '{}' and key hash '{}'",
                        key.scope, key.key_hash
                    ),
                });
            }
        }
        Ok(completed)
    }

    fn encode(
        completed: &BTreeMap<IdempotencyStoreKey, IdempotencyRecord>,
    ) -> Result<Vec<u8>, VfsError> {
        let records = completed
            .iter()
            .map(|(key, record)| {
                let response_body_json =
                    serde_json::to_vec(&record.response_body).map_err(|e| {
                        VfsError::CorruptStore {
                            message: format!("idempotency response JSON encode failed: {e}"),
                        }
                    })?;
                Ok(PersistedIdempotencyRecord {
                    scope: key.scope.clone(),
                    key_hash: key.key_hash.clone(),
                    request_fingerprint: record.request_fingerprint.clone(),
                    status_code: record.status_code,
                    response_body_json,
                })
            })
            .collect::<Result<Vec<_>, VfsError>>()?;
        bincode::serialize(&PersistedIdempotencyStore {
            version: IDEMPOTENCY_STORE_VERSION,
            records,
        })
        .map_err(|e| VfsError::CorruptStore {
            message: format!("idempotency store encode failed: {e}"),
        })
    }

    fn persist_completed(
        &self,
        completed: &BTreeMap<IdempotencyStoreKey, IdempotencyRecord>,
    ) -> Result<(), VfsError> {
        let bytes = Self::encode(completed)?;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let tmp = self.path.with_extension(format!("tmp-{}", Uuid::new_v4()));
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }
}

#[async_trait]
impl IdempotencyStore for LocalIdempotencyStore {
    async fn begin(
        &self,
        scope: &str,
        key: &IdempotencyKey,
        request_fingerprint: &str,
    ) -> Result<IdempotencyBegin, VfsError> {
        let mut guard = self.inner.write().await;
        begin_locked(&mut guard, scope, key, request_fingerprint)
    }

    async fn complete(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
    ) -> Result<(), VfsError> {
        let mut guard = self.inner.write().await;
        ensure_pending_matches(&guard, reservation)?;

        let mut next_completed = guard.completed.clone();
        next_completed.insert(
            reservation.key.clone(),
            IdempotencyRecord {
                request_fingerprint: reservation.request_fingerprint.clone(),
                status_code,
                response_body,
            },
        );
        self.persist_completed(&next_completed)?;
        guard.completed = next_completed;
        guard.pending.remove(&reservation.key);
        Ok(())
    }

    async fn abort(&self, reservation: &IdempotencyReservation) {
        let mut guard = self.inner.write().await;
        if guard
            .pending
            .get(&reservation.key)
            .is_some_and(|pending| pending == &reservation.request_fingerprint)
        {
            guard.pending.remove(&reservation.key);
        }
    }
}

pub fn request_fingerprint<T: Serialize>(scope: &str, body: &T) -> Result<String, VfsError> {
    let body = serde_json::to_vec(body).map_err(|e| VfsError::InvalidArgs {
        message: format!("request fingerprint serialization failed: {e}"),
    })?;
    let mut hasher = Sha256::new();
    hasher.update((scope.len() as u64).to_be_bytes());
    hasher.update(scope.as_bytes());
    hasher.update((body.len() as u64).to_be_bytes());
    hasher.update(&body);
    Ok(format!("{:x}", hasher.finalize()))
}

fn begin_locked(
    state: &mut IdempotencyState,
    scope: &str,
    key: &IdempotencyKey,
    request_fingerprint: &str,
) -> Result<IdempotencyBegin, VfsError> {
    let store_key = IdempotencyStoreKey::new(scope, key);
    if let Some(record) = state.completed.get(&store_key) {
        if record.request_fingerprint == request_fingerprint {
            return Ok(IdempotencyBegin::Replay(record.clone()));
        }
        return Ok(IdempotencyBegin::Conflict);
    }

    if let Some(pending_fingerprint) = state.pending.get(&store_key) {
        if pending_fingerprint == request_fingerprint {
            return Ok(IdempotencyBegin::InProgress);
        }
        return Ok(IdempotencyBegin::Conflict);
    }

    state
        .pending
        .insert(store_key.clone(), request_fingerprint.to_string());
    Ok(IdempotencyBegin::Execute(IdempotencyReservation {
        key: store_key,
        request_fingerprint: request_fingerprint.to_string(),
    }))
}

fn ensure_pending_matches(
    state: &IdempotencyState,
    reservation: &IdempotencyReservation,
) -> Result<(), VfsError> {
    match state.pending.get(&reservation.key) {
        Some(pending) if pending == &reservation.request_fingerprint => Ok(()),
        _ => Err(VfsError::InvalidArgs {
            message: "idempotency reservation is not pending".to_string(),
        }),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use serde_json::json;
    use std::fs;
    use std::path::PathBuf;

    fn temp_idempotency_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "stratum_idempotency_{}_{}_{}.bin",
            name,
            std::process::id(),
            uuid::Uuid::new_v4()
        ))
    }

    #[test]
    fn key_validation_accepts_only_non_empty_visible_ascii_up_to_255_bytes() {
        IdempotencyKey::parse_header_value(&HeaderValue::from_static("abc-XYZ_123:./~"))
            .expect("visible ASCII key should be accepted");

        assert!(IdempotencyKey::parse_header_value(&HeaderValue::from_static("")).is_err());
        assert!(
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("has space")).is_err()
        );
        assert!(
            IdempotencyKey::parse_header_value(&HeaderValue::from_bytes(&vec![b'a'; 256]).unwrap())
                .is_err()
        );
    }

    #[tokio::test]
    async fn same_key_replays_completed_response() {
        let store = InMemoryIdempotencyStore::new();
        let key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("run-create-1")).unwrap();

        let reservation = match store.begin("runs:create", &key, "request-a").await.unwrap() {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected execute, got {other:?}"),
        };
        store
            .complete(&reservation, 201, json!({"run_id": "run_123"}))
            .await
            .unwrap();

        let replay = match store.begin("runs:create", &key, "request-a").await.unwrap() {
            IdempotencyBegin::Replay(record) => record,
            other => panic!("expected replay, got {other:?}"),
        };
        assert_eq!(replay.status_code, 201);
        assert_eq!(replay.response_body, json!({"run_id": "run_123"}));
    }

    #[tokio::test]
    async fn same_key_with_different_request_conflicts() {
        let store = InMemoryIdempotencyStore::new();
        let key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("run-create-conflict"))
                .unwrap();

        let reservation = match store.begin("runs:create", &key, "request-a").await.unwrap() {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected execute, got {other:?}"),
        };
        store
            .complete(&reservation, 201, json!({"run_id": "run_123"}))
            .await
            .unwrap();

        assert!(matches!(
            store.begin("runs:create", &key, "request-b").await.unwrap(),
            IdempotencyBegin::Conflict
        ));
    }

    #[tokio::test]
    async fn in_progress_key_conflicts_before_completion() {
        let store = InMemoryIdempotencyStore::new();
        let key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("run-create-pending"))
                .unwrap();

        let reservation = match store.begin("runs:create", &key, "request-a").await.unwrap() {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected execute, got {other:?}"),
        };

        assert!(matches!(
            store.begin("runs:create", &key, "request-a").await.unwrap(),
            IdempotencyBegin::InProgress
        ));
        assert!(matches!(
            store.begin("runs:create", &key, "request-b").await.unwrap(),
            IdempotencyBegin::Conflict
        ));

        store.abort(&reservation).await;
        assert!(matches!(
            store.begin("runs:create", &key, "request-a").await.unwrap(),
            IdempotencyBegin::Execute(_)
        ));
    }

    #[tokio::test]
    async fn durable_store_reloads_completed_response_without_raw_key() {
        let path = temp_idempotency_path("reload");
        let raw_key = "run-create-durable";
        let key = IdempotencyKey::parse_header_value(&HeaderValue::from_static(raw_key)).unwrap();
        let store = LocalIdempotencyStore::open(&path).unwrap();

        let reservation = match store.begin("runs:create", &key, "request-a").await.unwrap() {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected execute, got {other:?}"),
        };
        store
            .complete(&reservation, 201, json!({"run_id": "run_123"}))
            .await
            .unwrap();

        let bytes = fs::read(&path).unwrap();
        let text = String::from_utf8_lossy(&bytes);
        assert!(!text.contains(raw_key));

        let reloaded = LocalIdempotencyStore::open(&path).unwrap();
        let replay = match reloaded
            .begin("runs:create", &key, "request-a")
            .await
            .unwrap()
        {
            IdempotencyBegin::Replay(record) => record,
            other => panic!("expected replay after reload, got {other:?}"),
        };
        assert_eq!(replay.status_code, 201);
        assert_eq!(replay.response_body, json!({"run_id": "run_123"}));
    }

    #[test]
    fn corrupt_store_bytes_return_corrupt_store() {
        let path = temp_idempotency_path("corrupt");
        fs::write(&path, b"not-idempotency").unwrap();

        let err = match LocalIdempotencyStore::open(&path) {
            Ok(_) => panic!("corrupt store should fail"),
            Err(err) => err,
        };
        assert!(matches!(err, crate::error::VfsError::CorruptStore { .. }));
    }

    #[tokio::test]
    async fn failed_persist_does_not_publish_completed_record() {
        let path = temp_idempotency_path("failed-persist");
        let store = LocalIdempotencyStore::open(&path).unwrap();
        fs::create_dir_all(&path).unwrap();
        let key = IdempotencyKey::parse_header_value(&HeaderValue::from_static("run-create-fail"))
            .unwrap();

        let reservation = match store.begin("runs:create", &key, "request-a").await.unwrap() {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected execute, got {other:?}"),
        };
        let err = store
            .complete(&reservation, 201, json!({"run_id": "run_123"}))
            .await
            .expect_err("rename over directory should fail");
        assert!(matches!(err, crate::error::VfsError::IoError(_)));

        assert!(matches!(
            store.begin("runs:create", &key, "request-a").await.unwrap(),
            IdempotencyBegin::InProgress
        ));
        store.abort(&reservation).await;
        assert!(matches!(
            store.begin("runs:create", &key, "request-a").await.unwrap(),
            IdempotencyBegin::Execute(_)
        ));
    }

    #[test]
    fn request_fingerprint_is_stable_for_serializable_bodies() {
        let a = request_fingerprint("runs:create", &json!({"run_id": "run_123"})).unwrap();
        let b = request_fingerprint("runs:create", &json!({"run_id": "run_123"})).unwrap();
        let c = request_fingerprint("runs:create", &json!({"run_id": "run_456"})).unwrap();

        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
