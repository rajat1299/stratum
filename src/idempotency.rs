use async_trait::async_trait;
use axum::http::HeaderValue;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::error::VfsError;

const IDEMPOTENCY_STORE_VERSION: u32 = 2;
const IDEMPOTENCY_STORE_V1_VERSION: u32 = 1;
const IDEMPOTENCY_RETAINED_COMMIT_JSON_DEPTH_LIMIT: usize = 16;
const IDEMPOTENCY_RETAINED_COMMIT_JSON_NODE_LIMIT: usize = 512;
#[allow(dead_code)]
const MAX_IDEMPOTENCY_STORE_PART_BYTES: usize = 255;

pub type SharedIdempotencyStore = Arc<dyn IdempotencyStore>;

#[derive(Clone, PartialEq, Eq)]
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

impl fmt::Debug for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdempotencyKey")
            .field("has_key_hash", &true)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

impl fmt::Debug for IdempotencyStoreKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdempotencyStoreKey")
            .field("scope", &self.scope)
            .field("has_key_hash", &true)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyReplayClassification {
    SecretFree,
    Partial,
    SecretBearing,
}

#[derive(Clone, PartialEq, Eq)]
pub struct IdempotencyRetentionPolicy {
    pub completed_ttl_seconds: u64,
    pub pending_stale_after_seconds: u64,
    pub max_records_per_scope: Option<usize>,
    pub max_records_per_repo: Option<usize>,
    pub max_records_per_workspace: Option<usize>,
    pub max_records_per_principal: Option<usize>,
}

impl IdempotencyRetentionPolicy {
    pub fn unlimited() -> Self {
        Self {
            completed_ttl_seconds: u64::MAX,
            pending_stale_after_seconds: u64::MAX,
            max_records_per_scope: None,
            max_records_per_repo: None,
            max_records_per_workspace: None,
            max_records_per_principal: None,
        }
    }
}

impl fmt::Debug for IdempotencyRetentionPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdempotencyRetentionPolicy")
            .field("completed_ttl_seconds", &self.completed_ttl_seconds)
            .field(
                "pending_stale_after_seconds",
                &self.pending_stale_after_seconds,
            )
            .field("max_records_per_scope", &self.max_records_per_scope)
            .field("max_records_per_repo", &self.max_records_per_repo)
            .field("max_records_per_workspace", &self.max_records_per_workspace)
            .field("max_records_per_principal", &self.max_records_per_principal)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyQuotaIdentity {
    pub scope: String,
    pub repo_id: Option<String>,
    pub workspace_id: Option<String>,
    pub principal_uid: Option<u64>,
}

impl IdempotencyQuotaIdentity {
    pub fn for_scope(scope: &str) -> Self {
        Self {
            scope: scope.to_string(),
            repo_id: parse_scope_component(scope, "repo"),
            workspace_id: parse_scope_component(scope, "workspace"),
            principal_uid: None,
        }
    }
}

impl fmt::Debug for IdempotencyQuotaIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdempotencyQuotaIdentity")
            .field("has_scope", &true)
            .field("has_repo_id", &self.repo_id.is_some())
            .field("has_workspace_id", &self.workspace_id.is_some())
            .field("has_principal_uid", &self.principal_uid.is_some())
            .finish()
    }
}

#[derive(Clone)]
pub struct IdempotencySweepRequest {
    pub now_unix_seconds: u64,
    pub limit: usize,
    pub policy: IdempotencyRetentionPolicy,
    pub repo_id: Option<crate::backend::RepoId>,
    pub retain_keys: Vec<(String, String)>,
    pub retain_commit_ids: Vec<String>,
    pub abort_stale_pending: bool,
}

impl fmt::Debug for IdempotencySweepRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdempotencySweepRequest")
            .field("now_unix_seconds", &self.now_unix_seconds)
            .field("limit", &self.limit)
            .field("has_repo_id", &self.repo_id.is_some())
            .field("retain_keys_count", &self.retain_keys.len())
            .field("retain_commit_ids_count", &self.retain_commit_ids.len())
            .field("abort_stale_pending", &self.abort_stale_pending)
            .finish()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IdempotencySweepSummary {
    pub scanned: usize,
    pub swept_completed: usize,
    pub aborted_pending: usize,
    pub retained_for_roots: usize,
    pub quota_blocked: usize,
    pub stale_pending: usize,
    pub remaining: usize,
    pub redacted_reasons: BTreeMap<String, usize>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdempotencyRetainedKey {
    pub scope: String,
    pub key_hash: String,
}

impl fmt::Debug for IdempotencyRetainedKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdempotencyRetainedKey")
            .field("scope", &self.scope)
            .field("has_key_hash", &true)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    request_fingerprint: String,
    pub status_code: u16,
    pub response_body: serde_json::Value,
    pub completed_at_unix_seconds: u64,
    pub classification: IdempotencyReplayClassification,
    quota_identity: IdempotencyQuotaIdentity,
}

impl IdempotencyRecord {
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub(crate) fn for_store(
        request_fingerprint: impl Into<String>,
        status_code: u16,
        response_body: serde_json::Value,
    ) -> Self {
        Self::for_store_with_policy(
            request_fingerprint,
            status_code,
            response_body,
            IdempotencyReplayClassification::SecretFree,
            now_unix_seconds(),
            IdempotencyQuotaIdentity::for_scope(""),
        )
    }

    fn for_store_with_policy(
        request_fingerprint: impl Into<String>,
        status_code: u16,
        response_body: serde_json::Value,
        classification: IdempotencyReplayClassification,
        completed_at_unix_seconds: u64,
        quota_identity: IdempotencyQuotaIdentity,
    ) -> Self {
        Self {
            request_fingerprint: request_fingerprint.into(),
            status_code,
            response_body,
            completed_at_unix_seconds,
            classification,
            quota_identity,
        }
    }
}

impl fmt::Debug for IdempotencyRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdempotencyRecord")
            .field("status_code", &self.status_code)
            .field("has_response_body", &true)
            .field("completed_at_unix_seconds", &self.completed_at_unix_seconds)
            .field("classification", &self.classification)
            .field("quota_identity", &self.quota_identity)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct IdempotencyReservation {
    key: IdempotencyStoreKey,
    request_fingerprint: String,
    reservation_token: String,
}

impl IdempotencyReservation {
    pub(crate) fn for_store(scope: &str, key: &IdempotencyKey, request_fingerprint: &str) -> Self {
        Self::for_store_with_token(scope, key, request_fingerprint, Uuid::new_v4().to_string())
    }

    pub(crate) fn for_store_with_token(
        scope: &str,
        key: &IdempotencyKey,
        request_fingerprint: &str,
        reservation_token: impl Into<String>,
    ) -> Self {
        Self {
            key: IdempotencyStoreKey::new(scope, key),
            request_fingerprint: request_fingerprint.to_string(),
            reservation_token: reservation_token.into(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn for_store_parts(
        scope: impl Into<String>,
        key_hash: impl Into<String>,
        request_fingerprint: impl Into<String>,
        reservation_token: impl Into<String>,
    ) -> Result<Self, VfsError> {
        let scope = validate_store_part(scope.into(), "idempotency scope")?;
        let key_hash = validate_hex_store_part(key_hash.into(), "idempotency key hash")?;
        let request_fingerprint = validate_hex_store_part(
            request_fingerprint.into(),
            "idempotency request fingerprint",
        )?;
        let reservation_token =
            validate_store_part(reservation_token.into(), "idempotency reservation token")?;
        Ok(Self {
            key: IdempotencyStoreKey { scope, key_hash },
            request_fingerprint,
            reservation_token,
        })
    }

    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub(crate) fn scope(&self) -> &str {
        &self.key.scope
    }

    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub(crate) fn key_hash(&self) -> &str {
        &self.key.key_hash
    }

    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub(crate) fn request_fingerprint(&self) -> &str {
        &self.request_fingerprint
    }

    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub(crate) fn reservation_token(&self) -> &str {
        &self.reservation_token
    }
}

impl fmt::Debug for IdempotencyReservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdempotencyReservation")
            .field("scope", &self.key.scope)
            .field("has_key_hash", &true)
            .field("has_request_fingerprint", &true)
            .field("has_reservation_token", &true)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum IdempotencyBegin {
    Execute(IdempotencyReservation),
    Replay(IdempotencyRecord),
    Conflict,
    InProgress,
}

impl fmt::Debug for IdempotencyBegin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Execute(reservation) => f.debug_tuple("Execute").field(reservation).finish(),
            Self::Replay(record) => f.debug_tuple("Replay").field(record).finish(),
            Self::Conflict => f.write_str("Conflict"),
            Self::InProgress => f.write_str("InProgress"),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RetainedIdempotencyRecord {
    scope: String,
    pub status_code: Option<u16>,
    pub(crate) commit_roots: Vec<String>,
    pub(crate) commit_roots_truncated: bool,
    pub pending: bool,
    pub classification: Option<IdempotencyReplayClassification>,
    pub reserved_at_unix_seconds: Option<u64>,
    pub completed_at_unix_seconds: Option<u64>,
}

impl RetainedIdempotencyRecord {
    pub fn scope(&self) -> &str {
        &self.scope
    }

    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub(crate) fn completed_response(
        scope: String,
        status_code: u16,
        response_body: serde_json::Value,
    ) -> Self {
        let commit_roots = collect_commit_root_hexes(&response_body);
        Self {
            scope,
            status_code: Some(status_code),
            commit_roots: commit_roots.roots,
            commit_roots_truncated: commit_roots.truncated,
            pending: false,
            classification: Some(IdempotencyReplayClassification::SecretFree),
            reserved_at_unix_seconds: None,
            completed_at_unix_seconds: None,
        }
    }

    fn completed_record(scope: String, record: &IdempotencyRecord) -> Self {
        let commit_roots = collect_commit_root_hexes(&record.response_body);
        Self {
            scope,
            status_code: Some(record.status_code),
            commit_roots: commit_roots.roots,
            commit_roots_truncated: commit_roots.truncated,
            pending: false,
            classification: Some(record.classification.clone()),
            reserved_at_unix_seconds: None,
            completed_at_unix_seconds: Some(record.completed_at_unix_seconds),
        }
    }

    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub(crate) fn pending(scope: String) -> Self {
        Self {
            scope,
            status_code: None,
            commit_roots: Vec::new(),
            commit_roots_truncated: false,
            pending: true,
            classification: None,
            reserved_at_unix_seconds: None,
            completed_at_unix_seconds: None,
        }
    }

    fn pending_record(scope: String, record: &PendingIdempotencyReservation) -> Self {
        Self {
            scope,
            status_code: None,
            commit_roots: Vec::new(),
            commit_roots_truncated: false,
            pending: true,
            classification: None,
            reserved_at_unix_seconds: Some(record.reserved_at_unix_seconds),
            completed_at_unix_seconds: None,
        }
    }
}

impl fmt::Debug for RetainedIdempotencyRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedIdempotencyRecord")
            .field("scope", &self.scope)
            .field("status_code", &self.status_code)
            .field("commit_root_count", &self.commit_roots.len())
            .field("commit_roots_truncated", &self.commit_roots_truncated)
            .field("pending", &self.pending)
            .field("classification", &self.classification)
            .field("reserved_at_unix_seconds", &self.reserved_at_unix_seconds)
            .field("completed_at_unix_seconds", &self.completed_at_unix_seconds)
            .finish()
    }
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

    async fn begin_with_policy(
        &self,
        scope: &str,
        key: &IdempotencyKey,
        request_fingerprint: &str,
        _quota_identity: IdempotencyQuotaIdentity,
        policy: &IdempotencyRetentionPolicy,
    ) -> Result<IdempotencyBegin, VfsError> {
        if policy == &IdempotencyRetentionPolicy::unlimited() {
            return self.begin(scope, key, request_fingerprint).await;
        }
        Err(idempotency_policy_not_supported())
    }

    async fn complete_with_classification(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
        classification: IdempotencyReplayClassification,
    ) -> Result<(), VfsError> {
        match classification {
            IdempotencyReplayClassification::SecretFree => {
                self.complete(reservation, status_code, response_body).await
            }
            IdempotencyReplayClassification::Partial => Err(idempotency_policy_not_supported()),
            IdempotencyReplayClassification::SecretBearing => {
                reject_secret_bearing_replay(&classification)
            }
        }
    }

    async fn complete_or_match(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
    ) -> Result<(), VfsError> {
        self.complete(reservation, status_code, response_body).await
    }

    async fn complete_or_match_with_classification(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
        classification: IdempotencyReplayClassification,
    ) -> Result<(), VfsError> {
        match classification {
            IdempotencyReplayClassification::SecretFree => {
                self.complete_or_match(reservation, status_code, response_body)
                    .await
            }
            IdempotencyReplayClassification::Partial => Err(idempotency_policy_not_supported()),
            IdempotencyReplayClassification::SecretBearing => {
                reject_secret_bearing_replay(&classification)
            }
        }
    }

    async fn abort(&self, reservation: &IdempotencyReservation);

    async fn sweep_retention(
        &self,
        _request: IdempotencySweepRequest,
    ) -> Result<IdempotencySweepSummary, VfsError> {
        Err(VfsError::NotSupported {
            message: "idempotency retention sweep is not supported by this store".to_string(),
        })
    }

    async fn list_retained_for_repo(
        &self,
        _repo_id: &crate::backend::RepoId,
        _limit: usize,
    ) -> Result<Vec<RetainedIdempotencyRecord>, VfsError> {
        Err(VfsError::NotSupported {
            message: "idempotency retained record listing is not supported by this store"
                .to_string(),
        })
    }
}

#[derive(Debug, Clone, Default)]
struct IdempotencyState {
    completed: BTreeMap<IdempotencyStoreKey, IdempotencyRecord>,
    pending: BTreeMap<IdempotencyStoreKey, PendingIdempotencyReservation>,
}

#[derive(Clone, PartialEq, Eq)]
struct PendingIdempotencyReservation {
    request_fingerprint: String,
    reservation_token: String,
    reserved_at_unix_seconds: u64,
    quota_identity: IdempotencyQuotaIdentity,
}

impl fmt::Debug for PendingIdempotencyReservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingIdempotencyReservation")
            .field("has_request_fingerprint", &true)
            .field("has_reservation_token", &true)
            .field("reserved_at_unix_seconds", &self.reserved_at_unix_seconds)
            .field("quota_identity", &self.quota_identity)
            .finish()
    }
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
        self.begin_with_policy(
            scope,
            key,
            request_fingerprint,
            IdempotencyQuotaIdentity::for_scope(scope),
            &IdempotencyRetentionPolicy::unlimited(),
        )
        .await
    }

    async fn begin_with_policy(
        &self,
        scope: &str,
        key: &IdempotencyKey,
        request_fingerprint: &str,
        quota_identity: IdempotencyQuotaIdentity,
        policy: &IdempotencyRetentionPolicy,
    ) -> Result<IdempotencyBegin, VfsError> {
        let mut guard = self.inner.write().await;
        begin_locked(
            &mut guard,
            scope,
            key,
            request_fingerprint,
            quota_identity,
            policy,
            now_unix_seconds(),
        )
    }

    async fn complete(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
    ) -> Result<(), VfsError> {
        self.complete_with_classification(
            reservation,
            status_code,
            response_body,
            IdempotencyReplayClassification::SecretFree,
        )
        .await
    }

    async fn complete_with_classification(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
        classification: IdempotencyReplayClassification,
    ) -> Result<(), VfsError> {
        reject_secret_bearing_replay(&classification)?;
        let mut guard = self.inner.write().await;
        complete_locked(
            &mut guard,
            reservation,
            status_code,
            response_body,
            classification,
            now_unix_seconds(),
        )
    }

    async fn complete_or_match(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
    ) -> Result<(), VfsError> {
        self.complete_or_match_with_classification(
            reservation,
            status_code,
            response_body,
            IdempotencyReplayClassification::SecretFree,
        )
        .await
    }

    async fn complete_or_match_with_classification(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
        classification: IdempotencyReplayClassification,
    ) -> Result<(), VfsError> {
        reject_secret_bearing_replay(&classification)?;
        let mut guard = self.inner.write().await;
        complete_or_match_locked(
            &mut guard,
            reservation,
            status_code,
            response_body,
            classification,
            now_unix_seconds(),
        )
    }

    async fn abort(&self, reservation: &IdempotencyReservation) {
        let mut guard = self.inner.write().await;
        if guard
            .pending
            .get(&reservation.key)
            .is_some_and(|pending| pending.matches(reservation))
        {
            guard.pending.remove(&reservation.key);
        }
    }

    async fn list_retained_for_repo(
        &self,
        repo_id: &crate::backend::RepoId,
        limit: usize,
    ) -> Result<Vec<RetainedIdempotencyRecord>, VfsError> {
        let guard = self.inner.read().await;
        list_retained_for_repo_locked(&guard, repo_id, limit)
    }

    async fn sweep_retention(
        &self,
        request: IdempotencySweepRequest,
    ) -> Result<IdempotencySweepSummary, VfsError> {
        let mut guard = self.inner.write().await;
        Ok(sweep_retention_locked(&mut guard, &request))
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

#[derive(Clone, Serialize, Deserialize)]
struct PersistedIdempotencyRecord {
    scope: String,
    key_hash: String,
    request_fingerprint: String,
    status_code: u16,
    response_body_json: Vec<u8>,
    completed_at_unix_seconds: Option<u64>,
    replay_classification: Option<IdempotencyReplayClassification>,
    quota_identity: Option<IdempotencyQuotaIdentity>,
}

#[derive(Serialize, Deserialize)]
struct PersistedIdempotencyStoreV1 {
    version: u32,
    records: Vec<PersistedIdempotencyRecordV1>,
}

#[derive(Serialize, Deserialize)]
struct PersistedIdempotencyRecordV1 {
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
        let version = persisted_store_version(bytes)?;
        if version == IDEMPOTENCY_STORE_V1_VERSION {
            return Self::decode_v1(bytes);
        }
        let persisted: PersistedIdempotencyStore =
            crate::codec::deserialize(bytes).map_err(|e| VfsError::CorruptStore {
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
            let classification = record
                .replay_classification
                .unwrap_or(IdempotencyReplayClassification::SecretFree);
            reject_secret_bearing_replay(&classification).map_err(|_| VfsError::CorruptStore {
                message: "idempotency store contains non-replayable response".to_string(),
            })?;
            if completed
                .insert(
                    key.clone(),
                    IdempotencyRecord::for_store_with_policy(
                        record.request_fingerprint,
                        record.status_code,
                        response_body,
                        classification,
                        record
                            .completed_at_unix_seconds
                            .unwrap_or_else(now_unix_seconds),
                        record
                            .quota_identity
                            .unwrap_or_else(|| IdempotencyQuotaIdentity::for_scope(&key.scope)),
                    ),
                )
                .is_some()
            {
                return Err(VfsError::CorruptStore {
                    message: "duplicate idempotency record".to_string(),
                });
            }
        }
        Ok(completed)
    }

    fn decode_v1(
        bytes: &[u8],
    ) -> Result<BTreeMap<IdempotencyStoreKey, IdempotencyRecord>, VfsError> {
        let persisted: PersistedIdempotencyStoreV1 =
            crate::codec::deserialize(bytes).map_err(|e| VfsError::CorruptStore {
                message: format!("idempotency v1 store decode failed: {e}"),
            })?;
        let migration_timestamp = now_unix_seconds();
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
                    IdempotencyRecord::for_store_with_policy(
                        record.request_fingerprint,
                        record.status_code,
                        response_body,
                        IdempotencyReplayClassification::SecretFree,
                        migration_timestamp,
                        IdempotencyQuotaIdentity::for_scope(&key.scope),
                    ),
                )
                .is_some()
            {
                return Err(VfsError::CorruptStore {
                    message: "duplicate idempotency record".to_string(),
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
                    completed_at_unix_seconds: Some(record.completed_at_unix_seconds),
                    replay_classification: Some(record.classification.clone()),
                    quota_identity: Some(record.quota_identity.clone()),
                })
            })
            .collect::<Result<Vec<_>, VfsError>>()?;
        crate::codec::serialize(&PersistedIdempotencyStore {
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
        if let Some(parent) = self.path.parent()
            && let Ok(dir) = std::fs::File::open(parent)
        {
            let _ = dir.sync_all();
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
        self.begin_with_policy(
            scope,
            key,
            request_fingerprint,
            IdempotencyQuotaIdentity::for_scope(scope),
            &IdempotencyRetentionPolicy::unlimited(),
        )
        .await
    }

    async fn begin_with_policy(
        &self,
        scope: &str,
        key: &IdempotencyKey,
        request_fingerprint: &str,
        quota_identity: IdempotencyQuotaIdentity,
        policy: &IdempotencyRetentionPolicy,
    ) -> Result<IdempotencyBegin, VfsError> {
        let mut guard = self.inner.write().await;
        begin_locked(
            &mut guard,
            scope,
            key,
            request_fingerprint,
            quota_identity,
            policy,
            now_unix_seconds(),
        )
    }

    async fn complete(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
    ) -> Result<(), VfsError> {
        self.complete_with_classification(
            reservation,
            status_code,
            response_body,
            IdempotencyReplayClassification::SecretFree,
        )
        .await
    }

    async fn complete_with_classification(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
        classification: IdempotencyReplayClassification,
    ) -> Result<(), VfsError> {
        reject_secret_bearing_replay(&classification)?;
        let mut guard = self.inner.write().await;
        ensure_pending_matches(&guard, reservation)?;

        let mut next_completed = guard.completed.clone();
        let quota_identity = guard
            .pending
            .get(&reservation.key)
            .map(|pending| pending.quota_identity.clone())
            .unwrap_or_else(|| IdempotencyQuotaIdentity::for_scope(reservation.scope()));
        next_completed.insert(
            reservation.key.clone(),
            IdempotencyRecord::for_store_with_policy(
                reservation.request_fingerprint.clone(),
                status_code,
                response_body,
                classification,
                now_unix_seconds(),
                quota_identity,
            ),
        );
        self.persist_completed(&next_completed)?;
        guard.completed = next_completed;
        guard.pending.remove(&reservation.key);
        Ok(())
    }

    async fn complete_or_match(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
    ) -> Result<(), VfsError> {
        self.complete_or_match_with_classification(
            reservation,
            status_code,
            response_body,
            IdempotencyReplayClassification::SecretFree,
        )
        .await
    }

    async fn complete_or_match_with_classification(
        &self,
        reservation: &IdempotencyReservation,
        status_code: u16,
        response_body: serde_json::Value,
        classification: IdempotencyReplayClassification,
    ) -> Result<(), VfsError> {
        reject_secret_bearing_replay(&classification)?;
        let mut guard = self.inner.write().await;

        if let Some(pending) = guard.pending.get(&reservation.key) {
            if !pending.matches(reservation) {
                return Err(idempotency_reservation_not_pending());
            }

            let mut next_completed = guard.completed.clone();
            next_completed.insert(
                reservation.key.clone(),
                IdempotencyRecord::for_store_with_policy(
                    reservation.request_fingerprint.clone(),
                    status_code,
                    response_body,
                    classification,
                    now_unix_seconds(),
                    pending.quota_identity.clone(),
                ),
            );
            self.persist_completed(&next_completed)?;
            guard.completed = next_completed;
            guard.pending.remove(&reservation.key);
            return Ok(());
        }

        match guard.completed.get(&reservation.key) {
            Some(record)
                if completed_record_matches(
                    record,
                    reservation,
                    status_code,
                    &response_body,
                    &classification,
                ) =>
            {
                Ok(())
            }
            Some(_) => Err(idempotency_completed_replay_mismatch()),
            None => Err(idempotency_reservation_not_pending()),
        }
    }

    async fn abort(&self, reservation: &IdempotencyReservation) {
        let mut guard = self.inner.write().await;
        if guard
            .pending
            .get(&reservation.key)
            .is_some_and(|pending| pending.matches(reservation))
        {
            guard.pending.remove(&reservation.key);
        }
    }

    async fn list_retained_for_repo(
        &self,
        repo_id: &crate::backend::RepoId,
        limit: usize,
    ) -> Result<Vec<RetainedIdempotencyRecord>, VfsError> {
        let guard = self.inner.read().await;
        list_retained_for_repo_locked(&guard, repo_id, limit)
    }

    async fn sweep_retention(
        &self,
        request: IdempotencySweepRequest,
    ) -> Result<IdempotencySweepSummary, VfsError> {
        let mut guard = self.inner.write().await;
        let mut next = guard.clone();
        let summary = sweep_retention_locked(&mut next, &request);
        self.persist_completed(&next.completed)?;
        *guard = next;
        Ok(summary)
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
    mut quota_identity: IdempotencyQuotaIdentity,
    policy: &IdempotencyRetentionPolicy,
    now_unix_seconds: u64,
) -> Result<IdempotencyBegin, VfsError> {
    normalize_quota_identity(&mut quota_identity, scope);
    let store_key = IdempotencyStoreKey::new(scope, key);
    if let Some(record) = state.completed.get(&store_key) {
        if record.request_fingerprint == request_fingerprint {
            reject_secret_bearing_replay(&record.classification)?;
            return Ok(IdempotencyBegin::Replay(record.clone()));
        }
        return Ok(IdempotencyBegin::Conflict);
    }

    if let Some(pending) = state.pending.get(&store_key).cloned() {
        let is_stale = now_unix_seconds.saturating_sub(pending.reserved_at_unix_seconds)
            > policy.pending_stale_after_seconds;
        if !is_stale {
            if pending.request_fingerprint == request_fingerprint {
                return Ok(IdempotencyBegin::InProgress);
            }
            return Ok(IdempotencyBegin::Conflict);
        }

        if pending.request_fingerprint != request_fingerprint {
            state.pending.remove(&store_key);
            return Ok(IdempotencyBegin::Conflict);
        }

        enforce_idempotency_quota(state, &store_key, &quota_identity, policy)?;
        let reservation = IdempotencyReservation::for_store(scope, key, request_fingerprint);
        state.pending.insert(
            store_key,
            PendingIdempotencyReservation {
                request_fingerprint: reservation.request_fingerprint.clone(),
                reservation_token: reservation.reservation_token.clone(),
                reserved_at_unix_seconds: now_unix_seconds,
                quota_identity,
            },
        );
        return Ok(IdempotencyBegin::Execute(reservation));
    }

    enforce_idempotency_quota(state, &store_key, &quota_identity, policy)?;
    let reservation = IdempotencyReservation::for_store(scope, key, request_fingerprint);
    state.pending.insert(
        store_key,
        PendingIdempotencyReservation {
            request_fingerprint: reservation.request_fingerprint.clone(),
            reservation_token: reservation.reservation_token.clone(),
            reserved_at_unix_seconds: now_unix_seconds,
            quota_identity,
        },
    );
    Ok(IdempotencyBegin::Execute(reservation))
}

fn complete_locked(
    state: &mut IdempotencyState,
    reservation: &IdempotencyReservation,
    status_code: u16,
    response_body: serde_json::Value,
    classification: IdempotencyReplayClassification,
    completed_at_unix_seconds: u64,
) -> Result<(), VfsError> {
    ensure_pending_matches(state, reservation)?;
    let quota_identity = state
        .pending
        .get(&reservation.key)
        .map(|pending| pending.quota_identity.clone())
        .unwrap_or_else(|| IdempotencyQuotaIdentity::for_scope(reservation.scope()));
    state.completed.insert(
        reservation.key.clone(),
        IdempotencyRecord::for_store_with_policy(
            reservation.request_fingerprint.clone(),
            status_code,
            response_body,
            classification,
            completed_at_unix_seconds,
            quota_identity,
        ),
    );
    state.pending.remove(&reservation.key);
    Ok(())
}

fn list_retained_for_repo_locked(
    state: &IdempotencyState,
    repo_id: &crate::backend::RepoId,
    limit: usize,
) -> Result<Vec<RetainedIdempotencyRecord>, VfsError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let repo_prefix = format!("repo:{}:", repo_id.as_str());
    let mut retained = Vec::new();
    for key in matching_idempotency_keys(&state.pending, Some(repo_id), Some(&repo_prefix), limit) {
        let pending = state
            .pending
            .get(&key)
            .expect("pending key disappeared during retained listing");
        retained.push(RetainedIdempotencyRecord::pending_record(
            key.scope.clone(),
            pending,
        ));
        if retained.len() == limit {
            return Ok(retained);
        }
    }
    let remaining = limit.saturating_sub(retained.len());
    for key in matching_idempotency_keys(
        &state.completed,
        Some(repo_id),
        Some(&repo_prefix),
        remaining,
    ) {
        let record = state
            .completed
            .get(&key)
            .expect("completed key disappeared during retained listing");
        retained.push(RetainedIdempotencyRecord::completed_record(
            key.scope.clone(),
            record,
        ));
        if retained.len() == limit {
            return Ok(retained);
        }
    }
    Ok(retained)
}

fn ensure_pending_matches(
    state: &IdempotencyState,
    reservation: &IdempotencyReservation,
) -> Result<(), VfsError> {
    match state.pending.get(&reservation.key) {
        Some(pending) if pending.matches(reservation) => Ok(()),
        _ => Err(idempotency_reservation_not_pending()),
    }
}

fn complete_or_match_locked(
    state: &mut IdempotencyState,
    reservation: &IdempotencyReservation,
    status_code: u16,
    response_body: serde_json::Value,
    classification: IdempotencyReplayClassification,
    completed_at_unix_seconds: u64,
) -> Result<(), VfsError> {
    if let Some(pending) = state.pending.get(&reservation.key) {
        if !pending.matches(reservation) {
            return Err(idempotency_reservation_not_pending());
        }
        state.completed.insert(
            reservation.key.clone(),
            IdempotencyRecord::for_store_with_policy(
                reservation.request_fingerprint.clone(),
                status_code,
                response_body,
                classification,
                completed_at_unix_seconds,
                pending.quota_identity.clone(),
            ),
        );
        state.pending.remove(&reservation.key);
        return Ok(());
    }

    match state.completed.get(&reservation.key) {
        Some(record)
            if completed_record_matches(
                record,
                reservation,
                status_code,
                &response_body,
                &classification,
            ) =>
        {
            Ok(())
        }
        Some(_) => Err(idempotency_completed_replay_mismatch()),
        None => Err(idempotency_reservation_not_pending()),
    }
}

fn completed_record_matches(
    record: &IdempotencyRecord,
    reservation: &IdempotencyReservation,
    status_code: u16,
    response_body: &serde_json::Value,
    classification: &IdempotencyReplayClassification,
) -> bool {
    record.request_fingerprint == reservation.request_fingerprint
        && record.status_code == status_code
        && record.response_body == *response_body
        && record.classification == *classification
}

fn enforce_idempotency_quota(
    state: &IdempotencyState,
    replacing_key: &IdempotencyStoreKey,
    identity: &IdempotencyQuotaIdentity,
    policy: &IdempotencyRetentionPolicy,
) -> Result<(), VfsError> {
    if quota_exceeded(
        policy.max_records_per_scope,
        count_records_matching(state, replacing_key, |record_identity| {
            record_identity.scope == identity.scope
        }),
    ) || identity.repo_id.as_ref().is_some_and(|repo_id| {
        quota_exceeded(
            policy.max_records_per_repo,
            count_records_matching(state, replacing_key, |record_identity| {
                record_identity.repo_id.as_ref() == Some(repo_id)
            }),
        )
    }) || identity.workspace_id.as_ref().is_some_and(|workspace_id| {
        quota_exceeded(
            policy.max_records_per_workspace,
            count_records_matching(state, replacing_key, |record_identity| {
                record_identity.workspace_id.as_ref() == Some(workspace_id)
            }),
        )
    }) || identity.principal_uid.is_some_and(|principal_uid| {
        quota_exceeded(
            policy.max_records_per_principal,
            count_records_matching(state, replacing_key, |record_identity| {
                record_identity.principal_uid == Some(principal_uid)
            }),
        )
    }) {
        return Err(VfsError::InvalidArgs {
            message: "idempotency quota exceeded".to_string(),
        });
    }
    Ok(())
}

fn quota_exceeded(limit: Option<usize>, current: usize) -> bool {
    limit.is_some_and(|limit| current >= limit)
}

fn count_records_matching(
    state: &IdempotencyState,
    replacing_key: &IdempotencyStoreKey,
    predicate: impl Fn(&IdempotencyQuotaIdentity) -> bool,
) -> usize {
    let completed = state
        .completed
        .iter()
        .filter(|(key, record)| *key != replacing_key && predicate(&record.quota_identity))
        .count();
    let pending = state
        .pending
        .iter()
        .filter(|(key, record)| *key != replacing_key && predicate(&record.quota_identity))
        .count();
    completed + pending
}

fn normalize_quota_identity(identity: &mut IdempotencyQuotaIdentity, scope: &str) {
    identity.scope = scope.to_string();
    let parsed = IdempotencyQuotaIdentity::for_scope(scope);
    identity.repo_id = parsed.repo_id.or_else(|| identity.repo_id.take());
    identity.workspace_id = parsed.workspace_id.or_else(|| identity.workspace_id.take());
}

fn parse_scope_component(scope: &str, label: &str) -> Option<String> {
    let marker = format!("{label}:");
    let start = scope.find(&marker)? + marker.len();
    let value = scope[start..]
        .split(|ch: char| ch == ':' || ch.is_whitespace())
        .next()
        .unwrap_or_default();
    (!value.is_empty()).then(|| value.to_string())
}

fn reject_secret_bearing_replay(
    classification: &IdempotencyReplayClassification,
) -> Result<(), VfsError> {
    if classification == &IdempotencyReplayClassification::SecretBearing {
        return Err(VfsError::InvalidArgs {
            message: "idempotency replay is not persistable".to_string(),
        });
    }
    Ok(())
}

fn sweep_retention_locked(
    state: &mut IdempotencyState,
    request: &IdempotencySweepRequest,
) -> IdempotencySweepSummary {
    let mut summary = IdempotencySweepSummary::default();
    let retain_keys = request
        .retain_keys
        .iter()
        .cloned()
        .collect::<HashSet<(String, String)>>();
    let retain_commit_ids = request
        .retain_commit_ids
        .iter()
        .cloned()
        .collect::<HashSet<String>>();
    let repo_prefix = request
        .repo_id
        .as_ref()
        .map(|repo_id| format!("repo:{}:", repo_id.as_str()));
    let repo_id = request.repo_id.as_ref();

    let pending_keys = matching_idempotency_keys(
        &state.pending,
        repo_id,
        repo_prefix.as_deref(),
        request.limit,
    );
    let mut pending_to_remove = Vec::new();
    for key in pending_keys {
        if summary.scanned >= request.limit {
            break;
        }
        summary.scanned += 1;
        if retain_keys.contains(&(key.scope.clone(), key.key_hash.clone())) {
            summary.retained_for_roots += 1;
            increment_reason(&mut summary, "explicit_key");
            continue;
        }
        let Some(pending) = state.pending.get(&key) else {
            continue;
        };
        let stale = request
            .now_unix_seconds
            .saturating_sub(pending.reserved_at_unix_seconds)
            > request.policy.pending_stale_after_seconds;
        if stale {
            summary.stale_pending += 1;
            if request.abort_stale_pending {
                pending_to_remove.push(key);
                summary.aborted_pending += 1;
            }
        }
    }
    for key in pending_to_remove {
        state.pending.remove(&key);
    }

    let remaining_limit = request.limit.saturating_sub(summary.scanned);
    let completed_keys = matching_idempotency_keys(
        &state.completed,
        repo_id,
        repo_prefix.as_deref(),
        remaining_limit,
    );
    let mut completed_to_remove = Vec::new();
    for key in completed_keys {
        if summary.scanned >= request.limit {
            break;
        }
        summary.scanned += 1;
        if retain_keys.contains(&(key.scope.clone(), key.key_hash.clone())) {
            summary.retained_for_roots += 1;
            increment_reason(&mut summary, "explicit_key");
            continue;
        }
        let Some(record) = state.completed.get(&key) else {
            continue;
        };
        if !retain_commit_ids.is_empty() {
            let commit_roots = collect_commit_root_hexes(&record.response_body);
            if commit_roots.truncated {
                summary.retained_for_roots += 1;
                increment_reason(&mut summary, "scan_limit_reached");
                continue;
            }
            if commit_roots
                .roots
                .iter()
                .any(|commit| retain_commit_ids.contains(commit))
            {
                summary.retained_for_roots += 1;
                increment_reason(&mut summary, "commit_root");
                continue;
            }
        }
        let expired = request
            .now_unix_seconds
            .saturating_sub(record.completed_at_unix_seconds)
            > request.policy.completed_ttl_seconds;
        if expired {
            completed_to_remove.push(key);
            summary.swept_completed += 1;
        }
    }
    for key in completed_to_remove {
        state.completed.remove(&key);
    }

    summary.remaining = state.pending.len() + state.completed.len();
    summary
}

fn matching_idempotency_keys<V>(
    records: &BTreeMap<IdempotencyStoreKey, V>,
    repo_id: Option<&crate::backend::RepoId>,
    repo_prefix: Option<&str>,
    limit: usize,
) -> Vec<IdempotencyStoreKey> {
    if limit == 0 {
        return Vec::new();
    }
    match (repo_id, repo_prefix) {
        (Some(repo_id), Some(repo_prefix)) if repo_id != &crate::backend::RepoId::local() => {
            let start = IdempotencyStoreKey {
                scope: repo_prefix.to_string(),
                key_hash: String::new(),
            };
            records
                .range(start..)
                .take_while(|(key, _)| key.scope.starts_with(repo_prefix))
                .take(limit)
                .map(|(key, _)| key.clone())
                .collect()
        }
        (Some(_repo_id), Some(_repo_prefix)) => {
            let repo_namespace_start = IdempotencyStoreKey {
                scope: "repo:".to_string(),
                key_hash: String::new(),
            };
            let repo_namespace_end = IdempotencyStoreKey {
                scope: "repo;".to_string(),
                key_hash: String::new(),
            };
            let mut keys = records
                .range(..repo_namespace_start)
                .take(limit)
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            if keys.len() < limit {
                keys.extend(
                    records
                        .range(repo_namespace_end..)
                        .take(limit - keys.len())
                        .map(|(key, _)| key.clone()),
                );
            }
            keys
        }
        _ => records.keys().take(limit).cloned().collect(),
    }
}

struct RetainedCommitRoots {
    roots: Vec<String>,
    truncated: bool,
}

fn collect_commit_root_hexes(value: &serde_json::Value) -> RetainedCommitRoots {
    fn spend_budget(budget: &mut usize) -> bool {
        let Some(next) = budget.checked_sub(1) else {
            return false;
        };
        *budget = next;
        true
    }

    fn collect_from_commit_value(
        value: &serde_json::Value,
        roots: &mut BTreeSet<String>,
        budget: &mut usize,
        depth: usize,
    ) -> bool {
        if depth > IDEMPOTENCY_RETAINED_COMMIT_JSON_DEPTH_LIMIT || !spend_budget(budget) {
            return false;
        }
        match value {
            serde_json::Value::String(text) => {
                if is_lower_hex_digest(text) {
                    roots.insert(text.clone());
                }
                true
            }
            serde_json::Value::Array(values) => values
                .iter()
                .all(|value| collect_from_commit_value(value, roots, budget, depth + 1)),
            serde_json::Value::Object(values) => values
                .values()
                .all(|value| collect_from_commit_value(value, roots, budget, depth + 1)),
            serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
                true
            }
        }
    }

    fn collect_any(
        value: &serde_json::Value,
        roots: &mut BTreeSet<String>,
        budget: &mut usize,
        depth: usize,
    ) -> bool {
        if depth > IDEMPOTENCY_RETAINED_COMMIT_JSON_DEPTH_LIMIT || !spend_budget(budget) {
            return false;
        }
        match value {
            serde_json::Value::Array(values) => values
                .iter()
                .all(|value| collect_any(value, roots, budget, depth + 1)),
            serde_json::Value::Object(values) => values.iter().all(|(key, value)| {
                if idempotency_commit_key(key) {
                    collect_from_commit_value(value, roots, budget, depth + 1)
                } else {
                    collect_any(value, roots, budget, depth + 1)
                }
            }),
            serde_json::Value::String(_)
            | serde_json::Value::Null
            | serde_json::Value::Bool(_)
            | serde_json::Value::Number(_) => true,
        }
    }

    let mut roots = BTreeSet::new();
    let mut budget = IDEMPOTENCY_RETAINED_COMMIT_JSON_NODE_LIMIT;
    let completed = collect_any(value, &mut roots, &mut budget, 0);
    RetainedCommitRoots {
        roots: roots.into_iter().collect(),
        truncated: !completed,
    }
}

fn is_lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn idempotency_commit_key(key: &str) -> bool {
    matches!(
        key,
        "hash"
            | "commit_id"
            | "head_commit"
            | "previous_commit"
            | "new_commit"
            | "revert_commit"
            | "reverted_to"
            | "target_commit"
            | "target"
            | "expected_head"
    )
}

fn increment_reason(summary: &mut IdempotencySweepSummary, reason: &str) {
    *summary
        .redacted_reasons
        .entry(reason.to_string())
        .or_insert(0) += 1;
}

fn idempotency_reservation_not_pending() -> VfsError {
    VfsError::InvalidArgs {
        message: "idempotency reservation is not pending".to_string(),
    }
}

fn idempotency_completed_replay_mismatch() -> VfsError {
    VfsError::InvalidArgs {
        message: "idempotency completed replay does not match reservation".to_string(),
    }
}

fn idempotency_policy_not_supported() -> VfsError {
    VfsError::NotSupported {
        message: "idempotency retention policy is not supported by this store".to_string(),
    }
}

impl PendingIdempotencyReservation {
    fn matches(&self, reservation: &IdempotencyReservation) -> bool {
        self.request_fingerprint == reservation.request_fingerprint
            && self.reservation_token == reservation.reservation_token
    }
}

fn persisted_store_version(bytes: &[u8]) -> Result<u32, VfsError> {
    let version_bytes = bytes.get(..4).ok_or_else(|| VfsError::CorruptStore {
        message: "idempotency store missing version".to_string(),
    })?;
    Ok(u32::from_le_bytes([
        version_bytes[0],
        version_bytes[1],
        version_bytes[2],
        version_bytes[3],
    ]))
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[allow(dead_code)]
fn validate_store_part(value: String, label: &str) -> Result<String, VfsError> {
    if value.is_empty()
        || value.len() > MAX_IDEMPOTENCY_STORE_PART_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(VfsError::InvalidArgs {
            message: format!("{label} must be 1-255 non-control characters"),
        });
    }
    Ok(value)
}

#[allow(dead_code)]
fn validate_hex_store_part(value: String, label: &str) -> Result<String, VfsError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(VfsError::InvalidArgs {
            message: format!("{label} must be a lowercase 64-character hex digest"),
        });
    }
    validate_store_part(value, label)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use serde_json::json;
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    fn temp_idempotency_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "stratum_idempotency_{}_{}_{}.bin",
            name,
            std::process::id(),
            uuid::Uuid::new_v4()
        ))
    }

    struct LegacyIdempotencyStore;

    #[async_trait]
    impl IdempotencyStore for LegacyIdempotencyStore {
        async fn begin(
            &self,
            scope: &str,
            key: &IdempotencyKey,
            request_fingerprint: &str,
        ) -> Result<IdempotencyBegin, VfsError> {
            Ok(IdempotencyBegin::Execute(
                IdempotencyReservation::for_store(scope, key, request_fingerprint),
            ))
        }

        async fn complete(
            &self,
            _reservation: &IdempotencyReservation,
            _status_code: u16,
            _response_body: serde_json::Value,
        ) -> Result<(), VfsError> {
            Ok(())
        }

        async fn abort(&self, _reservation: &IdempotencyReservation) {}
    }

    #[test]
    fn reservation_accessors_expose_store_identity_without_raw_key() {
        let key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("raw-retry-key")).unwrap();
        let reservation = IdempotencyReservation::for_store("runs:create", &key, "request-a");

        assert_eq!(reservation.scope(), "runs:create");
        assert_eq!(reservation.key_hash(), key.key_hash());
        assert_eq!(reservation.request_fingerprint(), "request-a");
        assert!(!reservation.reservation_token().is_empty());
        assert_ne!(reservation.key_hash(), "raw-retry-key");
    }

    #[test]
    fn reservation_for_store_parts_validates_bounded_redacted_parts() {
        let reservation = IdempotencyReservation::for_store_parts(
            "vcs:commit",
            "a".repeat(64),
            "b".repeat(64),
            "reservation-token",
        )
        .unwrap();

        assert_eq!(reservation.scope(), "vcs:commit");
        assert_eq!(reservation.key_hash(), "a".repeat(64));
        assert_eq!(reservation.request_fingerprint(), "b".repeat(64));
        assert_eq!(reservation.reservation_token(), "reservation-token");

        for (scope, key_hash, request_fingerprint, reservation_token) in [
            ("", "a".repeat(64), "b".repeat(64), "token".to_string()),
            (
                "scope\n",
                "a".repeat(64),
                "b".repeat(64),
                "token".to_string(),
            ),
            (
                "scope",
                "not-a-store-key-hash".to_string(),
                "b".repeat(64),
                "token".to_string(),
            ),
            (
                "scope",
                "a".repeat(64),
                "not-a-fingerprint".to_string(),
                "token".to_string(),
            ),
            ("scope", "A".repeat(64), "b".repeat(64), "token".to_string()),
            ("scope", "a".repeat(64), "b".repeat(64), "".to_string()),
            (
                "scope",
                "a".repeat(64),
                "b".repeat(64),
                "x".repeat(MAX_IDEMPOTENCY_STORE_PART_BYTES + 1),
            ),
        ] {
            assert!(
                IdempotencyReservation::for_store_parts(
                    scope,
                    key_hash,
                    request_fingerprint,
                    reservation_token
                )
                .is_err()
            );
        }
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
    async fn list_retained_for_repo_scopes_local_and_repo_qualified_records() {
        let store = InMemoryIdempotencyStore::new();
        let local_repo = crate::backend::RepoId::local();
        let repo_a = crate::backend::RepoId::new("repo_a").unwrap();
        let repo_b = crate::backend::RepoId::new("repo_b").unwrap();
        let local_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("local-key")).unwrap();
        let repo_a_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("repo-a-key")).unwrap();
        let repo_b_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("repo-b-key")).unwrap();

        let local_reservation = match store
            .begin("vcs:commit", &local_key, "local-request")
            .await
            .unwrap()
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected local execute, got {other:?}"),
        };
        store
            .complete(
                &local_reservation,
                200,
                json!({"commit_id": "local-commit"}),
            )
            .await
            .unwrap();

        let repo_a_pending = match store
            .begin("repo:repo_a:vcs:commit", &repo_a_key, "repo-a-request")
            .await
            .unwrap()
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected repo A execute, got {other:?}"),
        };
        let repo_b_reservation = match store
            .begin("repo:repo_b:vcs:commit", &repo_b_key, "repo-b-request")
            .await
            .unwrap()
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected repo B execute, got {other:?}"),
        };
        store
            .complete(
                &repo_b_reservation,
                200,
                json!({"commit_id": "repo-b-commit"}),
            )
            .await
            .unwrap();

        let local_records = store.list_retained_for_repo(&local_repo, 10).await.unwrap();
        assert_eq!(local_records.len(), 1);
        assert_eq!(local_records[0].scope(), "vcs:commit");
        assert!(!local_records[0].pending);
        assert!(!format!("{local_records:?}").contains("local-commit"));

        let repo_a_records = store.list_retained_for_repo(&repo_a, 10).await.unwrap();
        assert_eq!(repo_a_records.len(), 1);
        assert_eq!(repo_a_records[0].scope(), "repo:repo_a:vcs:commit");
        assert!(repo_a_records[0].pending);

        let repo_b_records = store.list_retained_for_repo(&repo_b, 10).await.unwrap();
        assert_eq!(repo_b_records.len(), 1);
        assert_eq!(repo_b_records[0].scope(), "repo:repo_b:vcs:commit");
        assert!(!repo_b_records[0].pending);

        store.abort(&repo_a_pending).await;
    }

    #[tokio::test]
    async fn stale_aborted_reservation_cannot_complete_or_abort_later_retry() {
        let store = InMemoryIdempotencyStore::new();
        let key = IdempotencyKey::parse_header_value(&HeaderValue::from_static("run-create-retry"))
            .unwrap();

        let stale = match store.begin("runs:create", &key, "request-a").await.unwrap() {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected first begin to execute, got {other:?}"),
        };
        store.abort(&stale).await;

        let current = match store.begin("runs:create", &key, "request-a").await.unwrap() {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected retry begin to execute, got {other:?}"),
        };

        assert!(matches!(
            store
                .complete(&stale, 201, json!({"run_id": "stale"}))
                .await,
            Err(VfsError::InvalidArgs { .. }),
        ));
        assert!(matches!(
            store.begin("runs:create", &key, "request-a").await.unwrap(),
            IdempotencyBegin::InProgress
        ));

        store.abort(&stale).await;
        assert!(matches!(
            store.begin("runs:create", &key, "request-a").await.unwrap(),
            IdempotencyBegin::InProgress
        ));

        store
            .complete(&current, 201, json!({"run_id": "current"}))
            .await
            .unwrap();
        let replay = match store.begin("runs:create", &key, "request-a").await.unwrap() {
            IdempotencyBegin::Replay(record) => record,
            other => panic!("expected replay after current completion, got {other:?}"),
        };
        assert_eq!(replay.response_body, json!({"run_id": "current"}));
    }

    fn strict_policy() -> IdempotencyRetentionPolicy {
        IdempotencyRetentionPolicy {
            completed_ttl_seconds: 10,
            pending_stale_after_seconds: 10,
            max_records_per_scope: Some(10),
            max_records_per_repo: Some(10),
            max_records_per_workspace: Some(10),
            max_records_per_principal: Some(10),
        }
    }

    fn quota_identity(scope: &str) -> IdempotencyQuotaIdentity {
        IdempotencyQuotaIdentity::for_scope(scope)
    }

    #[tokio::test]
    async fn sweep_removes_expired_completed_records_and_retains_unexpired_records() {
        let store = InMemoryIdempotencyStore::new();
        let expired_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("expired")).unwrap();
        let retained_key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("retained")).unwrap();
        let policy = strict_policy();

        let expired = match store
            .begin_with_policy(
                "repo:repo_a:vcs:commit",
                &expired_key,
                "request-expired",
                quota_identity("repo:repo_a:vcs:commit"),
                &policy,
            )
            .await
            .unwrap()
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected execute, got {other:?}"),
        };
        store
            .complete_with_classification(
                &expired,
                200,
                json!({"commit_id": "old"}),
                IdempotencyReplayClassification::SecretFree,
            )
            .await
            .unwrap();

        let retained = match store
            .begin_with_policy(
                "repo:repo_a:vcs:commit",
                &retained_key,
                "request-retained",
                quota_identity("repo:repo_a:vcs:commit"),
                &policy,
            )
            .await
            .unwrap()
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected execute, got {other:?}"),
        };
        store
            .complete_with_classification(
                &retained,
                200,
                json!({"commit_id": "new"}),
                IdempotencyReplayClassification::SecretFree,
            )
            .await
            .unwrap();

        {
            let mut guard = store.inner.write().await;
            guard
                .completed
                .get_mut(&expired.key)
                .unwrap()
                .completed_at_unix_seconds = 100;
            guard
                .completed
                .get_mut(&retained.key)
                .unwrap()
                .completed_at_unix_seconds = 200;
        }

        let summary = store
            .sweep_retention(IdempotencySweepRequest {
                now_unix_seconds: 206,
                limit: 10,
                policy: policy.clone(),
                repo_id: Some(crate::backend::RepoId::new("repo_a").unwrap()),
                retain_keys: Vec::new(),
                retain_commit_ids: Vec::new(),
                abort_stale_pending: true,
            })
            .await
            .unwrap();

        assert_eq!(summary.scanned, 2);
        assert_eq!(summary.swept_completed, 1);
        assert!(matches!(
            store
                .begin_with_policy(
                    "repo:repo_a:vcs:commit",
                    &expired_key,
                    "request-expired",
                    quota_identity("repo:repo_a:vcs:commit"),
                    &policy,
                )
                .await
                .unwrap(),
            IdempotencyBegin::Execute(_)
        ));
        assert!(matches!(
            store
                .begin_with_policy(
                    "repo:repo_a:vcs:commit",
                    &retained_key,
                    "request-retained",
                    quota_identity("repo:repo_a:vcs:commit"),
                    &policy,
                )
                .await
                .unwrap(),
            IdempotencyBegin::Replay(_)
        ));
    }

    #[tokio::test]
    async fn pending_stale_policy_controls_takeover_conflict_and_stale_token_fencing() {
        let store = InMemoryIdempotencyStore::new();
        let key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("stale-pending")).unwrap();
        let policy = strict_policy();
        let scope = "repo:repo_a:runs:create";
        let stale = match store
            .begin_with_policy(scope, &key, "request-a", quota_identity(scope), &policy)
            .await
            .unwrap()
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected execute, got {other:?}"),
        };

        assert!(matches!(
            store
                .begin_with_policy(scope, &key, "request-a", quota_identity(scope), &policy)
                .await
                .unwrap(),
            IdempotencyBegin::InProgress
        ));
        assert!(matches!(
            store
                .begin_with_policy(scope, &key, "request-b", quota_identity(scope), &policy)
                .await
                .unwrap(),
            IdempotencyBegin::Conflict
        ));

        store
            .inner
            .write()
            .await
            .pending
            .get_mut(&stale.key)
            .unwrap()
            .reserved_at_unix_seconds = 100;
        let current = match store
            .begin_with_policy(scope, &key, "request-a", quota_identity(scope), &policy)
            .await
            .unwrap()
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected stale takeover execute, got {other:?}"),
        };
        assert_ne!(stale.reservation_token(), current.reservation_token());

        assert!(matches!(
            store
                .complete_with_classification(
                    &stale,
                    201,
                    json!({"run_id": "stale"}),
                    IdempotencyReplayClassification::SecretFree,
                )
                .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        store.abort(&stale).await;
        assert!(matches!(
            store
                .begin_with_policy(scope, &key, "request-a", quota_identity(scope), &policy)
                .await
                .unwrap(),
            IdempotencyBegin::InProgress
        ));

        store
            .inner
            .write()
            .await
            .pending
            .get_mut(&current.key)
            .unwrap()
            .reserved_at_unix_seconds = 100;
        assert!(matches!(
            store
                .begin_with_policy(scope, &key, "request-b", quota_identity(scope), &policy)
                .await
                .unwrap(),
            IdempotencyBegin::Conflict
        ));
        assert!(store.inner.read().await.pending.is_empty());
    }

    #[tokio::test]
    async fn quotas_reject_before_pending_insert_and_errors_are_redacted() {
        let store = InMemoryIdempotencyStore::new();
        let mut policy = strict_policy();
        policy.max_records_per_scope = Some(1);
        policy.max_records_per_repo = Some(1);
        policy.max_records_per_workspace = Some(1);
        let scope = "repo:repo_secret:workspace:550e8400-e29b-41d4-a716-446655440000:runs:create";
        let first =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("quota-first")).unwrap();
        let second =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("quota-second")).unwrap();
        assert_eq!(
            IdempotencyQuotaIdentity::for_scope(
                "POST /runs workspace:550e8400-e29b-41d4-a716-446655440000"
            )
            .workspace_id
            .as_deref(),
            Some("550e8400-e29b-41d4-a716-446655440000")
        );

        assert!(matches!(
            store
                .begin_with_policy(
                    scope,
                    &first,
                    "fingerprint-secret",
                    quota_identity(scope),
                    &policy
                )
                .await
                .unwrap(),
            IdempotencyBegin::Execute(_)
        ));

        let err = store
            .begin_with_policy(
                scope,
                &second,
                "fingerprint-secret-body",
                quota_identity(scope),
                &policy,
            )
            .await
            .unwrap_err();
        let rendered = format!("{err:?}");
        assert!(!rendered.contains("quota-second"));
        assert!(!rendered.contains("fingerprint-secret"));
        assert!(!rendered.contains("repo_secret"));
        assert!(!rendered.contains("workspace_token"));
        assert_eq!(store.inner.read().await.pending.len(), 1);
    }

    #[tokio::test]
    async fn quota_identity_is_normalized_to_actual_scope_before_counting() {
        let store = InMemoryIdempotencyStore::new();
        let mut policy = strict_policy();
        policy.max_records_per_scope = Some(1);
        policy.max_records_per_repo = Some(1);
        policy.max_records_per_workspace = Some(1);
        let scope = "repo:repo_a:workspace:workspace_a:runs:create";
        let first =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("normalized-first"))
                .unwrap();
        let second =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("normalized-second"))
                .unwrap();
        let spoofed_identity = IdempotencyQuotaIdentity {
            scope: "repo:repo_b:workspace:workspace_b:runs:create".to_string(),
            repo_id: Some("repo_b".to_string()),
            workspace_id: Some("workspace_b".to_string()),
            principal_uid: None,
        };

        assert!(matches!(
            store
                .begin_with_policy(
                    scope,
                    &first,
                    "request-a",
                    spoofed_identity.clone(),
                    &policy
                )
                .await
                .unwrap(),
            IdempotencyBegin::Execute(_)
        ));
        assert!(matches!(
            store
                .begin_with_policy(scope, &second, "request-b", spoofed_identity, &policy)
                .await,
            Err(VfsError::InvalidArgs { .. })
        ));
    }

    #[tokio::test]
    async fn classification_rejects_secret_bearing_and_projects_partial_without_body_leaks() {
        let store = InMemoryIdempotencyStore::new();
        let key = IdempotencyKey::parse_header_value(&HeaderValue::from_static("classification"))
            .unwrap();
        let scope = "repo:repo_a:vcs:commit";
        let reservation = match store.begin(scope, &key, "request-a").await.unwrap() {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected execute, got {other:?}"),
        };

        let err = store
            .complete_with_classification(
                &reservation,
                200,
                json!({"workspace_token": "secret-token"}),
                IdempotencyReplayClassification::SecretBearing,
            )
            .await
            .unwrap_err();
        assert!(!format!("{err:?}").contains("secret-token"));
        assert!(matches!(
            store.begin(scope, &key, "request-a").await.unwrap(),
            IdempotencyBegin::InProgress
        ));

        store
            .complete_with_classification(
                &reservation,
                202,
                json!({"commit_id": "commit-1", "message": null}),
                IdempotencyReplayClassification::Partial,
            )
            .await
            .unwrap();
        let replay = match store.begin(scope, &key, "request-a").await.unwrap() {
            IdempotencyBegin::Replay(record) => record,
            other => panic!("expected replay, got {other:?}"),
        };
        let replay_debug = format!("{replay:?}");
        assert!(!replay_debug.contains("commit-1"));
        assert!(!replay_debug.contains("request-a"));
        assert!(!format!("{:?}", IdempotencyBegin::Replay(replay.clone())).contains("commit-1"));
        assert_eq!(
            replay.classification,
            IdempotencyReplayClassification::Partial
        );
        assert!(matches!(
            store
                .complete_or_match_with_classification(
                    &reservation,
                    202,
                    json!({"commit_id": "commit-1", "message": null}),
                    IdempotencyReplayClassification::SecretFree,
                )
                .await,
            Err(VfsError::InvalidArgs { .. })
        ));

        let retained = store
            .list_retained_for_repo(&crate::backend::RepoId::new("repo_a").unwrap(), 10)
            .await
            .unwrap();
        assert_eq!(
            retained[0].classification,
            Some(IdempotencyReplayClassification::Partial)
        );
        let rendered = format!("{retained:?}");
        assert!(rendered.contains("Partial"));
        assert!(!rendered.contains("commit-1"));
    }

    #[tokio::test]
    async fn legacy_store_policy_defaults_fail_closed_for_new_semantics() {
        let store = LegacyIdempotencyStore;
        let key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("legacy-policy")).unwrap();
        let mut policy = IdempotencyRetentionPolicy::unlimited();
        policy.max_records_per_scope = Some(1);

        assert!(matches!(
            store
                .begin_with_policy(
                    "runs:create",
                    &key,
                    "request-a",
                    quota_identity("runs:create"),
                    &policy,
                )
                .await,
            Err(VfsError::NotSupported { .. })
        ));

        let reservation = IdempotencyReservation::for_store("runs:create", &key, "request-a");
        assert!(matches!(
            store
                .complete_with_classification(
                    &reservation,
                    202,
                    json!({"run_id": "run_123"}),
                    IdempotencyReplayClassification::Partial,
                )
                .await,
            Err(VfsError::NotSupported { .. })
        ));
    }

    #[tokio::test]
    async fn local_v1_records_decode_as_secret_free_with_migration_timestamps() {
        let path = temp_idempotency_path("v1-migration");
        let key = IdempotencyStoreKey {
            scope: "runs:create".to_string(),
            key_hash: "a".repeat(64),
        };
        let bytes = crate::codec::serialize(&PersistedIdempotencyStoreV1 {
            version: 1,
            records: vec![PersistedIdempotencyRecordV1 {
                scope: key.scope.clone(),
                key_hash: key.key_hash.clone(),
                request_fingerprint: "b".repeat(64),
                status_code: 201,
                response_body_json: serde_json::to_vec(&json!({"run_id": "run_123"})).unwrap(),
            }],
        })
        .unwrap();
        fs::write(&path, bytes).unwrap();

        let store = LocalIdempotencyStore::open(&path).unwrap();
        let guard = store.inner.read().await;
        let record = guard.completed.get(&key).unwrap();
        assert_eq!(
            record.classification,
            IdempotencyReplayClassification::SecretFree
        );
        assert!(record.completed_at_unix_seconds > 0);
        assert!(record.completed_at_unix_seconds <= now_unix_seconds());
    }

    #[tokio::test]
    async fn local_v2_records_preserve_quota_identity_on_reload() {
        let path = temp_idempotency_path("v2-quota-identity");
        let store = LocalIdempotencyStore::open(&path).unwrap();
        let scope = "repo:repo_a:POST /runs workspace:workspace_a";
        let mut policy = IdempotencyRetentionPolicy::unlimited();
        policy.max_records_per_principal = Some(1);
        let mut identity = quota_identity(scope);
        identity.principal_uid = Some(42);
        let first =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("first-quota")).unwrap();
        let reservation = match store
            .begin_with_policy(scope, &first, "request-a", identity.clone(), &policy)
            .await
            .unwrap()
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected execute, got {other:?}"),
        };
        store
            .complete(&reservation, 201, json!({"run_id": "run_123"}))
            .await
            .unwrap();

        let reloaded = LocalIdempotencyStore::open(&path).unwrap();
        let second =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("second-quota")).unwrap();
        assert!(matches!(
            reloaded
                .begin_with_policy(scope, &second, "request-b", identity, &policy)
                .await,
            Err(VfsError::InvalidArgs { .. })
        ));
    }

    #[test]
    fn local_v2_secret_bearing_records_do_not_load() {
        let path = temp_idempotency_path("v2-secret-bearing");
        let bytes = crate::codec::serialize(&PersistedIdempotencyStore {
            version: IDEMPOTENCY_STORE_VERSION,
            records: vec![PersistedIdempotencyRecord {
                scope: "runs:create".to_string(),
                key_hash: "a".repeat(64),
                request_fingerprint: "b".repeat(64),
                status_code: 201,
                response_body_json: serde_json::to_vec(&json!({
                    "workspace_token": "secret-token"
                }))
                .unwrap(),
                completed_at_unix_seconds: Some(100),
                replay_classification: Some(IdempotencyReplayClassification::SecretBearing),
                quota_identity: None,
            }],
        })
        .unwrap();
        fs::write(&path, bytes).unwrap();

        let err = LocalIdempotencyStore::open(&path).unwrap_err();
        assert!(matches!(err, VfsError::CorruptStore { .. }));
        assert!(!format!("{err:?}").contains("secret-token"));
    }

    #[test]
    fn local_duplicate_decode_errors_are_redacted() {
        let scope = "repo:repo_secret:runs:create".to_string();
        let key_hash = "a".repeat(64);
        let request_fingerprint = "b".repeat(64);
        let response_body_json = serde_json::to_vec(&json!({"run_id": "run_123"})).unwrap();

        let v1 = crate::codec::serialize(&PersistedIdempotencyStoreV1 {
            version: IDEMPOTENCY_STORE_V1_VERSION,
            records: vec![
                PersistedIdempotencyRecordV1 {
                    scope: scope.clone(),
                    key_hash: key_hash.clone(),
                    request_fingerprint: request_fingerprint.clone(),
                    status_code: 201,
                    response_body_json: response_body_json.clone(),
                },
                PersistedIdempotencyRecordV1 {
                    scope: scope.clone(),
                    key_hash: key_hash.clone(),
                    request_fingerprint: request_fingerprint.clone(),
                    status_code: 201,
                    response_body_json: response_body_json.clone(),
                },
            ],
        })
        .unwrap();
        let v1_err = LocalIdempotencyStore::decode(&v1).unwrap_err();
        let v1_rendered = format!("{v1_err:?}");
        assert!(!v1_rendered.contains("repo_secret"));
        assert!(!v1_rendered.contains(&key_hash));

        let v2_record = PersistedIdempotencyRecord {
            scope: scope.clone(),
            key_hash: key_hash.clone(),
            request_fingerprint,
            status_code: 201,
            response_body_json,
            completed_at_unix_seconds: Some(100),
            replay_classification: Some(IdempotencyReplayClassification::SecretFree),
            quota_identity: None,
        };
        let v2 = crate::codec::serialize(&PersistedIdempotencyStore {
            version: IDEMPOTENCY_STORE_VERSION,
            records: vec![v2_record.clone(), v2_record],
        })
        .unwrap();
        let v2_err = LocalIdempotencyStore::decode(&v2).unwrap_err();
        let v2_rendered = format!("{v2_err:?}");
        assert!(!v2_rendered.contains("repo_secret"));
        assert!(!v2_rendered.contains(&key_hash));
    }

    #[tokio::test]
    async fn sweep_is_bounded_and_preserves_explicit_blockers() {
        let store = InMemoryIdempotencyStore::new();
        let policy = strict_policy();
        let blocked_commit = "b".repeat(64);
        let swept_commit = "c".repeat(64);
        for (raw, commit) in [("blocked", &blocked_commit), ("swept", &swept_commit)] {
            let key =
                IdempotencyKey::parse_header_value(&HeaderValue::from_str(raw).unwrap()).unwrap();
            let reservation = match store
                .begin_with_policy(
                    "repo:repo_a:vcs:commit",
                    &key,
                    raw,
                    quota_identity("repo:repo_a:vcs:commit"),
                    &policy,
                )
                .await
                .unwrap()
            {
                IdempotencyBegin::Execute(reservation) => reservation,
                other => panic!("expected execute, got {other:?}"),
            };
            store
                .complete_with_classification(
                    &reservation,
                    200,
                    json!({"commit_id": commit}),
                    IdempotencyReplayClassification::SecretFree,
                )
                .await
                .unwrap();
            store
                .inner
                .write()
                .await
                .completed
                .get_mut(&reservation.key)
                .unwrap()
                .completed_at_unix_seconds = 100;
        }

        let summary = store
            .sweep_retention(IdempotencySweepRequest {
                now_unix_seconds: 1_000,
                limit: 2,
                policy,
                repo_id: Some(crate::backend::RepoId::new("repo_a").unwrap()),
                retain_keys: Vec::new(),
                retain_commit_ids: vec![blocked_commit],
                abort_stale_pending: true,
            })
            .await
            .unwrap();

        assert_eq!(summary.scanned, 2);
        assert_eq!(summary.swept_completed, 1);
        assert_eq!(summary.retained_for_roots, 1);
        let retained = store
            .list_retained_for_repo(&crate::backend::RepoId::new("repo_a").unwrap(), 10)
            .await
            .unwrap();
        assert_eq!(retained.len(), 1);
    }

    #[test]
    fn retained_commit_projection_ignores_unrelated_hex_values() {
        let commit = "b".repeat(64);
        let checksum = "c".repeat(64);
        let roots = collect_commit_root_hexes(&json!({
            "commit_id": commit,
            "checksum": checksum,
            "metadata": {
                "object_id": checksum,
                "nested": [{ "hash": commit }]
            }
        }));

        assert_eq!(roots.roots, vec![commit]);
        assert!(!roots.truncated);
    }

    #[tokio::test]
    async fn local_repo_sweep_does_not_starve_behind_repo_qualified_scopes() {
        let store = InMemoryIdempotencyStore::new();
        let policy = strict_policy();
        for scope in ["repo:aaa:vcs:commit", "runs:create"] {
            let key =
                IdempotencyKey::parse_header_value(&HeaderValue::from_str(scope).unwrap()).unwrap();
            let reservation = match store
                .begin_with_policy(scope, &key, "request-a", quota_identity(scope), &policy)
                .await
                .unwrap()
            {
                IdempotencyBegin::Execute(reservation) => reservation,
                other => panic!("expected execute, got {other:?}"),
            };
            store
                .complete(&reservation, 200, json!({"run_id": scope}))
                .await
                .unwrap();
            store
                .inner
                .write()
                .await
                .completed
                .get_mut(&reservation.key)
                .unwrap()
                .completed_at_unix_seconds = 100;
        }

        let summary = store
            .sweep_retention(IdempotencySweepRequest {
                now_unix_seconds: 1_000,
                limit: 1,
                policy,
                repo_id: Some(crate::backend::RepoId::local()),
                retain_keys: Vec::new(),
                retain_commit_ids: Vec::new(),
                abort_stale_pending: true,
            })
            .await
            .unwrap();

        assert_eq!(summary.scanned, 1);
        assert_eq!(summary.swept_completed, 1);
        let retained = store
            .list_retained_for_repo(&crate::backend::RepoId::local(), 10)
            .await
            .unwrap();
        assert!(retained.is_empty());
        assert!(
            store
                .list_retained_for_repo(&crate::backend::RepoId::new("aaa").unwrap(), 10)
                .await
                .unwrap()
                .len()
                == 1
        );
    }

    async fn assert_complete_or_match_contract(store: &dyn IdempotencyStore) {
        let scope = format!("runs:create:{}", Uuid::new_v4());
        let key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("complete-or-match"))
                .unwrap();
        let request_fingerprint = "a".repeat(64);
        let reservation = match store
            .begin(&scope, &key, &request_fingerprint)
            .await
            .unwrap()
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected execute, got {other:?}"),
        };

        store
            .complete_or_match(&reservation, 201, json!({"run_id": "run_123"}))
            .await
            .unwrap();

        let replay_reservation = IdempotencyReservation::for_store_parts(
            reservation.scope(),
            reservation.key_hash(),
            reservation.request_fingerprint(),
            reservation.reservation_token(),
        )
        .unwrap();
        store
            .complete_or_match(&replay_reservation, 201, json!({"run_id": "run_123"}))
            .await
            .unwrap();

        assert!(matches!(
            store
                .complete_or_match(&replay_reservation, 202, json!({"run_id": "run_123"}))
                .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        assert!(matches!(
            store
                .complete_or_match(&replay_reservation, 201, json!({"run_id": "different"}))
                .await,
            Err(VfsError::InvalidArgs { .. })
        ));

        let wrong_fingerprint = IdempotencyReservation::for_store_parts(
            reservation.scope(),
            reservation.key_hash(),
            "b".repeat(64),
            reservation.reservation_token(),
        )
        .unwrap();
        assert!(matches!(
            store
                .complete_or_match(&wrong_fingerprint, 201, json!({"run_id": "run_123"}))
                .await,
            Err(VfsError::InvalidArgs { .. })
        ));
    }

    async fn assert_wrong_pending_token_contract(store: &dyn IdempotencyStore) {
        let scope = format!("runs:create:wrong-token:{}", Uuid::new_v4());
        let key =
            IdempotencyKey::parse_header_value(&HeaderValue::from_static("wrong-token")).unwrap();
        let request_fingerprint = "c".repeat(64);
        let reservation = match store
            .begin(&scope, &key, &request_fingerprint)
            .await
            .unwrap()
        {
            IdempotencyBegin::Execute(reservation) => reservation,
            other => panic!("expected execute, got {other:?}"),
        };
        let wrong_token = IdempotencyReservation::for_store_parts(
            reservation.scope(),
            reservation.key_hash(),
            reservation.request_fingerprint(),
            "wrong-token",
        )
        .unwrap();

        assert!(matches!(
            store
                .complete_or_match(&wrong_token, 201, json!({"run_id": "stale"}))
                .await,
            Err(VfsError::InvalidArgs { .. })
        ));
        assert!(matches!(
            store
                .begin(&scope, &key, reservation.request_fingerprint())
                .await
                .unwrap(),
            IdempotencyBegin::InProgress
        ));
        store.abort(&reservation).await;
    }

    #[tokio::test]
    async fn in_memory_complete_or_match_completes_pending_and_accepts_matching_replay() {
        let store = InMemoryIdempotencyStore::new();

        assert_complete_or_match_contract(&store).await;
        assert_wrong_pending_token_contract(&store).await;
    }

    #[tokio::test]
    async fn local_complete_or_match_completes_pending_and_accepts_matching_replay() {
        let path = temp_idempotency_path("complete-or-match");
        let store = LocalIdempotencyStore::open(&path).unwrap();

        assert_complete_or_match_contract(&store).await;
        assert_wrong_pending_token_contract(&store).await;
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
