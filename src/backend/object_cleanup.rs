//! Cleanup claim contracts for durable object repair workers.
//!
//! Claims are leases around object-cleanup work. They coordinate workers, but
//! they are not a distributed transaction with object storage. Final object
//! deletion must stay behind a stronger metadata fencing contract.

use async_trait::async_trait;
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::backend::RepoId;
use crate::error::VfsError;
use crate::store::{ObjectId, ObjectKind};

const STALE_CLEANUP_CLAIM_MESSAGE: &str = "cleanup claim lease token is stale";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ObjectCleanupClaimKind {
    FinalObjectMetadataRepair,
    DurableMutationCasLostObjectCleanup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectCleanupClaimRequest {
    pub repo_id: RepoId,
    pub claim_kind: ObjectCleanupClaimKind,
    pub object_kind: ObjectKind,
    pub object_id: ObjectId,
    pub object_key: String,
    pub lease_owner: String,
    pub lease_duration: Duration,
}

impl ObjectCleanupClaimRequest {
    pub fn validate(&self) -> Result<(), VfsError> {
        validate_canonical_object_key(
            &self.repo_id,
            self.object_kind,
            &self.object_id,
            &self.object_key,
        )?;
        validate_lease_owner(&self.lease_owner)?;
        if self.lease_duration.as_millis() == 0 {
            return Err(VfsError::InvalidArgs {
                message: "cleanup claim lease duration must be at least 1 millisecond".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectCleanupClaim {
    pub repo_id: RepoId,
    pub claim_kind: ObjectCleanupClaimKind,
    pub object_kind: ObjectKind,
    pub object_id: ObjectId,
    pub object_key: String,
    pub lease_owner: String,
    pub lease_token: Uuid,
    pub lease_expires_at: SystemTime,
    pub attempts: u64,
}

impl ObjectCleanupClaim {
    pub fn target(&self) -> ObjectCleanupClaimTarget {
        ObjectCleanupClaimTarget {
            repo_id: self.repo_id.clone(),
            claim_kind: self.claim_kind,
            object_key: self.object_key.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectCleanupClaimTarget {
    pub repo_id: RepoId,
    pub claim_kind: ObjectCleanupClaimKind,
    pub object_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ObjectCleanupClaimState {
    Active,
    StaleActive,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectCleanupClaimStatus {
    repo_id: RepoId,
    claim_kind: ObjectCleanupClaimKind,
    object_kind: ObjectKind,
    object_id: ObjectId,
    object_key: String,
    state: ObjectCleanupClaimState,
    is_stale: bool,
    attempts: u64,
    lease_expires_at: SystemTime,
    completed_at: Option<SystemTime>,
    created_at: SystemTime,
    updated_at: SystemTime,
    has_last_failure: bool,
}

impl ObjectCleanupClaimStatus {
    pub fn for_store(input: ObjectCleanupClaimStatusInput) -> Self {
        Self {
            repo_id: input.repo_id,
            claim_kind: input.claim_kind,
            object_kind: input.object_kind,
            object_id: input.object_id,
            object_key: input.object_key,
            state: input.state,
            is_stale: input.is_stale,
            attempts: input.attempts,
            lease_expires_at: input.lease_expires_at,
            completed_at: input.completed_at,
            created_at: input.created_at,
            updated_at: input.updated_at,
            has_last_failure: input.has_last_failure,
        }
    }

    pub fn repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    pub const fn claim_kind(&self) -> ObjectCleanupClaimKind {
        self.claim_kind
    }

    pub const fn object_kind(&self) -> ObjectKind {
        self.object_kind
    }

    pub const fn object_id(&self) -> ObjectId {
        self.object_id
    }

    pub fn object_key(&self) -> &str {
        &self.object_key
    }

    pub const fn state(&self) -> ObjectCleanupClaimState {
        self.state
    }

    pub const fn is_stale(&self) -> bool {
        self.is_stale
    }

    pub const fn attempts(&self) -> u64 {
        self.attempts
    }

    pub const fn lease_expires_at(&self) -> SystemTime {
        self.lease_expires_at
    }

    pub const fn completed_at(&self) -> Option<SystemTime> {
        self.completed_at
    }

    pub const fn created_at(&self) -> SystemTime {
        self.created_at
    }

    pub const fn updated_at(&self) -> SystemTime {
        self.updated_at
    }

    pub const fn has_last_failure(&self) -> bool {
        self.has_last_failure
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectCleanupClaimStatusInput {
    pub repo_id: RepoId,
    pub claim_kind: ObjectCleanupClaimKind,
    pub object_kind: ObjectKind,
    pub object_id: ObjectId,
    pub object_key: String,
    pub state: ObjectCleanupClaimState,
    pub is_stale: bool,
    pub attempts: u64,
    pub lease_expires_at: SystemTime,
    pub completed_at: Option<SystemTime>,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub has_last_failure: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObjectCleanupClaimCounts {
    active: usize,
    stale_active: usize,
    completed: usize,
    failed: usize,
}

impl ObjectCleanupClaimCounts {
    pub const fn active(&self) -> usize {
        self.active
    }

    pub const fn stale_active(&self) -> usize {
        self.stale_active
    }

    pub const fn completed(&self) -> usize {
        self.completed
    }

    pub const fn failed(&self) -> usize {
        self.failed
    }

    pub const fn total(&self) -> usize {
        self.active + self.stale_active + self.completed + self.failed
    }

    pub fn add(&mut self, state: ObjectCleanupClaimState, count: usize) {
        match state {
            ObjectCleanupClaimState::Active => self.active += count,
            ObjectCleanupClaimState::StaleActive => self.stale_active += count,
            ObjectCleanupClaimState::Completed => self.completed += count,
            ObjectCleanupClaimState::Failed => self.failed += count,
        }
    }

    fn increment(&mut self, state: ObjectCleanupClaimState) {
        self.add(state, 1);
    }
}

#[async_trait]
pub trait ObjectCleanupClaimStore: Send + Sync {
    async fn claim(
        &self,
        request: ObjectCleanupClaimRequest,
    ) -> Result<Option<ObjectCleanupClaim>, VfsError>;

    async fn complete(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError>;

    async fn record_failure(
        &self,
        claim: &ObjectCleanupClaim,
        message: &str,
    ) -> Result<(), VfsError>;

    async fn list(&self, _limit: usize) -> Result<Vec<ObjectCleanupClaimStatus>, VfsError> {
        Err(VfsError::NotSupported {
            message: "object cleanup claim status listing is not supported by this store"
                .to_string(),
        })
    }

    async fn counts(&self) -> Result<ObjectCleanupClaimCounts, VfsError> {
        Err(VfsError::NotSupported {
            message: "object cleanup claim status counts are not supported by this store"
                .to_string(),
        })
    }
}

#[derive(Debug, Default)]
pub struct InMemoryObjectCleanupClaimStore {
    inner: RwLock<BTreeMap<ObjectCleanupClaimTarget, InMemoryClaimEntry>>,
    now_for_tests: RwLock<Option<SystemTime>>,
}

impl InMemoryObjectCleanupClaimStore {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    async fn set_now_for_tests(&self, now: SystemTime) {
        *self.now_for_tests.write().await = Some(now);
    }

    async fn now(&self) -> SystemTime {
        self.now_for_tests
            .read()
            .await
            .unwrap_or_else(SystemTime::now)
    }
}

#[async_trait]
impl ObjectCleanupClaimStore for InMemoryObjectCleanupClaimStore {
    async fn claim(
        &self,
        request: ObjectCleanupClaimRequest,
    ) -> Result<Option<ObjectCleanupClaim>, VfsError> {
        request.validate()?;
        let now = self.now().await;
        let lease_expires_at =
            now.checked_add(request.lease_duration)
                .ok_or_else(|| VfsError::InvalidArgs {
                    message: "cleanup claim lease expiry overflow".to_string(),
                })?;

        let mut guard = self.inner.write().await;
        let target = request.target();
        let created_at = guard
            .get(&target)
            .map(|existing| existing.created_at)
            .unwrap_or(now);
        let attempts =
            match guard.get(&target) {
                Some(existing) if existing.completed_at.is_some() => return Ok(None),
                Some(existing) if existing.claim.lease_expires_at > now => return Ok(None),
                Some(existing) => existing.claim.attempts.checked_add(1).ok_or_else(|| {
                    VfsError::CorruptStore {
                        message: "cleanup claim attempt counter overflow".to_string(),
                    }
                })?,
                None => 1,
            };

        let claim = ObjectCleanupClaim {
            repo_id: request.repo_id,
            claim_kind: request.claim_kind,
            object_kind: request.object_kind,
            object_id: request.object_id,
            object_key: request.object_key,
            lease_owner: request.lease_owner,
            lease_token: Uuid::new_v4(),
            lease_expires_at,
            attempts,
        };
        guard.insert(
            target,
            InMemoryClaimEntry {
                claim: claim.clone(),
                completed_at: None,
                last_error: None,
                created_at,
                updated_at: now,
            },
        );

        Ok(Some(claim))
    }

    async fn complete(&self, claim: &ObjectCleanupClaim) -> Result<(), VfsError> {
        let completed_at = self.now().await;
        let mut guard = self.inner.write().await;
        let entry = active_entry_for_claim(&mut guard, claim, completed_at)?;
        entry.completed_at = Some(completed_at);
        entry.last_error = None;
        entry.updated_at = completed_at;
        Ok(())
    }

    async fn record_failure(
        &self,
        claim: &ObjectCleanupClaim,
        message: &str,
    ) -> Result<(), VfsError> {
        let failed_at = self.now().await;
        let mut guard = self.inner.write().await;
        let entry = active_entry_for_claim(&mut guard, claim, failed_at)?;
        entry.last_error = Some(message.to_string());
        entry.updated_at = failed_at;
        Ok(())
    }

    async fn list(&self, limit: usize) -> Result<Vec<ObjectCleanupClaimStatus>, VfsError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = self.now().await;
        let guard = self.inner.read().await;
        let mut entries: Vec<&InMemoryClaimEntry> = guard.values().collect();
        entries.sort_by(|left, right| {
            left.completed_at
                .is_some()
                .cmp(&right.completed_at.is_some())
                .then_with(|| left.updated_at.cmp(&right.updated_at))
                .then_with(|| left.claim.repo_id.cmp(&right.claim.repo_id))
                .then_with(|| left.claim.object_key.cmp(&right.claim.object_key))
        });
        Ok(entries
            .into_iter()
            .take(limit)
            .map(|entry| entry.to_status(now))
            .collect())
    }

    async fn counts(&self) -> Result<ObjectCleanupClaimCounts, VfsError> {
        let now = self.now().await;
        let guard = self.inner.read().await;
        let mut counts = ObjectCleanupClaimCounts::default();
        for entry in guard.values() {
            counts.increment(entry.state(now));
        }
        Ok(counts)
    }
}

#[derive(Debug, Clone)]
struct InMemoryClaimEntry {
    claim: ObjectCleanupClaim,
    completed_at: Option<SystemTime>,
    last_error: Option<String>,
    created_at: SystemTime,
    updated_at: SystemTime,
}

impl InMemoryClaimEntry {
    fn state(&self, now: SystemTime) -> ObjectCleanupClaimState {
        classify_cleanup_claim(
            self.completed_at,
            self.last_error.is_some(),
            self.claim.lease_expires_at,
            now,
        )
    }

    fn to_status(&self, now: SystemTime) -> ObjectCleanupClaimStatus {
        ObjectCleanupClaimStatus::for_store(ObjectCleanupClaimStatusInput {
            repo_id: self.claim.repo_id.clone(),
            claim_kind: self.claim.claim_kind,
            object_kind: self.claim.object_kind,
            object_id: self.claim.object_id,
            object_key: self.claim.object_key.clone(),
            state: self.state(now),
            is_stale: cleanup_claim_is_stale(self.completed_at, self.claim.lease_expires_at, now),
            attempts: self.claim.attempts,
            lease_expires_at: self.claim.lease_expires_at,
            completed_at: self.completed_at,
            created_at: self.created_at,
            updated_at: self.updated_at,
            has_last_failure: self.last_error.is_some(),
        })
    }
}

pub fn classify_cleanup_claim(
    completed_at: Option<SystemTime>,
    has_last_failure: bool,
    lease_expires_at: SystemTime,
    now: SystemTime,
) -> ObjectCleanupClaimState {
    if completed_at.is_some() {
        ObjectCleanupClaimState::Completed
    } else if has_last_failure {
        ObjectCleanupClaimState::Failed
    } else if lease_expires_at <= now {
        ObjectCleanupClaimState::StaleActive
    } else {
        ObjectCleanupClaimState::Active
    }
}

pub fn cleanup_claim_is_stale(
    completed_at: Option<SystemTime>,
    lease_expires_at: SystemTime,
    now: SystemTime,
) -> bool {
    completed_at.is_none() && lease_expires_at <= now
}

impl ObjectCleanupClaimRequest {
    fn target(&self) -> ObjectCleanupClaimTarget {
        ObjectCleanupClaimTarget {
            repo_id: self.repo_id.clone(),
            claim_kind: self.claim_kind,
            object_key: self.object_key.clone(),
        }
    }
}

fn active_entry_for_claim<'a>(
    entries: &'a mut BTreeMap<ObjectCleanupClaimTarget, InMemoryClaimEntry>,
    claim: &ObjectCleanupClaim,
    now: SystemTime,
) -> Result<&'a mut InMemoryClaimEntry, VfsError> {
    let Some(entry) = entries.get_mut(&claim.target()) else {
        return Err(stale_cleanup_claim());
    };
    if entry.completed_at.is_some()
        || entry.claim.lease_token != claim.lease_token
        || entry.claim.lease_expires_at <= now
    {
        return Err(stale_cleanup_claim());
    }
    Ok(entry)
}

pub fn validate_lease_owner(owner: &str) -> Result<(), VfsError> {
    if owner.is_empty() || owner.len() > 128 || owner.chars().any(char::is_control) {
        return Err(VfsError::InvalidArgs {
            message: "cleanup claim lease owner must be 1-128 non-control characters".to_string(),
        });
    }
    Ok(())
}

pub fn validate_object_key(key: &str) -> Result<(), VfsError> {
    if key.is_empty() || key.chars().any(char::is_control) {
        return Err(VfsError::InvalidArgs {
            message: "cleanup claim object key must be non-empty and contain no control characters"
                .to_string(),
        });
    }
    Ok(())
}

pub fn validate_canonical_object_key(
    repo_id: &RepoId,
    kind: ObjectKind,
    id: &ObjectId,
    key: &str,
) -> Result<(), VfsError> {
    validate_object_key(key)?;
    let expected = canonical_final_object_key(repo_id, kind, id);
    if key != expected {
        return Err(VfsError::InvalidArgs {
            message: format!(
                "cleanup claim object key must be canonical final object key {expected}"
            ),
        });
    }
    Ok(())
}

pub fn canonical_final_object_key(repo_id: &RepoId, kind: ObjectKind, id: &ObjectId) -> String {
    format!(
        "repos/{}/objects/{}/{}",
        repo_id.as_str(),
        object_kind_segment(kind),
        id.to_hex()
    )
}

fn object_kind_segment(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Blob => "blob",
        ObjectKind::Tree => "tree",
        ObjectKind::Commit => "commit",
    }
}

pub fn stale_cleanup_claim() -> VfsError {
    VfsError::ObjectWriteConflict {
        message: STALE_CLEANUP_CLAIM_MESSAGE.to_string(),
    }
}

pub fn is_stale_cleanup_claim(error: &VfsError) -> bool {
    matches!(
        error,
        VfsError::ObjectWriteConflict { message } if message == STALE_CLEANUP_CLAIM_MESSAGE
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo() -> RepoId {
        RepoId::new("repo_cleanup").unwrap()
    }

    fn object_id(bytes: &[u8]) -> ObjectId {
        ObjectId::from_bytes(bytes)
    }

    fn request(lease_duration: Duration) -> ObjectCleanupClaimRequest {
        let id = object_id(b"repair me");
        ObjectCleanupClaimRequest {
            repo_id: repo(),
            claim_kind: ObjectCleanupClaimKind::FinalObjectMetadataRepair,
            object_kind: ObjectKind::Blob,
            object_id: id,
            object_key: canonical_final_object_key(&repo(), ObjectKind::Blob, &id),
            lease_owner: "worker-a".to_string(),
            lease_duration,
        }
    }

    #[tokio::test]
    async fn first_claim_succeeds_and_active_lease_blocks_duplicate() {
        let store = InMemoryObjectCleanupClaimStore::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.set_now_for_tests(now).await;

        let first = store
            .claim(request(Duration::from_secs(60)))
            .await
            .unwrap()
            .unwrap();
        let duplicate = store.claim(request(Duration::from_secs(60))).await.unwrap();

        assert_eq!(first.attempts, 1);
        assert!(duplicate.is_none());
    }

    #[tokio::test]
    async fn expired_incomplete_claim_can_be_retried() {
        let store = InMemoryObjectCleanupClaimStore::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.set_now_for_tests(now).await;
        let first = store
            .claim(request(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        store.record_failure(&first, "transient").await.unwrap();

        store.set_now_for_tests(now + Duration::from_secs(6)).await;
        let retry = store
            .claim(request(Duration::from_secs(30)))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(retry.attempts, 2);
        assert_ne!(retry.lease_token, first.lease_token);
    }

    #[tokio::test]
    async fn completed_claim_is_not_reacquired() {
        let store = InMemoryObjectCleanupClaimStore::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.set_now_for_tests(now).await;
        let claim = store
            .claim(request(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        store.complete(&claim).await.unwrap();

        store.set_now_for_tests(now + Duration::from_secs(6)).await;
        let retry = store.claim(request(Duration::from_secs(30))).await.unwrap();

        assert!(retry.is_none());
    }

    #[tokio::test]
    async fn stale_token_completion_is_rejected() {
        let store = InMemoryObjectCleanupClaimStore::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.set_now_for_tests(now).await;
        let claim = store
            .claim(request(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        store.set_now_for_tests(now + Duration::from_secs(6)).await;
        let retry = store
            .claim(request(Duration::from_secs(30)))
            .await
            .unwrap()
            .unwrap();

        let err = store
            .complete(&claim)
            .await
            .expect_err("stale claim token should not complete retry lease");
        assert!(is_stale_cleanup_claim(&err));

        store.complete(&retry).await.unwrap();
    }

    #[tokio::test]
    async fn expired_claim_completion_and_failure_are_rejected() {
        let store = InMemoryObjectCleanupClaimStore::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.set_now_for_tests(now).await;
        let claim = store
            .claim(request(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();

        store.set_now_for_tests(now + Duration::from_secs(6)).await;
        let complete_err = store
            .complete(&claim)
            .await
            .expect_err("expired cleanup claim should not complete");
        assert!(is_stale_cleanup_claim(&complete_err));

        let failure_err = store
            .record_failure(&claim, "too late")
            .await
            .expect_err("expired cleanup claim should not record failure");
        assert!(is_stale_cleanup_claim(&failure_err));

        let retry = store
            .claim(request(Duration::from_secs(30)))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(retry.lease_token, claim.lease_token);
    }

    #[tokio::test]
    async fn invalid_claim_requests_are_rejected() {
        let store = InMemoryObjectCleanupClaimStore::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.set_now_for_tests(now).await;

        let mut bad_owner = request(Duration::from_secs(1));
        bad_owner.lease_owner = "bad\nowner".to_string();
        assert!(matches!(
            store.claim(bad_owner).await,
            Err(VfsError::InvalidArgs { .. })
        ));

        let mut bad_key = request(Duration::from_secs(1));
        bad_key.object_key = "repos/repo_cleanup/objects/blob/not-the-id".to_string();
        assert!(matches!(
            store.claim(bad_key).await,
            Err(VfsError::InvalidArgs { .. })
        ));

        assert!(matches!(
            store.claim(request(Duration::ZERO)).await,
            Err(VfsError::InvalidArgs { .. })
        ));
        assert!(matches!(
            store.claim(request(Duration::from_nanos(1))).await,
            Err(VfsError::InvalidArgs { .. })
        ));
    }

    #[tokio::test]
    async fn list_returns_bounded_redacted_cleanup_claim_statuses() {
        let store = InMemoryObjectCleanupClaimStore::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.set_now_for_tests(now).await;
        let failed = store
            .claim(request(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        store
            .record_failure(&failed, "raw storage path /secret/object failed")
            .await
            .unwrap();
        store.set_now_for_tests(now + Duration::from_secs(6)).await;

        let statuses = store.list(1).await.unwrap();

        assert_eq!(statuses.len(), 1);
        let status = &statuses[0];
        assert_eq!(status.repo_id(), &failed.repo_id);
        assert_eq!(status.claim_kind(), failed.claim_kind);
        assert_eq!(status.object_kind(), failed.object_kind);
        assert_eq!(status.object_id(), failed.object_id);
        assert_eq!(status.object_key(), failed.object_key);
        assert_eq!(status.attempts(), 1);
        assert_eq!(
            status.state(),
            ObjectCleanupClaimState::Failed,
            "failed takes precedence over stale_active for expired failed rows"
        );
        assert!(status.is_stale());
        assert!(status.has_last_failure());
        assert!(status.completed_at().is_none());
        assert!(
            !format!("{status:?}").contains("raw storage path /secret/object failed"),
            "status debug output must not expose raw failure text"
        );
        assert!(
            !format!("{status:?}").contains(&failed.lease_token.to_string()),
            "status debug output must not expose lease tokens"
        );
    }

    #[tokio::test]
    async fn counts_classify_active_stale_completed_and_failed_claims() {
        let store = InMemoryObjectCleanupClaimStore::new();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        store.set_now_for_tests(now).await;

        let active = store
            .claim(request_with_id(b"active", Duration::from_secs(60)))
            .await
            .unwrap()
            .unwrap();
        let stale = store
            .claim(request_with_id(b"stale", Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        let completed = store
            .claim(request_with_id(b"completed", Duration::from_secs(60)))
            .await
            .unwrap()
            .unwrap();
        store.complete(&completed).await.unwrap();
        let failed = store
            .claim(request_with_id(b"failed", Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        store.record_failure(&failed, "disk denied").await.unwrap();
        store.set_now_for_tests(now + Duration::from_secs(6)).await;

        let counts = store.counts().await.unwrap();

        assert_eq!(counts.active(), 1);
        assert_eq!(counts.stale_active(), 1);
        assert_eq!(counts.completed(), 1);
        assert_eq!(counts.failed(), 1);
        assert_eq!(counts.total(), 4);
        assert_eq!(active.attempts, 1);
        assert_eq!(stale.attempts, 1);
    }

    fn request_with_id(bytes: &[u8], lease_duration: Duration) -> ObjectCleanupClaimRequest {
        let id = object_id(bytes);
        ObjectCleanupClaimRequest {
            repo_id: repo(),
            claim_kind: ObjectCleanupClaimKind::FinalObjectMetadataRepair,
            object_kind: ObjectKind::Blob,
            object_id: id,
            object_key: canonical_final_object_key(&repo(), ObjectKind::Blob, &id),
            lease_owner: "worker-a".to_string(),
            lease_duration,
        }
    }
}
