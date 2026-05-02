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
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct InMemoryClaimEntry {
    claim: ObjectCleanupClaim,
    completed_at: Option<SystemTime>,
    last_error: Option<String>,
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
}
