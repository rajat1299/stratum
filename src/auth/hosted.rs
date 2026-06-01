use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use std::sync::RwLock;
use uuid::Uuid;

use crate::auth::{Gid, Uid};
use crate::backend::{OrgId, RepoId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostedSessionIdentity {
    pub session_id: Uuid,
    pub org_id: OrgId,
    pub repo_id: RepoId,
    pub uid: Uid,
    pub username: String,
    pub gid: Gid,
    pub groups: Vec<Gid>,
    pub external_identity_id: String,
}

#[derive(Clone)]
pub struct IssuedAccessToken {
    pub identity: HostedSessionIdentity,
    pub raw_secret: String,
    pub token_hash: String,
    pub issued_at_unix: u64,
    pub expires_at_unix: u64,
}

impl fmt::Debug for IssuedAccessToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IssuedAccessToken")
            .field("identity", &self.identity)
            .field("raw_secret", &"<redacted>")
            .field("token_hash", &"<redacted>")
            .field("issued_at_unix", &self.issued_at_unix)
            .field("expires_at_unix", &self.expires_at_unix)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct IssuedRefreshToken {
    pub token: RefreshTokenRecord,
    pub raw_secret: String,
}

impl fmt::Debug for IssuedRefreshToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IssuedRefreshToken")
            .field("token", &self.token)
            .field("raw_secret", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshTokenRecord {
    pub id: Uuid,
    pub family_id: Uuid,
    pub session_id: Uuid,
    pub org_id: OrgId,
    pub repo_id: RepoId,
    pub principal_uid: Uid,
    pub token_hash: String,
    pub version: u64,
    pub issued_at_unix: u64,
    pub expires_at_unix: u64,
    pub rotated_at_unix: Option<u64>,
    pub rotated_to_token_id: Option<Uuid>,
    pub revoked_at_unix: Option<u64>,
    pub reuse_denied_at_unix: Option<u64>,
}

impl fmt::Debug for RefreshTokenRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RefreshTokenRecord")
            .field("id", &self.id)
            .field("family_id", &self.family_id)
            .field("session_id", &self.session_id)
            .field("org_id", &self.org_id)
            .field("repo_id", &self.repo_id)
            .field("principal_uid", &self.principal_uid)
            .field("token_hash", &"<redacted>")
            .field("version", &self.version)
            .field("issued_at_unix", &self.issued_at_unix)
            .field("expires_at_unix", &self.expires_at_unix)
            .field("rotated_at_unix", &self.rotated_at_unix)
            .field("rotated_to_token_id", &self.rotated_to_token_id)
            .field("revoked_at_unix", &self.revoked_at_unix)
            .field("reuse_denied_at_unix", &self.reuse_denied_at_unix)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshTokenFamilyRecord {
    pub id: Uuid,
    pub session_id: Uuid,
    pub org_id: OrgId,
    pub repo_id: RepoId,
    pub principal_uid: Uid,
    pub revoked_at_unix: Option<u64>,
    pub reuse_detected_at_unix: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshTokenError {
    Invalid,
    Expired,
    ReuseDetected,
    FamilyCompromised,
}

#[derive(Debug, Default)]
pub struct InMemoryHostedAuthStore {
    inner: RwLock<InMemoryHostedAuthStoreInner>,
}

#[derive(Debug, Default)]
struct InMemoryHostedAuthStoreInner {
    refresh_families: HashMap<Uuid, RefreshTokenFamilyRecord>,
    refresh_tokens: HashMap<Uuid, RefreshTokenRecord>,
}

impl InMemoryHostedAuthStore {
    pub fn issue_access_token(
        &self,
        identity: &HostedSessionIdentity,
        issued_at_unix: u64,
        expires_at_unix: u64,
    ) -> IssuedAccessToken {
        let raw_secret = generate_hosted_token_secret();
        IssuedAccessToken {
            identity: identity.clone(),
            token_hash: hash_hosted_token_secret(&raw_secret),
            raw_secret,
            issued_at_unix,
            expires_at_unix,
        }
    }

    pub fn issue_refresh_token(
        &self,
        identity: &HostedSessionIdentity,
        issued_at_unix: u64,
        expires_at_unix: u64,
    ) -> IssuedRefreshToken {
        let raw_secret = generate_hosted_token_secret();
        let family = RefreshTokenFamilyRecord {
            id: Uuid::new_v4(),
            session_id: identity.session_id,
            org_id: identity.org_id.clone(),
            repo_id: identity.repo_id.clone(),
            principal_uid: identity.uid,
            revoked_at_unix: None,
            reuse_detected_at_unix: None,
        };
        let token = refresh_token_record(
            &family,
            raw_secret.as_str(),
            1,
            issued_at_unix,
            expires_at_unix,
        );

        let mut guard = self
            .inner
            .write()
            .expect("in-memory hosted auth store lock poisoned");
        guard.refresh_families.insert(family.id, family);
        guard.refresh_tokens.insert(token.id, token.clone());

        IssuedRefreshToken { token, raw_secret }
    }

    pub fn rotate_refresh_token(
        &self,
        raw_secret: &str,
        now_unix: u64,
        successor_expires_at_unix: u64,
    ) -> Result<IssuedRefreshToken, RefreshTokenError> {
        let expected_hash = hash_hosted_token_secret(raw_secret);
        let mut guard = self
            .inner
            .write()
            .expect("in-memory hosted auth store lock poisoned");
        let Some(token_id) = guard.refresh_tokens.iter().find_map(|(id, token)| {
            hosted_token_hash_eq(&token.token_hash, &expected_hash).then_some(*id)
        }) else {
            return Err(RefreshTokenError::Invalid);
        };
        let token = guard
            .refresh_tokens
            .get(&token_id)
            .cloned()
            .expect("matched token id exists");

        let Some(family) = guard.refresh_families.get(&token.family_id).cloned() else {
            return Err(RefreshTokenError::Invalid);
        };
        if family.reuse_detected_at_unix.is_some() || family.revoked_at_unix.is_some() {
            return Err(RefreshTokenError::FamilyCompromised);
        }
        if token.rotated_at_unix.is_some() || token.revoked_at_unix.is_some() {
            mark_refresh_family_reuse_detected(&mut guard, token.id, token.family_id, now_unix);
            return Err(RefreshTokenError::ReuseDetected);
        }
        if now_unix >= token.expires_at_unix {
            return Err(RefreshTokenError::Expired);
        }

        let raw_secret = generate_hosted_token_secret();
        let successor = refresh_token_record(
            &family,
            raw_secret.as_str(),
            token.version.saturating_add(1),
            now_unix,
            successor_expires_at_unix,
        );
        let successor_id = successor.id;
        let current = guard
            .refresh_tokens
            .get_mut(&token_id)
            .expect("matched token id exists for update");
        current.rotated_at_unix = Some(now_unix);
        current.rotated_to_token_id = Some(successor_id);
        guard.refresh_tokens.insert(successor_id, successor.clone());

        Ok(IssuedRefreshToken {
            token: successor,
            raw_secret,
        })
    }

    pub fn revoke_refresh_token(
        &self,
        token_id: Uuid,
        revoked_at_unix: u64,
    ) -> Option<RefreshTokenRecord> {
        let mut guard = self
            .inner
            .write()
            .expect("in-memory hosted auth store lock poisoned");
        let token = guard.refresh_tokens.get_mut(&token_id)?;
        token.revoked_at_unix = Some(revoked_at_unix);
        Some(token.clone())
    }

    pub fn refresh_token(&self, token_id: Uuid) -> Option<RefreshTokenRecord> {
        self.inner
            .read()
            .expect("in-memory hosted auth store lock poisoned")
            .refresh_tokens
            .get(&token_id)
            .cloned()
    }

    pub fn refresh_family(&self, family_id: Uuid) -> Option<RefreshTokenFamilyRecord> {
        self.inner
            .read()
            .expect("in-memory hosted auth store lock poisoned")
            .refresh_families
            .get(&family_id)
            .cloned()
    }

    pub fn refresh_token_count(&self) -> usize {
        self.inner
            .read()
            .expect("in-memory hosted auth store lock poisoned")
            .refresh_tokens
            .len()
    }

    pub fn raw_refresh_secret_is_stored(&self, raw_secret: &str) -> bool {
        self.inner
            .read()
            .expect("in-memory hosted auth store lock poisoned")
            .refresh_tokens
            .values()
            .any(|token| token.token_hash == raw_secret)
    }
}

fn refresh_token_record(
    family: &RefreshTokenFamilyRecord,
    raw_secret: &str,
    version: u64,
    issued_at_unix: u64,
    expires_at_unix: u64,
) -> RefreshTokenRecord {
    RefreshTokenRecord {
        id: Uuid::new_v4(),
        family_id: family.id,
        session_id: family.session_id,
        org_id: family.org_id.clone(),
        repo_id: family.repo_id.clone(),
        principal_uid: family.principal_uid,
        token_hash: hash_hosted_token_secret(raw_secret),
        version,
        issued_at_unix,
        expires_at_unix,
        rotated_at_unix: None,
        rotated_to_token_id: None,
        revoked_at_unix: None,
        reuse_denied_at_unix: None,
    }
}

fn mark_refresh_family_reuse_detected(
    guard: &mut InMemoryHostedAuthStoreInner,
    token_id: Uuid,
    family_id: Uuid,
    now_unix: u64,
) {
    if let Some(token) = guard.refresh_tokens.get_mut(&token_id) {
        token.reuse_denied_at_unix = Some(now_unix);
    }
    if let Some(family) = guard.refresh_families.get_mut(&family_id) {
        family.reuse_detected_at_unix = Some(now_unix);
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right.iter())
        .fold(0u8, |acc, (left, right)| acc | (left ^ right))
        == 0
}

pub(crate) fn hash_hosted_token_secret(raw_secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_secret.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub(crate) fn generate_hosted_token_secret() -> String {
    let mut bytes = [0u8; 24];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub(crate) fn hosted_token_hash_eq(left: &str, right: &str) -> bool {
    constant_time_eq(left.as_bytes(), right.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{OrgId, RepoId};
    use uuid::Uuid;

    #[test]
    fn access_token_debug_redacts_raw_secret() {
        let raw_secret = "raw-access-secret".to_string();
        let issued = IssuedAccessToken {
            identity: test_identity(),
            raw_secret,
            token_hash: hash_hosted_token_secret("raw-access-secret"),
            issued_at_unix: 10,
            expires_at_unix: 70,
        };

        let debug = format!("{issued:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("raw-access-secret"));
        assert!(!debug.contains(&issued.token_hash));
    }

    #[test]
    fn refresh_token_debug_redacts_raw_secret() {
        let raw_secret = "raw-refresh-secret".to_string();
        let issued = IssuedRefreshToken {
            token: RefreshTokenRecord {
                id: Uuid::new_v4(),
                family_id: Uuid::new_v4(),
                session_id: Uuid::new_v4(),
                org_id: OrgId::new("org_debug").unwrap(),
                repo_id: RepoId::new("repo_debug").unwrap(),
                principal_uid: 1000,
                token_hash: hash_hosted_token_secret("raw-refresh-secret"),
                version: 1,
                issued_at_unix: 10,
                expires_at_unix: 70,
                rotated_at_unix: None,
                rotated_to_token_id: None,
                revoked_at_unix: None,
                reuse_denied_at_unix: None,
            },
            raw_secret,
        };

        let debug = format!("{issued:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("raw-refresh-secret"));
        assert!(!debug.contains(&issued.token.token_hash));
    }

    #[test]
    fn refresh_store_issues_hash_only_token() {
        let store = InMemoryHostedAuthStore::default();
        let issued = store.issue_refresh_token(&test_identity(), 10, 70);

        assert!(issued.token.token_hash != issued.raw_secret);
        assert!(issued.token.token_hash == hash_hosted_token_secret(&issued.raw_secret));
        assert_eq!(issued.token.token_hash.len(), 64);
        assert!(
            issued
                .token
                .token_hash
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        );
        assert_eq!(store.refresh_token_count(), 1);
        assert!(!store.raw_refresh_secret_is_stored(&issued.raw_secret));
    }

    #[test]
    fn refresh_store_rotates_and_invalidates_previous_token() {
        let store = InMemoryHostedAuthStore::default();
        let issued = store.issue_refresh_token(&test_identity(), 10, 70);

        let rotated = store
            .rotate_refresh_token(&issued.raw_secret, 20, 80)
            .expect("rotation succeeds");

        assert!(rotated.raw_secret != issued.raw_secret);
        assert_eq!(rotated.token.family_id, issued.token.family_id);
        assert_eq!(rotated.token.version, issued.token.version + 1);
        let previous = store
            .refresh_token(issued.token.id)
            .expect("previous token exists");
        assert_eq!(previous.rotated_at_unix, Some(20));
        assert_eq!(previous.rotated_to_token_id, Some(rotated.token.id));
        assert_eq!(
            store.rotate_refresh_token(&issued.raw_secret, 21, 90),
            Err(RefreshTokenError::ReuseDetected)
        );
    }

    #[test]
    fn refresh_store_rejects_revoked_expired_and_stale_tokens() {
        let store = InMemoryHostedAuthStore::default();
        let revoked = store.issue_refresh_token(&test_identity(), 10, 70);
        let expired = store.issue_refresh_token(&test_identity(), 10, 20);
        let rotated = store.issue_refresh_token(&test_identity(), 10, 70);

        store.revoke_refresh_token(revoked.token.id, 15);
        assert_eq!(
            store.rotate_refresh_token(&revoked.raw_secret, 16, 80),
            Err(RefreshTokenError::ReuseDetected)
        );
        assert_eq!(
            store
                .refresh_family(revoked.token.family_id)
                .expect("revoked token family exists")
                .reuse_detected_at_unix,
            Some(16)
        );
        assert_eq!(
            store.rotate_refresh_token(&expired.raw_secret, 20, 80),
            Err(RefreshTokenError::Expired)
        );

        let successor = store
            .rotate_refresh_token(&rotated.raw_secret, 30, 90)
            .expect("initial rotation succeeds");
        assert_eq!(
            store.rotate_refresh_token(&rotated.raw_secret, 31, 100),
            Err(RefreshTokenError::ReuseDetected)
        );
        assert_eq!(store.refresh_token_count(), 4);
        assert_eq!(
            store
                .refresh_family(rotated.token.family_id)
                .expect("family exists")
                .reuse_detected_at_unix,
            Some(31)
        );
        assert_eq!(
            store
                .refresh_token(rotated.token.id)
                .expect("rotated token exists")
                .reuse_denied_at_unix,
            Some(31)
        );
        assert!(store.refresh_token(successor.token.id).is_some());
    }

    #[test]
    fn refresh_store_reuse_marks_family_compromised_without_issuing_successor() {
        let store = InMemoryHostedAuthStore::default();
        let issued = store.issue_refresh_token(&test_identity(), 10, 70);
        let successor = store
            .rotate_refresh_token(&issued.raw_secret, 20, 80)
            .expect("initial rotation succeeds");
        let token_count_before_reuse = store.refresh_token_count();

        assert_eq!(
            store.rotate_refresh_token(&issued.raw_secret, 21, 90),
            Err(RefreshTokenError::ReuseDetected)
        );

        assert_eq!(store.refresh_token_count(), token_count_before_reuse);
        assert_eq!(
            store
                .refresh_family(issued.token.family_id)
                .expect("family exists")
                .reuse_detected_at_unix,
            Some(21)
        );
        assert_eq!(
            store.rotate_refresh_token(&successor.raw_secret, 22, 100),
            Err(RefreshTokenError::FamilyCompromised)
        );
        assert_eq!(store.refresh_token_count(), token_count_before_reuse);
    }

    #[test]
    fn expired_stale_refresh_reuse_still_marks_family_compromised() {
        let store = InMemoryHostedAuthStore::default();
        let issued = store.issue_refresh_token(&test_identity(), 10, 30);
        let _successor = store
            .rotate_refresh_token(&issued.raw_secret, 20, 40)
            .expect("initial rotation succeeds");

        assert_eq!(
            store.rotate_refresh_token(&issued.raw_secret, 31, 50),
            Err(RefreshTokenError::ReuseDetected)
        );
        assert_eq!(
            store
                .refresh_family(issued.token.family_id)
                .expect("family exists")
                .reuse_detected_at_unix,
            Some(31)
        );
    }

    fn test_identity() -> HostedSessionIdentity {
        HostedSessionIdentity {
            session_id: Uuid::new_v4(),
            org_id: OrgId::new("org_demo").unwrap(),
            repo_id: RepoId::new("repo_demo").unwrap(),
            uid: 1000,
            username: "demo-user".to_string(),
            gid: 100,
            groups: vec![100, 101],
            external_identity_id: "oidc:demo:user".to_string(),
        }
    }
}
