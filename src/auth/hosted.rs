use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::sync::RwLock;
use uuid::Uuid;

use crate::auth::{Gid, Uid};
use crate::backend::{OrgId, RepoId};

pub type SharedHostedAuthStore = std::sync::Arc<InMemoryHostedAuthStore>;

#[derive(Clone, Copy)]
pub struct HostedOidcVerificationRequest<'a> {
    pub provider: &'a str,
    pub authorization_code: &'a str,
    pub redirect_uri: Option<&'a str>,
}

impl fmt::Debug for HostedOidcVerificationRequest<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostedOidcVerificationRequest")
            .field("provider", &self.provider)
            .field("authorization_code", &"<redacted>")
            .field("redirect_uri", &self.redirect_uri.map(|_| "<redacted>"))
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedHostedOidcClaims {
    pub org_id: OrgId,
    pub repo_id: RepoId,
    pub uid: Uid,
    pub username: String,
    pub gid: Gid,
    pub groups: Vec<Gid>,
    pub external_identity_id: String,
}

#[derive(Clone, PartialEq, Eq)]
pub enum HostedOidcVerificationError {
    Disabled,
    ProviderDenied { message: String },
}

impl fmt::Debug for HostedOidcVerificationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => f.write_str("Disabled"),
            Self::ProviderDenied { .. } => f
                .debug_struct("ProviderDenied")
                .field("message", &"<redacted>")
                .finish(),
        }
    }
}

pub trait HostedOidcVerifier: Send + Sync {
    fn verify(
        &self,
        request: HostedOidcVerificationRequest<'_>,
    ) -> Result<VerifiedHostedOidcClaims, HostedOidcVerificationError>;
}

#[derive(Debug, Default)]
struct DisabledHostedOidcVerifier;

impl HostedOidcVerifier for DisabledHostedOidcVerifier {
    fn verify(
        &self,
        request: HostedOidcVerificationRequest<'_>,
    ) -> Result<VerifiedHostedOidcClaims, HostedOidcVerificationError> {
        let _ = (
            request.provider,
            request.authorization_code,
            request.redirect_uri,
        );
        Err(HostedOidcVerificationError::Disabled)
    }
}

#[derive(Clone, Copy)]
pub struct HostedSamlVerificationRequest<'a> {
    pub provider: &'a str,
    pub saml_response: &'a str,
    pub relay_state: Option<&'a str>,
    pub org_id: &'a OrgId,
    pub repo_id: &'a RepoId,
}

impl fmt::Debug for HostedSamlVerificationRequest<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostedSamlVerificationRequest")
            .field("provider", &self.provider)
            .field("saml_response", &"<redacted>")
            .field("relay_state", &self.relay_state.map(|_| "<redacted>"))
            .field("org_id", &self.org_id)
            .field("repo_id", &self.repo_id)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SamlBinding {
    HttpPost,
    HttpRedirect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SamlGroupMapping {
    pub external_group: String,
    pub gid: Gid,
}

#[derive(Clone, PartialEq, Eq)]
pub struct SamlProviderMetadata {
    pub provider_key: String,
    pub org_id: OrgId,
    pub repo_id: RepoId,
    pub idp_entity_id: String,
    pub sp_entity_id: String,
    pub acs_url: String,
    pub audience: String,
    pub binding: SamlBinding,
    pub signing_certificate_ref: String,
    pub group_attribute_name: String,
    pub required_external_group: Option<String>,
    pub group_mappings: Vec<SamlGroupMapping>,
}

impl fmt::Debug for SamlProviderMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SamlProviderMetadata")
            .field("provider_key", &self.provider_key)
            .field("org_id", &self.org_id)
            .field("repo_id", &self.repo_id)
            .field("idp_entity_id", &"<redacted>")
            .field("sp_entity_id", &"<redacted>")
            .field("acs_url", &"<redacted>")
            .field("audience", &"<redacted>")
            .field("binding", &self.binding)
            .field("signing_certificate_ref", &"<redacted>")
            .field("group_attribute_name", &self.group_attribute_name)
            .field(
                "required_external_group_present",
                &self.required_external_group.is_some(),
            )
            .field("group_mapping_count", &self.group_mappings.len())
            .finish()
    }
}

impl SamlProviderMetadata {
    pub fn validate(&self) -> Result<(), HostedSamlVerificationError> {
        if !valid_saml_key(&self.provider_key)
            || !valid_saml_entity_id(&self.idp_entity_id)
            || !valid_saml_entity_id(&self.sp_entity_id)
            || !valid_saml_entity_id(&self.audience)
            || !valid_saml_acs_url(&self.acs_url)
            || self.binding != SamlBinding::HttpPost
            || !valid_saml_cert_ref(&self.signing_certificate_ref)
            || !valid_saml_key(&self.group_attribute_name)
        {
            return Err(HostedSamlVerificationError::InvalidMetadata);
        }
        if self
            .required_external_group
            .as_deref()
            .is_some_and(|group| !valid_saml_group_name(group))
        {
            return Err(HostedSamlVerificationError::InvalidMetadata);
        }
        if self
            .group_mappings
            .iter()
            .any(|mapping| !valid_saml_group_name(&mapping.external_group))
        {
            return Err(HostedSamlVerificationError::InvalidMetadata);
        }
        Ok(())
    }

    pub fn map_external_groups(
        &self,
        external_groups: &[String],
    ) -> Result<Vec<Gid>, HostedSamlVerificationError> {
        self.validate()?;
        if self
            .required_external_group
            .as_ref()
            .is_some_and(|required| !external_groups.iter().any(|group| group == required))
        {
            return Err(HostedSamlVerificationError::GroupMappingMismatch);
        }

        let mapped_groups = self
            .group_mappings
            .iter()
            .filter(|mapping| {
                external_groups
                    .iter()
                    .any(|group| group == &mapping.external_group)
            })
            .map(|mapping| mapping.gid)
            .collect::<Vec<_>>();
        if mapped_groups.is_empty() {
            return Err(HostedSamlVerificationError::GroupMappingMismatch);
        }
        Ok(mapped_groups)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct HostedSamlAssertion {
    pub provider_key: String,
    pub org_id: OrgId,
    pub repo_id: RepoId,
    pub uid: Uid,
    pub username: String,
    pub gid: Gid,
    pub external_identity_id: String,
    pub assertion_id: String,
    pub not_before_unix: u64,
    pub expires_at_unix: u64,
    pub audience: String,
    pub acs_url: String,
    pub issuer_entity_id: String,
    pub name_id: String,
    pub external_groups: Vec<String>,
}

impl fmt::Debug for HostedSamlAssertion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostedSamlAssertion")
            .field("provider_key", &self.provider_key)
            .field("org_id", &self.org_id)
            .field("repo_id", &self.repo_id)
            .field("uid", &self.uid)
            .field("username", &self.username)
            .field("gid", &self.gid)
            .field("external_identity_id", &"<redacted>")
            .field("assertion_id", &"<redacted>")
            .field("not_before_unix", &self.not_before_unix)
            .field("expires_at_unix", &self.expires_at_unix)
            .field("audience", &"<redacted>")
            .field("acs_url", &"<redacted>")
            .field("issuer_entity_id", &"<redacted>")
            .field("name_id", &"<redacted>")
            .field("external_group_count", &self.external_groups.len())
            .finish()
    }
}

impl HostedSamlAssertion {
    pub fn verify_against(
        &self,
        metadata: &SamlProviderMetadata,
        now_unix: u64,
    ) -> Result<VerifiedHostedSamlClaims, HostedSamlVerificationError> {
        metadata.validate()?;
        if now_unix >= self.expires_at_unix {
            return Err(HostedSamlVerificationError::AssertionExpired);
        }
        if now_unix < self.not_before_unix {
            return Err(HostedSamlVerificationError::AssertionNotYetValid);
        }
        if self.org_id != metadata.org_id || self.repo_id != metadata.repo_id {
            return Err(HostedSamlVerificationError::TenantMismatch);
        }
        if self.provider_key != metadata.provider_key
            || self.audience != metadata.audience
            || self.acs_url != metadata.acs_url
            || self.issuer_entity_id != metadata.idp_entity_id
            || !valid_saml_assertion_id(&self.assertion_id)
            || !valid_saml_key(&self.username)
            || !valid_saml_external_identity_id(&self.external_identity_id)
            || self
                .external_groups
                .iter()
                .any(|group| !valid_saml_group_name(group))
        {
            return Err(HostedSamlVerificationError::InvalidAssertion);
        }

        Ok(VerifiedHostedSamlClaims {
            org_id: self.org_id.clone(),
            repo_id: self.repo_id.clone(),
            uid: self.uid,
            username: self.username.clone(),
            gid: self.gid,
            groups: metadata.map_external_groups(&self.external_groups)?,
            external_identity_id: self.external_identity_id.clone(),
            assertion_id_hash: hash_saml_assertion_id(&self.assertion_id),
            not_before_unix: self.not_before_unix,
            expires_at_unix: self.expires_at_unix,
            audience: self.audience.clone(),
            acs_url: self.acs_url.clone(),
            issuer_entity_id: self.issuer_entity_id.clone(),
            provider_key: self.provider_key.clone(),
        })
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct VerifiedHostedSamlClaims {
    pub org_id: OrgId,
    pub repo_id: RepoId,
    pub uid: Uid,
    pub username: String,
    pub gid: Gid,
    pub groups: Vec<Gid>,
    pub external_identity_id: String,
    pub assertion_id_hash: String,
    pub not_before_unix: u64,
    pub expires_at_unix: u64,
    pub audience: String,
    pub acs_url: String,
    pub issuer_entity_id: String,
    pub provider_key: String,
}

impl fmt::Debug for VerifiedHostedSamlClaims {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedHostedSamlClaims")
            .field("org_id", &self.org_id)
            .field("repo_id", &self.repo_id)
            .field("uid", &self.uid)
            .field("username", &self.username)
            .field("gid", &self.gid)
            .field("group_count", &self.groups.len())
            .field("external_identity_id", &"<redacted>")
            .field("assertion_id_hash", &"<redacted>")
            .field("not_before_unix", &self.not_before_unix)
            .field("expires_at_unix", &self.expires_at_unix)
            .field("audience", &"<redacted>")
            .field("acs_url", &"<redacted>")
            .field("issuer_entity_id", &"<redacted>")
            .field("provider_key", &self.provider_key)
            .finish()
    }
}

impl VerifiedHostedSamlClaims {
    fn assertion_replay_key(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"saml-assertion-replay:");
        hasher.update(self.provider_key.as_bytes());
        hasher.update(b"\0");
        hasher.update(self.org_id.to_string().as_bytes());
        hasher.update(b"\0");
        hasher.update(self.repo_id.to_string().as_bytes());
        hasher.update(b"\0");
        hasher.update(self.issuer_entity_id.as_bytes());
        hasher.update(b"\0");
        hasher.update(self.assertion_id_hash.as_bytes());
        format!("{:x}", hasher.finalize())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct HostedSamlProviderDenial;

impl fmt::Debug for HostedSamlProviderDenial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HostedSamlProviderDenial(<redacted>)")
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum HostedSamlVerificationError {
    Disabled,
    InvalidMetadata,
    InvalidAssertion,
    AssertionExpired,
    AssertionNotYetValid,
    AssertionReplay,
    GroupMappingMismatch,
    TenantMismatch,
    ProviderDenied(HostedSamlProviderDenial),
}

impl fmt::Debug for HostedSamlVerificationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => f.write_str("Disabled"),
            Self::InvalidMetadata => f.write_str("InvalidMetadata"),
            Self::InvalidAssertion => f.write_str("InvalidAssertion"),
            Self::AssertionExpired => f.write_str("AssertionExpired"),
            Self::AssertionNotYetValid => f.write_str("AssertionNotYetValid"),
            Self::AssertionReplay => f.write_str("AssertionReplay"),
            Self::GroupMappingMismatch => f.write_str("GroupMappingMismatch"),
            Self::TenantMismatch => f.write_str("TenantMismatch"),
            Self::ProviderDenied(_) => f.write_str("ProviderDenied(<redacted>)"),
        }
    }
}

pub trait HostedSamlVerifier: Send + Sync {
    fn verify(
        &self,
        request: HostedSamlVerificationRequest<'_>,
    ) -> Result<VerifiedHostedSamlClaims, HostedSamlVerificationError>;
}

#[derive(Debug, Default)]
struct DisabledHostedSamlVerifier;

impl HostedSamlVerifier for DisabledHostedSamlVerifier {
    fn verify(
        &self,
        request: HostedSamlVerificationRequest<'_>,
    ) -> Result<VerifiedHostedSamlClaims, HostedSamlVerificationError> {
        let _ = (
            request.provider,
            request.saml_response,
            request.relay_state,
            request.org_id,
            request.repo_id,
        );
        Err(HostedSamlVerificationError::Disabled)
    }
}

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

pub struct InMemoryHostedAuthStore {
    inner: RwLock<InMemoryHostedAuthStoreInner>,
    oidc_verifier: RwLock<Arc<dyn HostedOidcVerifier>>,
    saml_verifier: RwLock<Arc<dyn HostedSamlVerifier>>,
}

impl fmt::Debug for InMemoryHostedAuthStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let guard = self
            .inner
            .read()
            .expect("in-memory hosted auth store lock poisoned");
        f.debug_struct("InMemoryHostedAuthStore")
            .field("access_token_count", &guard.access_tokens.len())
            .field("refresh_family_count", &guard.refresh_families.len())
            .field("refresh_token_count", &guard.refresh_tokens.len())
            .field(
                "saml_assertion_replay_count",
                &guard.saml_assertion_replay.len(),
            )
            .finish()
    }
}

impl Default for InMemoryHostedAuthStore {
    fn default() -> Self {
        Self {
            inner: RwLock::new(InMemoryHostedAuthStoreInner::default()),
            oidc_verifier: RwLock::new(Arc::new(DisabledHostedOidcVerifier)),
            saml_verifier: RwLock::new(Arc::new(DisabledHostedSamlVerifier)),
        }
    }
}

#[derive(Default)]
struct InMemoryHostedAuthStoreInner {
    access_tokens: HashMap<String, StoredAccessTokenRecord>,
    session_identities: HashMap<Uuid, HostedSessionIdentity>,
    refresh_families: HashMap<Uuid, RefreshTokenFamilyRecord>,
    refresh_tokens: HashMap<Uuid, RefreshTokenRecord>,
    saml_assertion_replay: HashMap<String, u64>,
    saml_external_identities: HashSet<SamlExternalIdentityBinding>,
    scim_clients: Vec<ScimClientConfig>,
    scim_users: HashMap<ScimUserKey, ScimUserRecord>,
    scim_groups: HashMap<ScimGroupKey, ScimGroupRecord>,
    scim_group_members: HashMap<ScimGroupMemberKey, ScimGroupMemberRecord>,
}

/// Natural idempotency key for SCIM users: tenant + client + external id hash.
#[derive(Clone, PartialEq, Eq, Hash)]
struct ScimUserKey {
    org_id: OrgId,
    repo_id: RepoId,
    client_provider_key: String,
    external_id_hash: String,
}

/// Natural idempotency key for SCIM groups: tenant + client + external id hash.
#[derive(Clone, PartialEq, Eq, Hash)]
struct ScimGroupKey {
    org_id: OrgId,
    repo_id: RepoId,
    client_provider_key: String,
    external_id_hash: String,
}

/// Natural idempotency key for SCIM group memberships: group + principal.
#[derive(Clone, PartialEq, Eq, Hash)]
struct ScimGroupMemberKey {
    group_id: Uuid,
    principal_uid: Uid,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SamlExternalIdentityBinding {
    org_id: OrgId,
    repo_id: RepoId,
    provider_key: String,
    external_identity_id: String,
    principal_uid: Uid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredAccessTokenRecord {
    identity: HostedSessionIdentity,
    expires_at_unix: u64,
    revoked_at_unix: Option<u64>,
}

/// Tenant-scoped SCIM provisioning client credential.
///
/// Only the SHA-256 hash of the bearer token is retained; the raw token is
/// never stored. The manual `Debug` redacts `token_hash` so credentials never
/// leak into logs.
#[derive(Clone, PartialEq, Eq)]
pub struct ScimClientConfig {
    pub provider_key: String,
    pub org_id: OrgId,
    pub repo_id: RepoId,
    pub token_hash: String,
    pub enabled: bool,
}

impl fmt::Debug for ScimClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScimClientConfig")
            .field("provider_key", &self.provider_key)
            .field("org_id", &self.org_id)
            .field("repo_id", &self.repo_id)
            .field("token_hash", &"<redacted>")
            .field("enabled", &self.enabled)
            .finish()
    }
}

/// Resolved, authenticated SCIM client scoped to a single tenant.
///
/// Carries no secrets: it only names the tenant and provider whose credential
/// the caller presented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScimClientContext {
    pub org_id: OrgId,
    pub repo_id: RepoId,
    pub provider_key: String,
}

/// Reason a SCIM client failed authentication. Variants are redaction-safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScimAuthError {
    Disabled,
    Unauthorized,
    TenantMismatch,
}

/// A SCIM user binding an external SCIM identity to an existing tenant principal.
///
/// The manual `Debug` redacts `external_id_hash` so external identifiers never
/// surface in logs.
#[derive(Clone, PartialEq, Eq)]
pub struct ScimUserRecord {
    pub id: Uuid,
    pub org_id: OrgId,
    pub repo_id: RepoId,
    pub client_provider_key: String,
    pub principal_uid: Uid,
    pub external_id_hash: String,
    pub username_hint: Option<String>,
    pub active: bool,
    pub created_at_unix: u64,
    pub updated_at_unix: u64,
    pub disabled_at_unix: Option<u64>,
}

impl fmt::Debug for ScimUserRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScimUserRecord")
            .field("id", &self.id)
            .field("org_id", &self.org_id)
            .field("repo_id", &self.repo_id)
            .field("client_provider_key", &self.client_provider_key)
            .field("principal_uid", &self.principal_uid)
            .field("external_id_hash", &"<redacted>")
            .field("username_hint", &self.username_hint)
            .field("active", &self.active)
            .field("created_at_unix", &self.created_at_unix)
            .field("updated_at_unix", &self.updated_at_unix)
            .field("disabled_at_unix", &self.disabled_at_unix)
            .finish()
    }
}

/// A SCIM group mapping an external SCIM group to a local gid for a tenant.
///
/// The manual `Debug` redacts `external_id_hash`.
#[derive(Clone, PartialEq, Eq)]
pub struct ScimGroupRecord {
    pub id: Uuid,
    pub org_id: OrgId,
    pub repo_id: RepoId,
    pub client_provider_key: String,
    pub local_gid: Gid,
    pub external_id_hash: String,
    pub display_name: Option<String>,
    pub active: bool,
    pub created_at_unix: u64,
    pub updated_at_unix: u64,
    pub disabled_at_unix: Option<u64>,
}

impl fmt::Debug for ScimGroupRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScimGroupRecord")
            .field("id", &self.id)
            .field("org_id", &self.org_id)
            .field("repo_id", &self.repo_id)
            .field("client_provider_key", &self.client_provider_key)
            .field("local_gid", &self.local_gid)
            .field("external_id_hash", &"<redacted>")
            .field("display_name", &self.display_name)
            .field("active", &self.active)
            .field("created_at_unix", &self.created_at_unix)
            .field("updated_at_unix", &self.updated_at_unix)
            .field("disabled_at_unix", &self.disabled_at_unix)
            .finish()
    }
}

/// Membership of an existing tenant principal in a SCIM group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScimGroupMemberRecord {
    pub id: Uuid,
    pub org_id: OrgId,
    pub repo_id: RepoId,
    pub group_id: Uuid,
    pub principal_uid: Uid,
    pub active: bool,
    pub created_at_unix: u64,
    pub updated_at_unix: u64,
    pub disabled_at_unix: Option<u64>,
}

/// Bounded counts of hosted credentials revoked during deprovisioning.
///
/// Carries no secrets; only the number of records newly transitioned to a
/// revoked state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScimRevocationSummary {
    pub access_tokens_revoked: usize,
    pub refresh_tokens_revoked: usize,
    pub refresh_families_revoked: usize,
}

/// Outcome of an idempotent SCIM user provisioning call.
///
/// `created` is `true` only when this call inserted the binding, so callers can
/// emit a provisioning audit exactly once without a check-then-act race.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScimUserProvisionOutcome {
    pub record: ScimUserRecord,
    pub created: bool,
}

/// Outcome of an idempotent SCIM group provisioning call. `created` is `true`
/// only when this call inserted the group mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScimGroupProvisionOutcome {
    pub record: ScimGroupRecord,
    pub created: bool,
}

impl InMemoryHostedAuthStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn verify_oidc_login(
        &self,
        request: HostedOidcVerificationRequest<'_>,
    ) -> Result<VerifiedHostedOidcClaims, HostedOidcVerificationError> {
        self.oidc_verifier
            .read()
            .expect("in-memory hosted auth verifier lock poisoned")
            .verify(request)
    }

    pub fn verify_saml_login(
        &self,
        request: HostedSamlVerificationRequest<'_>,
    ) -> Result<VerifiedHostedSamlClaims, HostedSamlVerificationError> {
        self.saml_verifier
            .read()
            .expect("in-memory hosted auth verifier lock poisoned")
            .verify(request)
    }

    pub fn verify_saml_login_once(
        &self,
        request: HostedSamlVerificationRequest<'_>,
        now_unix: u64,
    ) -> Result<VerifiedHostedSamlClaims, HostedSamlVerificationError> {
        let claims = self.verify_saml_login(request)?;
        self.record_saml_assertion_replay_for_claims(&claims, now_unix)?;
        Ok(claims)
    }

    #[cfg(test)]
    pub fn set_oidc_verifier_for_test(&self, verifier: Arc<dyn HostedOidcVerifier>) {
        *self
            .oidc_verifier
            .write()
            .expect("in-memory hosted auth verifier lock poisoned") = verifier;
    }

    #[cfg(test)]
    pub fn set_saml_verifier_for_test(&self, verifier: Arc<dyn HostedSamlVerifier>) {
        *self
            .saml_verifier
            .write()
            .expect("in-memory hosted auth verifier lock poisoned") = verifier;
    }

    pub fn record_saml_assertion_replay_for_claims(
        &self,
        claims: &VerifiedHostedSamlClaims,
        now_unix: u64,
    ) -> Result<(), HostedSamlVerificationError> {
        self.record_saml_assertion_replay(
            &claims.assertion_replay_key(),
            claims.expires_at_unix,
            now_unix,
        )
    }

    pub fn saml_external_identity_is_bound(&self, claims: &VerifiedHostedSamlClaims) -> bool {
        let binding = SamlExternalIdentityBinding {
            org_id: claims.org_id.clone(),
            repo_id: claims.repo_id.clone(),
            provider_key: claims.provider_key.clone(),
            external_identity_id: claims.external_identity_id.clone(),
            principal_uid: claims.uid,
        };
        self.inner
            .read()
            .expect("in-memory hosted auth store lock poisoned")
            .saml_external_identities
            .contains(&binding)
    }

    #[cfg(test)]
    pub fn bind_saml_external_identity_for_test(&self, claims: &VerifiedHostedSamlClaims) {
        self.inner
            .write()
            .expect("in-memory hosted auth store lock poisoned")
            .saml_external_identities
            .insert(SamlExternalIdentityBinding {
                org_id: claims.org_id.clone(),
                repo_id: claims.repo_id.clone(),
                provider_key: claims.provider_key.clone(),
                external_identity_id: claims.external_identity_id.clone(),
                principal_uid: claims.uid,
            });
    }

    /// Authenticate a SCIM client by presented token hash and requested tenant.
    ///
    /// Returns the resolved tenant-scoped client context on success. Fails
    /// closed: an unknown token hash is `Unauthorized`, a matching but disabled
    /// client is `Disabled`, and a matching token bound to a different tenant is
    /// `TenantMismatch`. Never falls back to any default identity.
    pub fn authenticate_scim_client(
        &self,
        presented_token_hash: &str,
        org_id: &OrgId,
        repo_id: &RepoId,
    ) -> Result<ScimClientContext, ScimAuthError> {
        let guard = self
            .inner
            .read()
            .expect("in-memory hosted auth store lock poisoned");
        let Some(client) = guard
            .scim_clients
            .iter()
            .find(|client| hosted_token_hash_eq(&client.token_hash, presented_token_hash))
        else {
            return Err(ScimAuthError::Unauthorized);
        };
        if !client.enabled {
            return Err(ScimAuthError::Disabled);
        }
        if &client.org_id != org_id || &client.repo_id != repo_id {
            return Err(ScimAuthError::TenantMismatch);
        }
        Ok(ScimClientContext {
            org_id: client.org_id.clone(),
            repo_id: client.repo_id.clone(),
            provider_key: client.provider_key.clone(),
        })
    }

    /// Idempotently provision (bind) a SCIM user for an existing tenant principal.
    ///
    /// Keyed on `(org, repo, provider, external_id_hash)`: a retry returns the
    /// existing record without creating a duplicate. The principal is expected
    /// to already exist for the tenant; this only records the external binding.
    pub fn provision_scim_user(
        &self,
        client: &ScimClientContext,
        principal_uid: Uid,
        external_id_hash: &str,
        username_hint: Option<&str>,
        now_unix: u64,
    ) -> ScimUserProvisionOutcome {
        let key = ScimUserKey {
            org_id: client.org_id.clone(),
            repo_id: client.repo_id.clone(),
            client_provider_key: client.provider_key.clone(),
            external_id_hash: external_id_hash.to_string(),
        };
        let mut guard = self
            .inner
            .write()
            .expect("in-memory hosted auth store lock poisoned");
        match guard.scim_users.entry(key) {
            std::collections::hash_map::Entry::Occupied(entry) => ScimUserProvisionOutcome {
                record: entry.get().clone(),
                created: false,
            },
            std::collections::hash_map::Entry::Vacant(entry) => {
                let record = ScimUserRecord {
                    id: Uuid::new_v4(),
                    org_id: client.org_id.clone(),
                    repo_id: client.repo_id.clone(),
                    client_provider_key: client.provider_key.clone(),
                    principal_uid,
                    external_id_hash: external_id_hash.to_string(),
                    username_hint: username_hint.map(str::to_string),
                    active: true,
                    created_at_unix: now_unix,
                    updated_at_unix: now_unix,
                    disabled_at_unix: None,
                };
                ScimUserProvisionOutcome {
                    record: entry.insert(record).clone(),
                    created: true,
                }
            }
        }
    }

    /// Update bounded attributes and/or the `active` flag of a SCIM user.
    ///
    /// Looks up the user by tenant + external id hash. Never re-binds to a
    /// different principal or tenant. Returns `None` if the user is unknown.
    pub fn update_scim_user(
        &self,
        client: &ScimClientContext,
        external_id_hash: &str,
        username_hint: Option<&str>,
        active: Option<bool>,
        now_unix: u64,
    ) -> Option<ScimUserRecord> {
        let key = ScimUserKey {
            org_id: client.org_id.clone(),
            repo_id: client.repo_id.clone(),
            client_provider_key: client.provider_key.clone(),
            external_id_hash: external_id_hash.to_string(),
        };
        let mut guard = self
            .inner
            .write()
            .expect("in-memory hosted auth store lock poisoned");
        let user = guard.scim_users.get_mut(&key)?;
        if let Some(username_hint) = username_hint {
            user.username_hint = Some(username_hint.to_string());
        }
        if let Some(active) = active {
            if active {
                user.active = true;
                user.disabled_at_unix = None;
            } else if user.active {
                user.active = false;
                user.disabled_at_unix = Some(now_unix);
            }
        }
        user.updated_at_unix = now_unix;
        Some(user.clone())
    }

    /// Deactivate a SCIM user, returning the record (including `principal_uid`)
    /// so the caller can sequence audit and hosted-access revocation.
    ///
    /// Idempotent: re-deactivating an already-inactive user is a no-op that
    /// returns the unchanged record. Returns `None` if the user is unknown.
    pub fn deactivate_scim_user(
        &self,
        client: &ScimClientContext,
        external_id_hash: &str,
        now_unix: u64,
    ) -> Option<ScimUserRecord> {
        let key = ScimUserKey {
            org_id: client.org_id.clone(),
            repo_id: client.repo_id.clone(),
            client_provider_key: client.provider_key.clone(),
            external_id_hash: external_id_hash.to_string(),
        };
        let mut guard = self
            .inner
            .write()
            .expect("in-memory hosted auth store lock poisoned");
        let user = guard.scim_users.get_mut(&key)?;
        if user.active {
            user.active = false;
            user.disabled_at_unix = Some(now_unix);
            user.updated_at_unix = now_unix;
        }
        Some(user.clone())
    }

    /// Idempotently provision a SCIM group mapping to a local gid.
    ///
    /// Keyed on `(org, repo, provider, external_id_hash)`: a retry returns the
    /// existing record without creating a duplicate.
    pub fn provision_scim_group(
        &self,
        client: &ScimClientContext,
        local_gid: Gid,
        external_id_hash: &str,
        display_name: Option<&str>,
        now_unix: u64,
    ) -> ScimGroupProvisionOutcome {
        let key = ScimGroupKey {
            org_id: client.org_id.clone(),
            repo_id: client.repo_id.clone(),
            client_provider_key: client.provider_key.clone(),
            external_id_hash: external_id_hash.to_string(),
        };
        let mut guard = self
            .inner
            .write()
            .expect("in-memory hosted auth store lock poisoned");
        match guard.scim_groups.entry(key) {
            std::collections::hash_map::Entry::Occupied(entry) => ScimGroupProvisionOutcome {
                record: entry.get().clone(),
                created: false,
            },
            std::collections::hash_map::Entry::Vacant(entry) => {
                let record = ScimGroupRecord {
                    id: Uuid::new_v4(),
                    org_id: client.org_id.clone(),
                    repo_id: client.repo_id.clone(),
                    client_provider_key: client.provider_key.clone(),
                    local_gid,
                    external_id_hash: external_id_hash.to_string(),
                    display_name: display_name.map(str::to_string),
                    active: true,
                    created_at_unix: now_unix,
                    updated_at_unix: now_unix,
                    disabled_at_unix: None,
                };
                ScimGroupProvisionOutcome {
                    record: entry.insert(record).clone(),
                    created: true,
                }
            }
        }
    }

    /// Look up a SCIM group by id within the authenticated client's tenant.
    ///
    /// Read-only: returns `None` if no group with that id is bound to this
    /// tenant/provider. Used to resolve a `PATCH` target without mutating state.
    pub fn find_scim_group(
        &self,
        client: &ScimClientContext,
        group_id: Uuid,
    ) -> Option<ScimGroupRecord> {
        self.inner
            .read()
            .expect("in-memory hosted auth store lock poisoned")
            .scim_groups
            .values()
            .find(|group| {
                group.id == group_id
                    && group.org_id == client.org_id
                    && group.repo_id == client.repo_id
                    && group.client_provider_key == client.provider_key
            })
            .cloned()
    }

    /// Update the bounded attributes of an existing SCIM group, scoped to the
    /// authenticated client's tenant. Returns `None` if the group is unknown.
    pub fn update_scim_group(
        &self,
        client: &ScimClientContext,
        group_id: Uuid,
        display_name: Option<&str>,
        now_unix: u64,
    ) -> Option<ScimGroupRecord> {
        let mut guard = self
            .inner
            .write()
            .expect("in-memory hosted auth store lock poisoned");
        let group = guard.scim_groups.values_mut().find(|group| {
            group.id == group_id
                && group.org_id == client.org_id
                && group.repo_id == client.repo_id
                && group.client_provider_key == client.provider_key
        })?;
        if let Some(display_name) = display_name {
            group.display_name = Some(display_name.to_string());
        }
        group.updated_at_unix = now_unix;
        Some(group.clone())
    }

    /// Idempotently add an existing principal to a SCIM group.
    ///
    /// Keyed on `(group_id, principal_uid)`: re-adding an active member is a
    /// no-op; re-adding an inactive member re-activates the existing row.
    pub fn add_scim_group_member(
        &self,
        client: &ScimClientContext,
        group_id: Uuid,
        principal_uid: Uid,
        now_unix: u64,
    ) -> ScimGroupMemberRecord {
        let key = ScimGroupMemberKey {
            group_id,
            principal_uid,
        };
        let mut guard = self
            .inner
            .write()
            .expect("in-memory hosted auth store lock poisoned");
        let member = guard
            .scim_group_members
            .entry(key)
            .or_insert_with(|| ScimGroupMemberRecord {
                id: Uuid::new_v4(),
                org_id: client.org_id.clone(),
                repo_id: client.repo_id.clone(),
                group_id,
                principal_uid,
                active: true,
                created_at_unix: now_unix,
                updated_at_unix: now_unix,
                disabled_at_unix: None,
            });
        if !member.active {
            member.active = true;
            member.disabled_at_unix = None;
            member.updated_at_unix = now_unix;
        }
        member.clone()
    }

    /// Idempotently remove a principal from a SCIM group.
    ///
    /// Sets `active = false` and `disabled_at_unix`; a second removal is a
    /// no-op that preserves the original disable timestamp. Returns `None` if
    /// the membership is unknown.
    pub fn remove_scim_group_member(
        &self,
        _client: &ScimClientContext,
        group_id: Uuid,
        principal_uid: Uid,
        now_unix: u64,
    ) -> Option<ScimGroupMemberRecord> {
        let key = ScimGroupMemberKey {
            group_id,
            principal_uid,
        };
        let mut guard = self
            .inner
            .write()
            .expect("in-memory hosted auth store lock poisoned");
        let member = guard.scim_group_members.get_mut(&key)?;
        if member.active {
            member.active = false;
            member.disabled_at_unix = Some(now_unix);
            member.updated_at_unix = now_unix;
        }
        Some(member.clone())
    }

    /// Revoke all hosted access and refresh credentials for a tenant principal.
    ///
    /// Marks every matching refresh family, refresh token, and access token as
    /// revoked at `revoked_at_unix`, returning bounded counts of records that
    /// newly transitioned from not-revoked to revoked. Idempotent: a second
    /// call revokes nothing new and returns all-zero counts.
    pub fn revoke_principal_hosted_access(
        &self,
        org_id: &OrgId,
        repo_id: &RepoId,
        principal_uid: Uid,
        revoked_at_unix: u64,
    ) -> ScimRevocationSummary {
        let mut guard = self
            .inner
            .write()
            .expect("in-memory hosted auth store lock poisoned");

        let mut refresh_families_revoked = 0;
        for family in guard.refresh_families.values_mut() {
            if family.org_id == *org_id
                && family.repo_id == *repo_id
                && family.principal_uid == principal_uid
                && family.revoked_at_unix.is_none()
            {
                family.revoked_at_unix = Some(revoked_at_unix);
                refresh_families_revoked += 1;
            }
        }

        let mut refresh_tokens_revoked = 0;
        for token in guard.refresh_tokens.values_mut() {
            if token.org_id == *org_id
                && token.repo_id == *repo_id
                && token.principal_uid == principal_uid
                && token.revoked_at_unix.is_none()
            {
                token.revoked_at_unix = Some(revoked_at_unix);
                refresh_tokens_revoked += 1;
            }
        }

        let mut access_tokens_revoked = 0;
        for record in guard.access_tokens.values_mut() {
            if record.identity.org_id == *org_id
                && record.identity.repo_id == *repo_id
                && record.identity.uid == principal_uid
                && record.revoked_at_unix.is_none()
            {
                record.revoked_at_unix = Some(revoked_at_unix);
                access_tokens_revoked += 1;
            }
        }

        ScimRevocationSummary {
            access_tokens_revoked,
            refresh_tokens_revoked,
            refresh_families_revoked,
        }
    }

    #[cfg(test)]
    pub fn bind_scim_client_for_test(&self, config: ScimClientConfig) {
        self.inner
            .write()
            .expect("in-memory hosted auth store lock poisoned")
            .scim_clients
            .push(config);
    }

    #[cfg(test)]
    pub fn scim_user_count(&self) -> usize {
        self.inner
            .read()
            .expect("in-memory hosted auth store lock poisoned")
            .scim_users
            .len()
    }

    #[cfg(test)]
    pub fn scim_group_count(&self) -> usize {
        self.inner
            .read()
            .expect("in-memory hosted auth store lock poisoned")
            .scim_groups
            .len()
    }

    #[cfg(test)]
    pub fn scim_group_member_count(&self) -> usize {
        self.inner
            .read()
            .expect("in-memory hosted auth store lock poisoned")
            .scim_group_members
            .len()
    }

    fn record_saml_assertion_replay(
        &self,
        assertion_replay_key: &str,
        expires_at_unix: u64,
        now_unix: u64,
    ) -> Result<(), HostedSamlVerificationError> {
        if now_unix >= expires_at_unix {
            return Err(HostedSamlVerificationError::AssertionExpired);
        }
        if !valid_saml_replay_key(assertion_replay_key) {
            return Err(HostedSamlVerificationError::InvalidAssertion);
        }

        let mut guard = self
            .inner
            .write()
            .expect("in-memory hosted auth store lock poisoned");
        guard
            .saml_assertion_replay
            .retain(|_, active_until_unix| now_unix < *active_until_unix);
        if guard
            .saml_assertion_replay
            .contains_key(assertion_replay_key)
        {
            return Err(HostedSamlVerificationError::AssertionReplay);
        }
        guard
            .saml_assertion_replay
            .insert(assertion_replay_key.to_string(), expires_at_unix);
        Ok(())
    }

    pub fn issue_access_token(
        &self,
        identity: &HostedSessionIdentity,
        issued_at_unix: u64,
        expires_at_unix: u64,
    ) -> IssuedAccessToken {
        let raw_secret = generate_hosted_token_secret();
        let token_hash = hash_hosted_token_secret(&raw_secret);
        let mut guard = self
            .inner
            .write()
            .expect("in-memory hosted auth store lock poisoned");
        guard
            .session_identities
            .insert(identity.session_id, identity.clone());
        guard.access_tokens.insert(
            token_hash.clone(),
            StoredAccessTokenRecord {
                identity: identity.clone(),
                expires_at_unix,
                revoked_at_unix: None,
            },
        );
        IssuedAccessToken {
            identity: identity.clone(),
            token_hash,
            raw_secret,
            issued_at_unix,
            expires_at_unix,
        }
    }

    pub fn validate_access_token_at(
        &self,
        raw_secret: &str,
        now_unix: u64,
    ) -> Option<HostedSessionIdentity> {
        let expected_hash = hash_hosted_token_secret(raw_secret);
        self.inner
            .read()
            .expect("in-memory hosted auth store lock poisoned")
            .access_tokens
            .iter()
            .find_map(|(token_hash, record)| {
                (hosted_token_hash_eq(token_hash, &expected_hash)
                    && record.revoked_at_unix.is_none()
                    && now_unix < record.expires_at_unix)
                    .then(|| record.identity.clone())
            })
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
        guard
            .session_identities
            .insert(identity.session_id, identity.clone());
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

    pub fn revoke_refresh_token_secret(
        &self,
        raw_secret: &str,
        revoked_at_unix: u64,
    ) -> Result<RefreshTokenRecord, RefreshTokenError> {
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
            .get_mut(&token_id)
            .expect("matched token id exists for revoke");
        token.revoked_at_unix = Some(revoked_at_unix);
        let token = token.clone();
        if let Some(family) = guard.refresh_families.get_mut(&token.family_id) {
            family.revoked_at_unix = Some(revoked_at_unix);
        }
        Ok(token)
    }

    pub fn refresh_token_for_secret(&self, raw_secret: &str) -> Option<RefreshTokenRecord> {
        let expected_hash = hash_hosted_token_secret(raw_secret);
        self.inner
            .read()
            .expect("in-memory hosted auth store lock poisoned")
            .refresh_tokens
            .values()
            .find(|token| hosted_token_hash_eq(&token.token_hash, &expected_hash))
            .cloned()
    }

    pub fn hosted_session_identity(&self, session_id: Uuid) -> Option<HostedSessionIdentity> {
        self.inner
            .read()
            .expect("in-memory hosted auth store lock poisoned")
            .session_identities
            .get(&session_id)
            .cloned()
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

pub(crate) fn hash_saml_assertion_id(assertion_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"saml-assertion-id:");
    hasher.update(assertion_id.as_bytes());
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

fn valid_saml_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_saml_entity_id(value: &str) -> bool {
    valid_bounded_visible_ascii(value, 512)
}

fn valid_saml_acs_url(value: &str) -> bool {
    if !valid_bounded_visible_ascii(value, 2048) {
        return false;
    }
    let Ok(uri) = value.parse::<axum::http::Uri>() else {
        return false;
    };
    if uri.scheme_str() != Some("https") {
        return false;
    }
    let Some(authority) = uri.authority() else {
        return false;
    };
    let authority = authority.as_str();
    if authority.contains('@') || authority.contains('[') || authority.contains(']') {
        return false;
    }
    let host = if let Some((host, port)) = authority.rsplit_once(':') {
        if host.contains(':') || port.parse::<u16>().is_err() {
            return false;
        }
        host
    } else {
        authority
    };
    valid_saml_acs_host(host)
}

fn valid_saml_acs_host(host: &str) -> bool {
    !host.is_empty()
        && host.split('.').all(|label| {
            !label.is_empty()
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-'))
                && label
                    .bytes()
                    .next()
                    .is_some_and(|byte| byte.is_ascii_alphanumeric())
                && label
                    .bytes()
                    .last()
                    .is_some_and(|byte| byte.is_ascii_alphanumeric())
        })
}

fn valid_saml_cert_ref(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(valid_sha256_hex)
}

fn valid_saml_group_name(value: &str) -> bool {
    valid_bounded_visible_ascii(value, 128)
        && !value
            .bytes()
            .any(|byte| matches!(byte, b'<' | b'>' | b'"' | b'\''))
}

fn valid_saml_external_identity_id(value: &str) -> bool {
    valid_bounded_visible_ascii(value, 256)
}

fn valid_saml_assertion_id(value: &str) -> bool {
    valid_bounded_visible_ascii(value, 256)
}

fn valid_bounded_visible_ascii(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !byte.is_ascii_whitespace())
}

fn valid_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn valid_saml_replay_key(value: &str) -> bool {
    valid_sha256_hex(value)
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
    fn oidc_verification_request_debug_redacts_secret_inputs() {
        let request = HostedOidcVerificationRequest {
            provider: "provider_key",
            authorization_code: "secret-auth-code",
            redirect_uri: Some("https://sensitive.example/callback"),
        };

        let debug = format!("{request:?}");

        assert!(debug.contains("provider_key"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret-auth-code"));
        assert!(!debug.contains("sensitive.example"));
    }

    #[test]
    fn saml_request_debug_redacts_assertion_and_relay_state() {
        let org_id = OrgId::new("org_demo").unwrap();
        let repo_id = RepoId::new("repo_demo").unwrap();
        let request = HostedSamlVerificationRequest {
            provider: "saml_provider",
            saml_response: "<Assertion><NameID>secret-name-id</NameID></Assertion>",
            relay_state: Some("secret-relay-state"),
            org_id: &org_id,
            repo_id: &repo_id,
        };

        let debug = format!("{request:?}");

        assert!(debug.contains("saml_provider"));
        assert!(debug.contains("org_demo"));
        assert!(debug.contains("repo_demo"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret-name-id"));
        assert!(!debug.contains("secret-relay-state"));
        assert!(!debug.contains("<Assertion>"));

        let assertion_debug = format!("{:?}", test_saml_assertion());
        assert!(assertion_debug.contains("<redacted>"));
        assert!(!assertion_debug.contains("secret-name-id"));
        assert!(!assertion_debug.contains("secret-assertion-id"));
        assert!(!assertion_debug.contains("auth.example.invalid"));
    }

    #[test]
    fn saml_metadata_rejects_malformed_entity_id_and_oversized_inputs() {
        let mut metadata = test_saml_metadata();
        metadata.idp_entity_id = "idp entity with spaces".to_string();

        assert_eq!(
            metadata.validate(),
            Err(HostedSamlVerificationError::InvalidMetadata)
        );

        let mut metadata = test_saml_metadata();
        metadata.provider_key = "a".repeat(129);

        assert_eq!(
            metadata.validate(),
            Err(HostedSamlVerificationError::InvalidMetadata)
        );

        let mut metadata = test_saml_metadata();
        metadata.acs_url = "https://".to_string();

        assert_eq!(
            metadata.validate(),
            Err(HostedSamlVerificationError::InvalidMetadata)
        );

        let mut metadata = test_saml_metadata();
        metadata.acs_url = "https://sp.example:bad/auth/saml/acs".to_string();

        assert_eq!(
            metadata.validate(),
            Err(HostedSamlVerificationError::InvalidMetadata)
        );
    }

    #[test]
    fn saml_metadata_rejects_unsupported_binding() {
        let mut metadata = test_saml_metadata();
        metadata.binding = SamlBinding::HttpRedirect;

        assert_eq!(
            metadata.validate(),
            Err(HostedSamlVerificationError::InvalidMetadata)
        );
    }

    #[test]
    fn saml_metadata_rejects_missing_or_invalid_signing_cert_reference() {
        let mut metadata = test_saml_metadata();
        metadata.signing_certificate_ref = String::new();

        assert_eq!(
            metadata.validate(),
            Err(HostedSamlVerificationError::InvalidMetadata)
        );

        let mut metadata = test_saml_metadata();
        metadata.signing_certificate_ref =
            "-----BEGIN CERTIFICATE-----secret-----END CERTIFICATE-----".to_string();

        let debug = format!("{metadata:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("BEGIN CERTIFICATE"));
        assert!(!debug.contains("auth.example.invalid"));

        assert_eq!(
            metadata.validate(),
            Err(HostedSamlVerificationError::InvalidMetadata)
        );

        let mut metadata = test_saml_metadata();
        metadata.signing_certificate_ref =
            "sha256:ABCDEFabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123".to_string();

        assert_eq!(
            metadata.validate(),
            Err(HostedSamlVerificationError::InvalidMetadata)
        );
    }

    #[test]
    fn saml_assertion_rejects_expired_and_not_yet_valid_windows() {
        let metadata = test_saml_metadata();
        let mut assertion = test_saml_assertion();
        assertion.expires_at_unix = 20;

        assert_eq!(
            assertion.verify_against(&metadata, 20),
            Err(HostedSamlVerificationError::AssertionExpired)
        );

        let metadata = test_saml_metadata();
        let mut assertion = test_saml_assertion();
        assertion.not_before_unix = 30;

        assert_eq!(
            assertion.verify_against(&metadata, 20),
            Err(HostedSamlVerificationError::AssertionNotYetValid)
        );
    }

    #[test]
    fn saml_assertion_rejects_audience_acs_and_entity_mismatch() {
        let metadata = test_saml_metadata();
        let mut assertion = test_saml_assertion();
        assertion.audience = "sp:wrong-audience".to_string();

        assert_eq!(
            assertion.verify_against(&metadata, 20),
            Err(HostedSamlVerificationError::InvalidAssertion)
        );

        let metadata = test_saml_metadata();
        let mut assertion = test_saml_assertion();
        assertion.acs_url = "https://auth.example.invalid/wrong-acs".to_string();

        assert_eq!(
            assertion.verify_against(&metadata, 20),
            Err(HostedSamlVerificationError::InvalidAssertion)
        );

        let metadata = test_saml_metadata();
        let mut assertion = test_saml_assertion();
        assertion.issuer_entity_id = "idp:wrong".to_string();

        assert_eq!(
            assertion.verify_against(&metadata, 20),
            Err(HostedSamlVerificationError::InvalidAssertion)
        );
    }

    #[test]
    fn saml_group_mapping_requires_configured_group() {
        let metadata = test_saml_metadata();

        assert_eq!(
            metadata.map_external_groups(&["engineering".to_string()]),
            Err(HostedSamlVerificationError::GroupMappingMismatch)
        );

        assert_eq!(
            metadata
                .map_external_groups(&["admins".to_string()])
                .expect("required group maps"),
            vec![200]
        );

        let claims = test_saml_assertion()
            .verify_against(&metadata, 20)
            .expect("valid assertion verifies");
        let debug = format!("{claims:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains(&claims.assertion_id_hash));
        assert!(!debug.contains("auth.example.invalid"));
        assert!(!debug.contains("saml:demo:user"));

        let mut assertion = test_saml_assertion();
        assertion.external_groups = vec!["admins".to_string(), "bad group".to_string()];

        assert_eq!(
            assertion.verify_against(&metadata, 20),
            Err(HostedSamlVerificationError::InvalidAssertion)
        );
    }

    #[test]
    fn saml_assertion_replay_is_denied_until_expiry() {
        let store = InMemoryHostedAuthStore::default();
        let metadata = test_saml_metadata();
        let claims = test_saml_assertion()
            .verify_against(&metadata, 10)
            .expect("valid assertion verifies");

        assert_eq!(
            store.record_saml_assertion_replay_for_claims(&claims, 10),
            Ok(())
        );
        assert_eq!(
            store.record_saml_assertion_replay_for_claims(&claims, 19),
            Err(HostedSamlVerificationError::AssertionReplay)
        );
        assert_eq!(
            store.record_saml_assertion_replay_for_claims(&claims, 30),
            Err(HostedSamlVerificationError::AssertionExpired)
        );
        let mut later_claims = claims.clone();
        later_claims.expires_at_unix = 40;
        assert_eq!(
            store.record_saml_assertion_replay_for_claims(&later_claims, 30),
            Ok(())
        );
        assert!(!format!("{store:?}").contains(&claims.assertion_id_hash));

        let store = InMemoryHostedAuthStore::default();
        let mut other_metadata = test_saml_metadata();
        other_metadata.provider_key = "other_saml_provider".to_string();
        let mut other_assertion = test_saml_assertion();
        other_assertion.provider_key = "other_saml_provider".to_string();
        let other_claims = other_assertion
            .verify_against(&other_metadata, 10)
            .expect("same assertion id in different provider verifies");

        assert_eq!(
            store.record_saml_assertion_replay_for_claims(&other_claims, 10),
            Ok(())
        );
    }

    #[test]
    fn saml_provider_denial_debug_redacts_provider_message_surface() {
        let error = HostedSamlVerificationError::ProviderDenied(HostedSamlProviderDenial);

        let debug = format!("{error:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("NameID"));
        assert!(!debug.contains("BEGIN CERTIFICATE"));
    }

    #[test]
    fn oidc_verification_error_debug_redacts_provider_message() {
        let error = HostedOidcVerificationError::ProviderDenied {
            message: "id_token=secret-id-token https://issuer.example/jwks".to_string(),
        };

        let debug = format!("{error:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret-id-token"));
        assert!(!debug.contains("issuer.example"));
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
    fn hosted_store_debug_redacts_access_token_hashes() {
        let store = InMemoryHostedAuthStore::default();
        let issued = store.issue_access_token(&test_identity(), 10, 70);

        let debug = format!("{store:?}");

        assert!(!debug.contains(&issued.raw_secret));
        assert!(!debug.contains(&issued.token_hash));
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

    #[test]
    fn scim_client_config_debug_redacts_token_hash() {
        let config = ScimClientConfig {
            provider_key: "scim_provider".to_string(),
            org_id: OrgId::new("org_demo").unwrap(),
            repo_id: RepoId::new("repo_demo").unwrap(),
            token_hash: hash_hosted_token_secret("super-secret-scim-token"),
            enabled: true,
        };

        let debug = format!("{config:?}");

        assert!(debug.contains("scim_provider"));
        assert!(debug.contains("org_demo"));
        assert!(debug.contains("repo_demo"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains(&config.token_hash));
    }

    #[test]
    fn scim_user_and_group_records_redact_external_id_hash() {
        let user = ScimUserRecord {
            id: Uuid::new_v4(),
            org_id: OrgId::new("org_demo").unwrap(),
            repo_id: RepoId::new("repo_demo").unwrap(),
            client_provider_key: "scim_provider".to_string(),
            principal_uid: 1000,
            external_id_hash: hash_hosted_token_secret("scim:demo:user-external-id"),
            username_hint: Some("demo-user".to_string()),
            active: true,
            created_at_unix: 10,
            updated_at_unix: 10,
            disabled_at_unix: None,
        };
        let group = ScimGroupRecord {
            id: Uuid::new_v4(),
            org_id: OrgId::new("org_demo").unwrap(),
            repo_id: RepoId::new("repo_demo").unwrap(),
            client_provider_key: "scim_provider".to_string(),
            local_gid: 200,
            external_id_hash: hash_hosted_token_secret("scim:demo:group-external-id"),
            display_name: Some("Admins".to_string()),
            active: true,
            created_at_unix: 10,
            updated_at_unix: 10,
            disabled_at_unix: None,
        };

        let user_debug = format!("{user:?}");
        let group_debug = format!("{group:?}");

        assert!(user_debug.contains("<redacted>"));
        assert!(!user_debug.contains(&user.external_id_hash));
        assert!(group_debug.contains("<redacted>"));
        assert!(!group_debug.contains(&group.external_id_hash));
    }

    #[test]
    fn scim_authenticate_rejects_disabled_unknown_and_tenant_mismatch() {
        let store = InMemoryHostedAuthStore::default();
        let org_id = OrgId::new("org_demo").unwrap();
        let repo_id = RepoId::new("repo_demo").unwrap();
        let other_repo_id = RepoId::new("repo_other").unwrap();
        let token_hash = hash_hosted_token_secret("scim-bearer-secret");
        let disabled_hash = hash_hosted_token_secret("scim-disabled-secret");

        store.bind_scim_client_for_test(ScimClientConfig {
            provider_key: "scim_provider".to_string(),
            org_id: org_id.clone(),
            repo_id: repo_id.clone(),
            token_hash: token_hash.clone(),
            enabled: true,
        });
        store.bind_scim_client_for_test(ScimClientConfig {
            provider_key: "scim_disabled".to_string(),
            org_id: org_id.clone(),
            repo_id: repo_id.clone(),
            token_hash: disabled_hash.clone(),
            enabled: false,
        });

        let context = store
            .authenticate_scim_client(&token_hash, &org_id, &repo_id)
            .expect("enabled client authenticates");
        assert_eq!(context.org_id, org_id);
        assert_eq!(context.repo_id, repo_id);
        assert_eq!(context.provider_key, "scim_provider");

        assert_eq!(
            store.authenticate_scim_client("0".repeat(64).as_str(), &org_id, &repo_id),
            Err(ScimAuthError::Unauthorized)
        );
        assert_eq!(
            store.authenticate_scim_client(&disabled_hash, &org_id, &repo_id),
            Err(ScimAuthError::Disabled)
        );
        assert_eq!(
            store.authenticate_scim_client(&token_hash, &org_id, &other_repo_id),
            Err(ScimAuthError::TenantMismatch)
        );
    }

    #[test]
    fn scim_user_provision_is_idempotent_on_external_id() {
        let store = InMemoryHostedAuthStore::default();
        let context = test_scim_context(&store);
        let external_id_hash = hash_hosted_token_secret("scim:demo:user-external-id");

        let first =
            store.provision_scim_user(&context, 1000, &external_id_hash, Some("demo-user"), 10);
        let second =
            store.provision_scim_user(&context, 1000, &external_id_hash, Some("demo-user"), 20);

        assert!(first.created);
        assert!(!second.created);
        assert_eq!(first.record.id, second.record.id);
        assert_eq!(store.scim_user_count(), 1);
        assert!(second.record.active);
    }

    #[test]
    fn scim_group_membership_add_remove_is_idempotent() {
        let store = InMemoryHostedAuthStore::default();
        let context = test_scim_context(&store);
        let group_external_id = hash_hosted_token_secret("scim:demo:group-external-id");

        let group =
            store.provision_scim_group(&context, 200, &group_external_id, Some("Admins"), 10);
        let group_again =
            store.provision_scim_group(&context, 200, &group_external_id, Some("Admins"), 20);
        assert!(group.created);
        assert!(!group_again.created);
        assert_eq!(group.record.id, group_again.record.id);
        assert_eq!(store.scim_group_count(), 1);
        let group_id = group.record.id;

        let member = store.add_scim_group_member(&context, group_id, 1000, 30);
        let member_again = store.add_scim_group_member(&context, group_id, 1000, 40);
        assert_eq!(member.id, member_again.id);
        assert!(member_again.active);
        assert_eq!(store.scim_group_member_count(), 1);

        let removed = store
            .remove_scim_group_member(&context, group_id, 1000, 50)
            .expect("member exists for removal");
        assert!(!removed.active);
        assert_eq!(removed.disabled_at_unix, Some(50));
        let removed_again = store
            .remove_scim_group_member(&context, group_id, 1000, 60)
            .expect("member still tracked after removal");
        assert!(!removed_again.active);
        assert_eq!(removed_again.disabled_at_unix, Some(50));
        assert_eq!(store.scim_group_member_count(), 1);

        let re_added = store.add_scim_group_member(&context, group_id, 1000, 70);
        assert!(re_added.active);
        assert_eq!(re_added.id, member.id);
        assert_eq!(store.scim_group_member_count(), 1);
    }

    #[test]
    fn scim_find_group_is_read_only_and_update_is_tenant_scoped() {
        let store = InMemoryHostedAuthStore::default();
        let context = test_scim_context(&store);
        let group_external_id = hash_hosted_token_secret("scim:demo:group-external-id");

        // Looking up an unknown group must not create one.
        assert!(store.find_scim_group(&context, Uuid::new_v4()).is_none());
        assert_eq!(store.scim_group_count(), 0);
        assert!(
            store
                .update_scim_group(&context, Uuid::new_v4(), Some("X"), 5)
                .is_none()
        );
        assert_eq!(store.scim_group_count(), 0);

        let group = store
            .provision_scim_group(&context, 200, &group_external_id, Some("Admins"), 10)
            .record;
        let found = store
            .find_scim_group(&context, group.id)
            .expect("group is found by id within tenant");
        assert_eq!(found.id, group.id);
        assert_eq!(found.local_gid, 200);

        let updated = store
            .update_scim_group(&context, group.id, Some("Renamed"), 20)
            .expect("group updates within tenant");
        assert_eq!(updated.display_name.as_deref(), Some("Renamed"));
        assert_eq!(updated.updated_at_unix, 20);
        assert_eq!(store.scim_group_count(), 1);
    }

    #[test]
    fn scim_revoke_principal_hosted_access_invalidates_access_and_refresh() {
        let store = InMemoryHostedAuthStore::default();
        let identity = test_identity();
        let access = store.issue_access_token(&identity, 10, 100);
        let refresh = store.issue_refresh_token(&identity, 10, 100);

        assert!(
            store
                .validate_access_token_at(&access.raw_secret, 20)
                .is_some()
        );

        let summary = store.revoke_principal_hosted_access(
            &identity.org_id,
            &identity.repo_id,
            identity.uid,
            30,
        );

        assert_eq!(summary.access_tokens_revoked, 1);
        assert_eq!(summary.refresh_tokens_revoked, 1);
        assert_eq!(summary.refresh_families_revoked, 1);
        assert!(
            store
                .validate_access_token_at(&access.raw_secret, 40)
                .is_none()
        );
        assert_eq!(
            store.rotate_refresh_token(&refresh.raw_secret, 40, 120),
            Err(RefreshTokenError::FamilyCompromised)
        );
    }

    #[test]
    fn scim_revoke_principal_hosted_access_is_idempotent() {
        let store = InMemoryHostedAuthStore::default();
        let identity = test_identity();
        store.issue_access_token(&identity, 10, 100);
        store.issue_refresh_token(&identity, 10, 100);

        let first = store.revoke_principal_hosted_access(
            &identity.org_id,
            &identity.repo_id,
            identity.uid,
            30,
        );
        let second = store.revoke_principal_hosted_access(
            &identity.org_id,
            &identity.repo_id,
            identity.uid,
            40,
        );

        assert_eq!(first.access_tokens_revoked, 1);
        assert_eq!(first.refresh_tokens_revoked, 1);
        assert_eq!(first.refresh_families_revoked, 1);
        assert_eq!(
            second,
            ScimRevocationSummary {
                access_tokens_revoked: 0,
                refresh_tokens_revoked: 0,
                refresh_families_revoked: 0,
            }
        );
    }

    fn test_scim_context(store: &InMemoryHostedAuthStore) -> ScimClientContext {
        let org_id = OrgId::new("org_demo").unwrap();
        let repo_id = RepoId::new("repo_demo").unwrap();
        let token_hash = hash_hosted_token_secret("scim-bearer-secret");
        store.bind_scim_client_for_test(ScimClientConfig {
            provider_key: "scim_provider".to_string(),
            org_id: org_id.clone(),
            repo_id: repo_id.clone(),
            token_hash: token_hash.clone(),
            enabled: true,
        });
        store
            .authenticate_scim_client(&token_hash, &org_id, &repo_id)
            .expect("seeded scim client authenticates")
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

    fn test_saml_metadata() -> SamlProviderMetadata {
        SamlProviderMetadata {
            provider_key: "saml_provider".to_string(),
            org_id: OrgId::new("org_demo").unwrap(),
            repo_id: RepoId::new("repo_demo").unwrap(),
            idp_entity_id: "idp:demo".to_string(),
            sp_entity_id: "sp:demo".to_string(),
            acs_url: "https://auth.example.invalid/saml/acs".to_string(),
            audience: "sp:demo".to_string(),
            binding: SamlBinding::HttpPost,
            signing_certificate_ref:
                "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string(),
            group_attribute_name: "groups".to_string(),
            required_external_group: Some("admins".to_string()),
            group_mappings: vec![SamlGroupMapping {
                external_group: "admins".to_string(),
                gid: 200,
            }],
        }
    }

    fn test_saml_assertion() -> HostedSamlAssertion {
        HostedSamlAssertion {
            provider_key: "saml_provider".to_string(),
            org_id: OrgId::new("org_demo").unwrap(),
            repo_id: RepoId::new("repo_demo").unwrap(),
            uid: 1000,
            username: "demo-user".to_string(),
            gid: 100,
            external_identity_id: "saml:demo:user".to_string(),
            assertion_id: "secret-assertion-id".to_string(),
            not_before_unix: 10,
            expires_at_unix: 30,
            audience: "sp:demo".to_string(),
            acs_url: "https://auth.example.invalid/saml/acs".to_string(),
            issuer_entity_id: "idp:demo".to_string(),
            name_id: "secret-name-id".to_string(),
            external_groups: vec!["admins".to_string()],
        }
    }
}
