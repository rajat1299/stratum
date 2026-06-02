//! Provider-free SCIM 2.0 provisioning routes (`/scim/v2/*`).
//!
//! SCIM is disabled by default and orthogonal to the login-provider mode. Each
//! handler authenticates a tenant-scoped SCIM client from a distinct
//! `Authorization: Scim-Bearer <token>` scheme plus validated `X-Stratum-Org` /
//! `X-Stratum-Repo` headers, never falling back to local/root/session identity.
//! All public errors, audit details, and responses are redacted: they never
//! carry raw bodies, SCIM tokens, token hashes, or raw external ids.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{patch, post};
use axum::{Json, Router};
use ring::hmac;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::AppState;
use super::ServerState;
use super::repo_context::{TenantRepoResolver, parse_org_header, parse_repo_header};
use super::routes_auth::{bounded_auth_field, current_unix_time, public_error};
use crate::audit::{AuditAction, AuditActor, AuditResource, AuditResourceKind, NewAuditEvent};
use crate::auth::hosted::{
    ScimAuthError, ScimClientContext, ScimRevocationSummary, hash_hosted_token_secret,
};
use crate::backend::{OrgId, RepoId};

/// Authorization scheme used exclusively by SCIM routes. Distinct from the
/// agent/workspace `Bearer` and hosted `Stratum-Session` schemes so there is
/// zero overlap with existing authentication paths.
const SCIM_BEARER_SCHEME: &str = "Scim-Bearer ";

/// Maximum byte length for a raw SCIM external id before it is rejected.
const MAX_EXTERNAL_ID_BYTES: usize = 4096;
/// Maximum byte length for bounded display/user-name hints.
const MAX_NAME_HINT_BYTES: usize = 256;
/// Maximum byte length for a SCIM resource path id (a uuid string).
const MAX_RESOURCE_ID_BYTES: usize = 64;
/// Maximum number of member references accepted in a single group PATCH.
const MAX_GROUP_MEMBER_OPS: usize = 256;
/// Domain separator for keyed SCIM external-id hashes.
const SCIM_EXTERNAL_ID_HASH_CONTEXT: &[u8] = b"stratum-scim-external-id-v1\0";

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/scim/v2/Users", post(create_user))
        .route(
            "/scim/v2/Users/{user_id}",
            patch(patch_user).delete(delete_user),
        )
        .route("/scim/v2/Groups", post(create_group))
        .route("/scim/v2/Groups/{group_id}", patch(patch_group))
}

// ---------------------------------------------------------------------------
// Request bodies (bounded, conservative).
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct ScimUserCreateRequest {
    pub external_id: String,
    pub principal_uid: u32,
    pub user_name: Option<String>,
    pub active: Option<bool>,
}

#[derive(Deserialize)]
pub struct ScimUserPatchRequest {
    pub external_id: String,
    pub user_name: Option<String>,
    pub active: Option<bool>,
}

#[derive(Deserialize)]
pub struct ScimUserDeleteRequest {
    pub external_id: String,
}

#[derive(Deserialize)]
pub struct ScimGroupCreateRequest {
    pub external_id: String,
    pub local_gid: u32,
    pub display_name: Option<String>,
}

#[derive(Deserialize)]
pub struct ScimGroupPatchRequest {
    pub external_id: String,
    #[serde(default)]
    pub members_add: Vec<ScimMemberRef>,
    #[serde(default)]
    pub members_remove: Vec<ScimMemberRef>,
    pub display_name: Option<String>,
}

/// A reference to a tenant principal that should join or leave the group named
/// by the request path. `group_external_id` is accepted for SCIM-client
/// compatibility but the path id is authoritative.
#[derive(Deserialize)]
pub struct ScimMemberRef {
    #[serde(default)]
    pub group_external_id: Option<String>,
    pub principal_uid: u32,
}

// ---------------------------------------------------------------------------
// Response bodies (redacted: no raw external ids, hashes, or tokens).
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ScimUserResponse {
    id: String,
    org_id: String,
    repo_id: String,
    principal_uid: u32,
    active: bool,
}

#[derive(Serialize)]
struct ScimGroupResponse {
    id: String,
    org_id: String,
    repo_id: String,
    local_gid: u32,
    active: bool,
    members_added: usize,
    members_removed: usize,
}

// ---------------------------------------------------------------------------
// Errors.
// ---------------------------------------------------------------------------

/// Redaction-safe SCIM route failure. Maps to bounded public errors; variants
/// never carry raw bodies, tokens, hashes, external ids, or provider errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScimRouteError {
    /// Missing/wrong-scheme `Scim-Bearer` credential. Redacted reason.
    MissingBearer,
    /// Missing or invalid tenant headers.
    MissingTenant,
    /// SCIM provider disabled for the presented credential.
    Disabled,
    /// Unknown credential.
    Unauthorized,
    /// Credential bound to a different tenant.
    TenantMismatch,
    /// Defense-in-depth: repo is not bound to the org.
    RepoNotBound,
    /// Request body failed bounds/shape validation.
    InvalidRequest,
    /// Targeted resource does not exist for the tenant.
    UnknownResource,
    /// Audit append failed; fail closed.
    Audit,
}

impl ScimRouteError {
    /// Redacted reason recorded in the `AuthScimRequestDenied` audit event.
    fn deny_reason(self) -> &'static str {
        match self {
            ScimRouteError::MissingBearer => "missing_bearer",
            ScimRouteError::MissingTenant => "missing_tenant",
            ScimRouteError::Disabled => "provider_disabled",
            ScimRouteError::Unauthorized => "unauthorized",
            ScimRouteError::TenantMismatch => "tenant_mismatch",
            ScimRouteError::RepoNotBound => "repo_not_bound",
            ScimRouteError::InvalidRequest => "invalid_request",
            ScimRouteError::UnknownResource => "unknown_resource",
            ScimRouteError::Audit => "audit_unavailable",
        }
    }

    fn into_response(self) -> Response {
        match self {
            ScimRouteError::MissingBearer
            | ScimRouteError::Disabled
            | ScimRouteError::Unauthorized
            | ScimRouteError::TenantMismatch
            | ScimRouteError::RepoNotBound
            | ScimRouteError::MissingTenant => {
                public_error(StatusCode::UNAUTHORIZED, "scim request unauthorized")
            }
            ScimRouteError::InvalidRequest => {
                public_error(StatusCode::BAD_REQUEST, "invalid scim request")
            }
            ScimRouteError::UnknownResource => {
                public_error(StatusCode::NOT_FOUND, "scim resource not found")
            }
            ScimRouteError::Audit => {
                public_error(StatusCode::INTERNAL_SERVER_ERROR, "scim audit unavailable")
            }
        }
    }
}

impl From<ScimAuthError> for ScimRouteError {
    fn from(error: ScimAuthError) -> Self {
        match error {
            ScimAuthError::Disabled => ScimRouteError::Disabled,
            ScimAuthError::Unauthorized => ScimRouteError::Unauthorized,
            ScimAuthError::TenantMismatch => ScimRouteError::TenantMismatch,
        }
    }
}

// ---------------------------------------------------------------------------
// Authentication.
// ---------------------------------------------------------------------------

/// Authenticate the SCIM client behind the request.
///
/// Fails closed: a missing/wrong-scheme credential, missing/invalid tenant
/// headers, a disabled/unknown/tenant-mismatched client, or an unbound repo all
/// return an error and (best effort) record a redacted `AuthScimRequestDenied`
/// audit event. Never falls back to any local/root/session identity.
async fn authenticate(
    state: &ServerState,
    headers: &HeaderMap,
) -> Result<ScimClientContext, ScimRouteError> {
    let Some(raw_token) = scim_bearer_token(headers) else {
        return Err(record_denial(state, ScimRouteError::MissingBearer, None, None).await);
    };

    let (Some(org_id), Some(repo_id)) = parse_tenant(headers) else {
        return Err(record_denial(state, ScimRouteError::MissingTenant, None, None).await);
    };

    // Hash the presented bearer token; only the hash is ever compared/stored.
    let presented_hash = hash_hosted_token_secret(raw_token);
    let client = state
        .hosted_auth
        .authenticate_scim_client(&presented_hash, &org_id, &repo_id)
        .map_err(ScimRouteError::from);
    let client = match client {
        Ok(client) => client,
        Err(error) => {
            return Err(record_denial(state, error, Some(&org_id), Some(&repo_id)).await);
        }
    };

    // Defense-in-depth: the repo must still be bound to the org.
    if !state
        .repo_belongs_to_org(&org_id, &repo_id)
        .unwrap_or(false)
    {
        return Err(record_denial(
            state,
            ScimRouteError::RepoNotBound,
            Some(&org_id),
            Some(&repo_id),
        )
        .await);
    }

    Ok(client)
}

/// Extract the raw token from a `Authorization: Scim-Bearer <token>` header.
fn scim_bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")?
        .to_str()
        .ok()?
        .strip_prefix(SCIM_BEARER_SCHEME)
        .map(str::trim)
        .filter(|token| !token.is_empty())
}

/// Hash a raw SCIM external id with the authenticated SCIM bearer token as a
/// tenant/client-local HMAC key. This prevents low-entropy external ids from
/// being checked by comparing plain hashes across stores.
fn scim_external_id_hash(headers: &HeaderMap, external_id: &str) -> Result<String, ScimRouteError> {
    let raw_token = scim_bearer_token(headers).ok_or(ScimRouteError::MissingBearer)?;
    Ok(scim_external_id_hash_for_token(raw_token, external_id))
}

fn scim_external_id_hash_for_token(raw_token: &str, external_id: &str) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, raw_token.as_bytes());
    let mut context = hmac::Context::with_key(&key);
    context.update(SCIM_EXTERNAL_ID_HASH_CONTEXT);
    context.update(external_id.as_bytes());
    lower_hex(context.sign().as_ref())
}

fn lower_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Parse and require both tenant headers; either missing/invalid yields `None`.
fn parse_tenant(headers: &HeaderMap) -> (Option<OrgId>, Option<RepoId>) {
    let org_id = parse_org_header(headers).ok().flatten();
    let repo_id = parse_repo_header(headers).ok().flatten();
    (org_id, repo_id)
}

/// Record a redacted denial audit event and return the original error.
///
/// If the audit append itself fails, the audit failure is surfaced so the
/// route fails closed rather than silently dropping the denial record.
async fn record_denial(
    state: &ServerState,
    error: ScimRouteError,
    org_id: Option<&OrgId>,
    repo_id: Option<&RepoId>,
) -> ScimRouteError {
    let mut event = NewAuditEvent::new(
        scim_audit_actor(),
        AuditAction::AuthScimRequestDenied,
        AuditResource::id(AuditResourceKind::ScimClient, "scim"),
    )
    .with_detail("reason", error.deny_reason());
    if let Some(org_id) = org_id {
        event = event.with_detail("org_id", org_id);
    }
    if let Some(repo_id) = repo_id {
        event = event.with_detail("repo_id", repo_id);
    }
    match state.audit.append(event).await {
        Ok(_) => error,
        Err(_) => ScimRouteError::Audit,
    }
}

/// Bounded, identity-free actor for SCIM provisioning audit events.
fn scim_audit_actor() -> AuditActor {
    AuditActor::new(0, "scim-provisioning")
}

// ---------------------------------------------------------------------------
// Handlers.
// ---------------------------------------------------------------------------

async fn create_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ScimUserCreateRequest>,
) -> Response {
    match create_user_inner(&state, &headers, req).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn create_user_inner(
    state: &ServerState,
    headers: &HeaderMap,
    req: ScimUserCreateRequest,
) -> Result<Response, ScimRouteError> {
    let client = authenticate(state, headers).await?;

    if !valid_external_id(&req.external_id) || !valid_name_hint(req.user_name.as_deref()) {
        return Err(ScimRouteError::InvalidRequest);
    }
    require_scim_principal(state, &client, req.principal_uid)?;
    let external_id_hash = scim_external_id_hash(headers, &req.external_id)?;
    let now = current_unix_time();

    // `provision_scim_user` is idempotent on `(tenant, provider, external_id)`
    // and reports whether this call created the binding, so the provision audit
    // is emitted exactly once across retries.
    let outcome = state.hosted_auth.provision_scim_user(
        &client,
        req.principal_uid,
        &external_id_hash,
        req.user_name.as_deref(),
        now,
    );
    let record = outcome.record;

    if outcome.created {
        let event = scim_user_event(
            &client,
            AuditAction::AuthScimUserProvision,
            &record.id,
            record.principal_uid,
            "provisioned",
        );
        append_audit(state, event).await?;
    }

    // A create that immediately requests `active:false` deactivates and revokes.
    if req.active == Some(false) {
        return deactivate_and_revoke(state, &client, &external_id_hash, now).await;
    }

    Ok(user_response(StatusCode::CREATED, &client, &record))
}

async fn patch_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
    Json(req): Json<ScimUserPatchRequest>,
) -> Response {
    match patch_user_inner(&state, &headers, user_id, req).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn patch_user_inner(
    state: &ServerState,
    headers: &HeaderMap,
    user_id: String,
    req: ScimUserPatchRequest,
) -> Result<Response, ScimRouteError> {
    let client = authenticate(state, headers).await?;

    if !valid_resource_id(&user_id)
        || !valid_external_id(&req.external_id)
        || !valid_name_hint(req.user_name.as_deref())
    {
        return Err(ScimRouteError::InvalidRequest);
    }
    let external_id_hash = scim_external_id_hash(headers, &req.external_id)?;
    let user_uuid = Uuid::parse_str(&user_id).map_err(|_| ScimRouteError::InvalidRequest)?;
    require_scim_user_target(state, &client, user_uuid, &external_id_hash)?;
    let now = current_unix_time();

    // active:false deactivates and revokes hosted access for the principal.
    // `deactivate_and_revoke` resolves the principal and rejects unknown users.
    if req.active == Some(false) {
        return deactivate_and_revoke(state, &client, &external_id_hash, now).await;
    }

    let outcome = state
        .hosted_auth
        .update_scim_user(
            &client,
            &external_id_hash,
            req.user_name.as_deref(),
            req.active,
            now,
        )
        .ok_or(ScimRouteError::UnknownResource)?;
    let record = outcome.record;

    if outcome.changed {
        let event = scim_user_event(
            &client,
            AuditAction::AuthScimUserUpdate,
            &record.id,
            record.principal_uid,
            "updated",
        );
        append_audit(state, event).await?;
    }

    Ok(user_response(StatusCode::OK, &client, &record))
}

async fn delete_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
    Json(req): Json<ScimUserDeleteRequest>,
) -> Response {
    match delete_user_inner(&state, &headers, user_id, req).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn delete_user_inner(
    state: &ServerState,
    headers: &HeaderMap,
    user_id: String,
    req: ScimUserDeleteRequest,
) -> Result<Response, ScimRouteError> {
    let client = authenticate(state, headers).await?;

    // The path id is validated as a bounded uuid string; the user is looked up
    // and mutated by `external_id` within the tenant (same as PATCH).
    if !valid_resource_id(&user_id) || !valid_external_id(&req.external_id) {
        return Err(ScimRouteError::InvalidRequest);
    }
    let external_id_hash = scim_external_id_hash(headers, &req.external_id)?;
    let user_uuid = Uuid::parse_str(&user_id).map_err(|_| ScimRouteError::InvalidRequest)?;
    require_scim_user_target(state, &client, user_uuid, &external_id_hash)?;
    let now = current_unix_time();
    let outcome = state
        .hosted_auth
        .deactivate_scim_user(&client, &external_id_hash, now)
        .ok_or(ScimRouteError::UnknownResource)?;
    let record = outcome.record;
    let summary = state.hosted_auth.revoke_principal_hosted_access(
        &client.org_id,
        &client.repo_id,
        record.principal_uid,
        now,
    );

    if outcome.deactivated || revocation_summary_changed(&summary) {
        let event = scim_user_deactivate_event(&client, &record.id, record.principal_uid, &summary);
        append_audit(state, event).await?;
    }

    Ok(public_no_content())
}

async fn create_group(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ScimGroupCreateRequest>,
) -> Response {
    match create_group_inner(&state, &headers, req).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn create_group_inner(
    state: &ServerState,
    headers: &HeaderMap,
    req: ScimGroupCreateRequest,
) -> Result<Response, ScimRouteError> {
    let client = authenticate(state, headers).await?;

    if !valid_external_id(&req.external_id) || !valid_name_hint(req.display_name.as_deref()) {
        return Err(ScimRouteError::InvalidRequest);
    }
    let external_id_hash = scim_external_id_hash(headers, &req.external_id)?;
    let now = current_unix_time();

    // Idempotent on `(tenant, provider, external_id)`; `created` flags the first
    // insertion so the provision audit fires exactly once across retries.
    let outcome = state.hosted_auth.provision_scim_group(
        &client,
        req.local_gid,
        &external_id_hash,
        req.display_name.as_deref(),
        now,
    );
    let record = outcome.record;

    if outcome.created {
        let event = scim_group_event(
            &client,
            AuditAction::AuthScimGroupProvision,
            &record.id,
            "provisioned",
            None,
        );
        append_audit(state, event).await?;
    }

    Ok(group_response(StatusCode::CREATED, &client, &record, 0, 0))
}

async fn patch_group(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(group_id): Path<String>,
    Json(req): Json<ScimGroupPatchRequest>,
) -> Response {
    match patch_group_inner(&state, &headers, group_id, req).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn patch_group_inner(
    state: &ServerState,
    headers: &HeaderMap,
    group_id: String,
    req: ScimGroupPatchRequest,
) -> Result<Response, ScimRouteError> {
    let client = authenticate(state, headers).await?;

    if !valid_resource_id(&group_id)
        || !valid_external_id(&req.external_id)
        || !valid_name_hint(req.display_name.as_deref())
        || req.members_add.len() > MAX_GROUP_MEMBER_OPS
        || req.members_remove.len() > MAX_GROUP_MEMBER_OPS
    {
        return Err(ScimRouteError::InvalidRequest);
    }
    for member in req.members_add.iter().chain(req.members_remove.iter()) {
        if !valid_optional_external_id(member.group_external_id.as_deref()) {
            return Err(ScimRouteError::InvalidRequest);
        }
    }
    if has_overlapping_member_ops(&req.members_add, &req.members_remove) {
        return Err(ScimRouteError::InvalidRequest);
    }
    for member in req.members_add.iter().chain(req.members_remove.iter()) {
        require_scim_principal(state, &client, member.principal_uid)?;
    }
    let group_uuid = Uuid::parse_str(&group_id).map_err(|_| ScimRouteError::InvalidRequest)?;
    let external_id_hash = scim_external_id_hash(headers, &req.external_id)?;
    let now = current_unix_time();

    // Resolve the PATCH target read-only by its path resource id within the
    // tenant; an unknown id is rejected without creating any group. The body
    // `external_id` must agree with the resolved group's binding.
    let group = state
        .hosted_auth
        .find_scim_group(&client, group_uuid)
        .filter(|group| group.external_id_hash == external_id_hash)
        .ok_or(ScimRouteError::UnknownResource)?;

    // Apply a bounded attribute update only when a new value is supplied.
    let mut group_update_changed = false;
    let group = if req.display_name.is_some() {
        let outcome = state
            .hosted_auth
            .update_scim_group(&client, group_uuid, req.display_name.as_deref(), now)
            .ok_or(ScimRouteError::UnknownResource)?;
        group_update_changed = outcome.changed;
        outcome.record
    } else {
        group
    };

    let mut members_added = 0;
    for member in &req.members_add {
        let outcome =
            state
                .hosted_auth
                .add_scim_group_member(&client, group_uuid, member.principal_uid, now);
        if outcome.changed {
            members_added += 1;
            let record = outcome.record;
            let event = scim_membership_event(
                &client,
                AuditAction::AuthScimGroupMemberAdd,
                &record.id,
                record.principal_uid,
                "member_added",
            );
            append_audit(state, event).await?;
        }
    }

    let mut members_removed = 0;
    for member in &req.members_remove {
        if let Some(outcome) = state.hosted_auth.remove_scim_group_member(
            &client,
            group_uuid,
            member.principal_uid,
            now,
        ) && outcome.changed
        {
            members_removed += 1;
            let record = outcome.record;
            let event = scim_membership_event(
                &client,
                AuditAction::AuthScimGroupMemberRemove,
                &record.id,
                record.principal_uid,
                "member_removed",
            );
            append_audit(state, event).await?;
        }
    }

    if group_update_changed || members_added > 0 || members_removed > 0 {
        let event = scim_group_event(
            &client,
            AuditAction::AuthScimGroupUpdate,
            &group.id,
            "updated",
            Some((members_added, members_removed)),
        );
        append_audit(state, event).await?;
    }

    Ok(group_response(
        StatusCode::OK,
        &client,
        &group,
        members_added,
        members_removed,
    ))
}

// ---------------------------------------------------------------------------
// Shared mutation helpers.
// ---------------------------------------------------------------------------

/// Deactivate a SCIM user (by external id) and revoke hosted access, returning
/// the redacted success response. Idempotent: revoking again yields zero counts.
async fn deactivate_and_revoke(
    state: &ServerState,
    client: &ScimClientContext,
    external_id_hash: &str,
    now: u64,
) -> Result<Response, ScimRouteError> {
    let outcome = state
        .hosted_auth
        .deactivate_scim_user(client, external_id_hash, now)
        .ok_or(ScimRouteError::UnknownResource)?;
    let record = outcome.record;
    let summary = state.hosted_auth.revoke_principal_hosted_access(
        &client.org_id,
        &client.repo_id,
        record.principal_uid,
        now,
    );
    if outcome.deactivated || revocation_summary_changed(&summary) {
        let event = scim_user_deactivate_event(client, &record.id, record.principal_uid, &summary);
        append_audit(state, event).await?;
    }
    Ok(user_response(StatusCode::OK, client, &record))
}

async fn append_audit(state: &ServerState, event: NewAuditEvent) -> Result<(), ScimRouteError> {
    state
        .audit
        .append(event)
        .await
        .map(|_| ())
        .map_err(|_| ScimRouteError::Audit)
}

fn revocation_summary_changed(summary: &ScimRevocationSummary) -> bool {
    summary.access_tokens_revoked > 0
        || summary.refresh_tokens_revoked > 0
        || summary.refresh_families_revoked > 0
}

fn require_scim_principal(
    state: &ServerState,
    client: &ScimClientContext,
    principal_uid: u32,
) -> Result<(), ScimRouteError> {
    if state
        .hosted_auth
        .scim_principal_is_known(&client.org_id, &client.repo_id, principal_uid)
    {
        Ok(())
    } else {
        Err(ScimRouteError::UnknownResource)
    }
}

fn require_scim_user_target(
    state: &ServerState,
    client: &ScimClientContext,
    user_id: Uuid,
    external_id_hash: &str,
) -> Result<(), ScimRouteError> {
    state
        .hosted_auth
        .find_scim_user(client, user_id)
        .filter(|user| user.external_id_hash == external_id_hash)
        .map(|_| ())
        .ok_or(ScimRouteError::UnknownResource)
}

fn has_overlapping_member_ops(
    members_add: &[ScimMemberRef],
    members_remove: &[ScimMemberRef],
) -> bool {
    members_add.iter().any(|add| {
        members_remove
            .iter()
            .any(|remove| remove.principal_uid == add.principal_uid)
    })
}

// ---------------------------------------------------------------------------
// Audit event builders (bounded, redacted).
// ---------------------------------------------------------------------------

fn scim_user_event(
    client: &ScimClientContext,
    action: AuditAction,
    resource_id: &Uuid,
    principal_uid: u32,
    reason: &str,
) -> NewAuditEvent {
    scim_tenant_event(
        client,
        action,
        AuditResource::id(AuditResourceKind::ScimUser, resource_id.to_string()),
        reason,
    )
    .with_detail("principal_uid", principal_uid)
}

fn scim_user_deactivate_event(
    client: &ScimClientContext,
    resource_id: &Uuid,
    principal_uid: u32,
    summary: &ScimRevocationSummary,
) -> NewAuditEvent {
    scim_user_event(
        client,
        AuditAction::AuthScimUserDeactivate,
        resource_id,
        principal_uid,
        "deactivated",
    )
    .with_detail("access_tokens_revoked", summary.access_tokens_revoked)
    .with_detail("refresh_tokens_revoked", summary.refresh_tokens_revoked)
    .with_detail("refresh_families_revoked", summary.refresh_families_revoked)
}

fn scim_group_event(
    client: &ScimClientContext,
    action: AuditAction,
    resource_id: &Uuid,
    reason: &str,
    member_counts: Option<(usize, usize)>,
) -> NewAuditEvent {
    let mut event = scim_tenant_event(
        client,
        action,
        AuditResource::id(AuditResourceKind::ScimGroup, resource_id.to_string()),
        reason,
    );
    if let Some((added, removed)) = member_counts {
        event = event
            .with_detail("members_added", added)
            .with_detail("members_removed", removed);
    }
    event
}

fn scim_membership_event(
    client: &ScimClientContext,
    action: AuditAction,
    resource_id: &Uuid,
    principal_uid: u32,
    reason: &str,
) -> NewAuditEvent {
    scim_tenant_event(
        client,
        action,
        AuditResource::id(
            AuditResourceKind::ScimGroupMembership,
            resource_id.to_string(),
        ),
        reason,
    )
    .with_detail("principal_uid", principal_uid)
}

fn scim_tenant_event(
    client: &ScimClientContext,
    action: AuditAction,
    resource: AuditResource,
    reason: &str,
) -> NewAuditEvent {
    NewAuditEvent::new(scim_audit_actor(), action, resource)
        .with_detail("org_id", &client.org_id)
        .with_detail("repo_id", &client.repo_id)
        .with_detail("provider_key", &client.provider_key)
        .with_detail("reason", reason)
}

// ---------------------------------------------------------------------------
// Response builders (redacted).
// ---------------------------------------------------------------------------

fn user_response(
    status: StatusCode,
    client: &ScimClientContext,
    record: &crate::auth::hosted::ScimUserRecord,
) -> Response {
    (
        status,
        Json(ScimUserResponse {
            id: record.id.to_string(),
            org_id: client.org_id.to_string(),
            repo_id: client.repo_id.to_string(),
            principal_uid: record.principal_uid,
            active: record.active,
        }),
    )
        .into_response()
}

fn group_response(
    status: StatusCode,
    client: &ScimClientContext,
    record: &crate::auth::hosted::ScimGroupRecord,
    members_added: usize,
    members_removed: usize,
) -> Response {
    (
        status,
        Json(ScimGroupResponse {
            id: record.id.to_string(),
            org_id: client.org_id.to_string(),
            repo_id: client.repo_id.to_string(),
            local_gid: record.local_gid,
            active: record.active,
            members_added,
            members_removed,
        }),
    )
        .into_response()
}

fn public_no_content() -> Response {
    StatusCode::NO_CONTENT.into_response()
}

// ---------------------------------------------------------------------------
// Validation.
// ---------------------------------------------------------------------------

fn valid_external_id(value: &str) -> bool {
    bounded_auth_field(value) && value.len() <= MAX_EXTERNAL_ID_BYTES
}

fn valid_optional_external_id(value: Option<&str>) -> bool {
    value.is_none_or(valid_external_id)
}

fn valid_name_hint(value: Option<&str>) -> bool {
    value.is_none_or(|value| !value.is_empty() && value.len() <= MAX_NAME_HINT_BYTES)
}

fn valid_resource_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_RESOURCE_ID_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::{AuditEvent, InMemoryAuditStore};
    use crate::auth::hosted::{HostedSessionIdentity, InMemoryHostedAuthStore, ScimClientConfig};
    use crate::idempotency::InMemoryIdempotencyStore;
    use crate::review::InMemoryReviewStore;
    use crate::server::core::LocalCoreRuntime;
    use crate::server::repo_context::InMemoryTenantRepoResolver;
    use crate::server::{ServerLocalDb, ServerState};
    use crate::workspace::InMemoryWorkspaceMetadataStore;
    use axum::http::HeaderValue;
    use std::sync::Arc;

    const PROVIDER_KEY: &str = "scim_provider";
    const ORG: &str = "org_scim";
    const REPO: &str = "repo_scim";
    const RAW_TOKEN: &str = "raw-scim-bearer-secret";
    const RAW_EXTERNAL_ID: &str = "scim:demo:external-user-id";

    fn test_state() -> Arc<ServerState> {
        let db = Arc::new(crate::db::StratumDb::open_memory());
        Arc::new(ServerState {
            core: LocalCoreRuntime::shared_from_arc(db.clone()),
            db: ServerLocalDb::available(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(InMemoryAuditStore::new()),
            review: Arc::new(InMemoryReviewStore::new()),
            hosted_auth: Arc::new(InMemoryHostedAuthStore::new()),
            tenant_repos: Arc::new(InMemoryTenantRepoResolver::new()),
            secret_replay_kms: None,
        })
    }

    /// Build a tenant-bound, SCIM-client-enabled state for the happy path.
    fn provisioned_state() -> Arc<ServerState> {
        let state = test_state();
        let org_id = OrgId::new(ORG).unwrap();
        let repo_id = RepoId::new(REPO).unwrap();
        state.bind_tenant_repo_for_test(org_id.clone(), repo_id.clone());
        state
            .hosted_auth
            .bind_scim_client_for_test(ScimClientConfig {
                provider_key: PROVIDER_KEY.to_string(),
                org_id: org_id.clone(),
                repo_id: repo_id.clone(),
                token_hash: hash_hosted_token_secret(RAW_TOKEN),
                enabled: true,
            });
        for principal_uid in [4242, 4343] {
            state.hosted_auth.bind_scim_principal_for_test(
                org_id.clone(),
                repo_id.clone(),
                principal_uid,
            );
        }
        state
    }

    fn scim_headers(token: &str, org: &str, repo: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_str(&format!("Scim-Bearer {token}")).unwrap(),
        );
        headers.insert("x-stratum-org", HeaderValue::from_str(org).unwrap());
        headers.insert("x-stratum-repo", HeaderValue::from_str(repo).unwrap());
        headers
    }

    fn valid_headers() -> HeaderMap {
        scim_headers(RAW_TOKEN, ORG, REPO)
    }

    async fn audit_events(state: &ServerState) -> Vec<AuditEvent> {
        state.audit.list_recent(200).await.expect("audit events")
    }

    async fn response_json(response: Response) -> serde_json::Value {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        serde_json::from_slice(&body).expect("response body is json")
    }

    fn user_create(external_id: &str, principal_uid: u32) -> ScimUserCreateRequest {
        ScimUserCreateRequest {
            external_id: external_id.to_string(),
            principal_uid,
            user_name: Some("demo-user".to_string()),
            active: None,
        }
    }

    fn hosted_identity(principal_uid: u32) -> HostedSessionIdentity {
        HostedSessionIdentity {
            session_id: Uuid::new_v4(),
            org_id: OrgId::new(ORG).unwrap(),
            repo_id: RepoId::new(REPO).unwrap(),
            uid: principal_uid,
            username: "hosted-user".to_string(),
            gid: principal_uid,
            groups: vec![principal_uid],
            external_identity_id: "saml:scim:hosted-user".to_string(),
        }
    }

    #[tokio::test]
    async fn scim_request_denied_when_disabled_without_secret_leak() {
        // No SCIM client bound: authentication must fail closed.
        let state = test_state();
        state.bind_tenant_repo_for_test(OrgId::new(ORG).unwrap(), RepoId::new(REPO).unwrap());

        let response = create_user(
            State(state.clone()),
            valid_headers(),
            Json(user_create(RAW_EXTERNAL_ID, 4242)),
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = response_json(response).await.to_string();
        let events = audit_events(&state).await;
        let audit_text = format!("{events:?}");
        for forbidden in [RAW_TOKEN, RAW_EXTERNAL_ID] {
            assert!(!body.contains(forbidden), "response leaked {forbidden}");
            assert!(!audit_text.contains(forbidden), "audit leaked {forbidden}");
        }
        assert!(
            events
                .iter()
                .any(|event| event.action == AuditAction::AuthScimRequestDenied),
            "expected denial audit"
        );
    }

    #[tokio::test]
    async fn scim_request_rejects_missing_or_invalid_scim_bearer() {
        let state = provisioned_state();
        let mut headers = valid_headers();
        // Wrong scheme: a plain Bearer token must not authenticate SCIM.
        headers.insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {RAW_TOKEN}")).unwrap(),
        );

        let response = create_user(
            State(state),
            headers,
            Json(user_create(RAW_EXTERNAL_ID, 4242)),
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn scim_request_rejects_tenant_mismatch_without_local_fallback() {
        let state = provisioned_state();
        state.bind_tenant_repo_for_test(
            OrgId::new("org_other").unwrap(),
            RepoId::new(REPO).unwrap(),
        );
        // Correct token, but headers name a different org than the client binding.
        let headers = scim_headers(RAW_TOKEN, "org_other", REPO);

        let response = create_user(
            State(state),
            headers,
            Json(user_create(RAW_EXTERNAL_ID, 4242)),
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn scim_user_create_update_deactivate_is_tenant_scoped() {
        let state = provisioned_state();

        let created = create_user(
            State(state.clone()),
            valid_headers(),
            Json(user_create(RAW_EXTERNAL_ID, 4242)),
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED);
        let created_body = response_json(created).await;
        let user_id = created_body["id"].as_str().unwrap().to_string();
        assert_eq!(created_body["org_id"], ORG);
        assert_eq!(created_body["principal_uid"], 4242);

        let updated = patch_user(
            State(state.clone()),
            valid_headers(),
            Path(user_id.clone()),
            Json(ScimUserPatchRequest {
                external_id: RAW_EXTERNAL_ID.to_string(),
                user_name: Some("renamed-user".to_string()),
                active: Some(true),
            }),
        )
        .await;
        assert_eq!(updated.status(), StatusCode::OK);
        assert_eq!(response_json(updated).await["active"], true);

        let deactivated = patch_user(
            State(state.clone()),
            valid_headers(),
            Path(user_id),
            Json(ScimUserPatchRequest {
                external_id: RAW_EXTERNAL_ID.to_string(),
                user_name: None,
                active: Some(false),
            }),
        )
        .await;
        assert_eq!(deactivated.status(), StatusCode::OK);
        assert_eq!(response_json(deactivated).await["active"], false);
    }

    #[tokio::test]
    async fn scim_user_path_id_must_match_body_external_id() {
        let state = provisioned_state();

        let first = create_user(
            State(state.clone()),
            valid_headers(),
            Json(user_create("scim:demo:first-user", 4242)),
        )
        .await;
        assert_eq!(first.status(), StatusCode::CREATED);
        let first_id = response_json(first).await["id"]
            .as_str()
            .unwrap()
            .to_string();

        let second = create_user(
            State(state.clone()),
            valid_headers(),
            Json(user_create("scim:demo:second-user", 4343)),
        )
        .await;
        assert_eq!(second.status(), StatusCode::CREATED);

        let mismatched_patch = patch_user(
            State(state.clone()),
            valid_headers(),
            Path(first_id.clone()),
            Json(ScimUserPatchRequest {
                external_id: "scim:demo:second-user".to_string(),
                user_name: None,
                active: Some(false),
            }),
        )
        .await;
        assert_eq!(mismatched_patch.status(), StatusCode::NOT_FOUND);

        let mismatched_delete = delete_user(
            State(state),
            valid_headers(),
            Path(first_id),
            Json(ScimUserDeleteRequest {
                external_id: "scim:demo:second-user".to_string(),
            }),
        )
        .await;
        assert_eq!(mismatched_delete.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn scim_user_create_rejects_unknown_principal_without_binding() {
        let state = provisioned_state();

        let response = create_user(
            State(state.clone()),
            valid_headers(),
            Json(user_create("scim:demo:unknown-principal", 9999)),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(state.hosted_auth.scim_user_count(), 0);
    }

    #[tokio::test]
    async fn scim_user_create_accepts_principal_known_from_hosted_session() {
        let state = provisioned_state();
        let now = current_unix_time();
        let identity = hosted_identity(5454);
        state
            .hosted_auth
            .issue_access_token(&identity, now, now + 900);

        let response = create_user(
            State(state),
            valid_headers(),
            Json(user_create("scim:demo:session-known-principal", 5454)),
        )
        .await;

        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response_json(response).await["principal_uid"], 5454);
    }

    #[tokio::test]
    async fn scim_user_deactivate_revokes_hosted_access_and_refresh() {
        let state = provisioned_state();
        let now = current_unix_time();
        let identity = hosted_identity(4242);
        let access = state
            .hosted_auth
            .issue_access_token(&identity, now, now + 900);
        let _refresh = state
            .hosted_auth
            .issue_refresh_token(&identity, now, now + 3600);
        // Sanity: the access token validates before deactivation.
        assert!(
            state
                .hosted_auth
                .validate_access_token_at(&access.raw_secret, now)
                .is_some()
        );

        let created = create_user(
            State(state.clone()),
            valid_headers(),
            Json(user_create(RAW_EXTERNAL_ID, 4242)),
        )
        .await;
        let user_id = response_json(created).await["id"]
            .as_str()
            .unwrap()
            .to_string();

        let deleted = delete_user(
            State(state.clone()),
            valid_headers(),
            Path(user_id),
            Json(ScimUserDeleteRequest {
                external_id: RAW_EXTERNAL_ID.to_string(),
            }),
        )
        .await;
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);

        // After deprovisioning the access token no longer validates.
        assert!(
            state
                .hosted_auth
                .validate_access_token_at(&access.raw_secret, now)
                .is_none()
        );
    }

    #[tokio::test]
    async fn scim_group_membership_add_update_remove_is_tenant_scoped() {
        let state = provisioned_state();

        let group_external = "scim:demo:group-external-id";
        let created = create_group(
            State(state.clone()),
            valid_headers(),
            Json(ScimGroupCreateRequest {
                external_id: group_external.to_string(),
                local_gid: 6000,
                display_name: Some("Engineers".to_string()),
            }),
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED);
        let group_id = response_json(created).await["id"]
            .as_str()
            .unwrap()
            .to_string();

        let patched = patch_group(
            State(state.clone()),
            valid_headers(),
            Path(group_id.clone()),
            Json(ScimGroupPatchRequest {
                external_id: group_external.to_string(),
                members_add: vec![
                    ScimMemberRef {
                        group_external_id: None,
                        principal_uid: 4242,
                    },
                    ScimMemberRef {
                        group_external_id: None,
                        principal_uid: 4343,
                    },
                ],
                members_remove: vec![],
                display_name: Some("Engineering".to_string()),
            }),
        )
        .await;
        assert_eq!(patched.status(), StatusCode::OK);
        assert_eq!(response_json(patched).await["members_added"], 2);

        let removed = patch_group(
            State(state.clone()),
            valid_headers(),
            Path(group_id),
            Json(ScimGroupPatchRequest {
                external_id: group_external.to_string(),
                members_add: vec![],
                members_remove: vec![ScimMemberRef {
                    group_external_id: None,
                    principal_uid: 4242,
                }],
                display_name: None,
            }),
        )
        .await;
        assert_eq!(removed.status(), StatusCode::OK);
        assert_eq!(response_json(removed).await["members_removed"], 1);
    }

    #[tokio::test]
    async fn scim_group_patch_unknown_group_404s_without_creating_state() {
        let state = provisioned_state();

        let patched = patch_group(
            State(state.clone()),
            valid_headers(),
            Path(Uuid::new_v4().to_string()),
            Json(ScimGroupPatchRequest {
                external_id: "scim:demo:unknown-group".to_string(),
                members_add: vec![ScimMemberRef {
                    group_external_id: None,
                    principal_uid: 4242,
                }],
                members_remove: vec![],
                display_name: Some("Phantom".to_string()),
            }),
        )
        .await;

        assert_eq!(patched.status(), StatusCode::NOT_FOUND);
        // The PATCH must not have created a phantom group or any membership.
        assert_eq!(state.hosted_auth.scim_group_count(), 0);
        assert_eq!(state.hosted_auth.scim_group_member_count(), 0);
    }

    #[tokio::test]
    async fn scim_group_membership_rejects_unknown_principal_without_binding() {
        let state = provisioned_state();
        let group_external = "scim:demo:principal-check-group";
        let created = create_group(
            State(state.clone()),
            valid_headers(),
            Json(ScimGroupCreateRequest {
                external_id: group_external.to_string(),
                local_gid: 6000,
                display_name: Some("Principal Check".to_string()),
            }),
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED);
        let group_id = response_json(created).await["id"]
            .as_str()
            .unwrap()
            .to_string();

        let patched = patch_group(
            State(state.clone()),
            valid_headers(),
            Path(group_id),
            Json(ScimGroupPatchRequest {
                external_id: group_external.to_string(),
                members_add: vec![ScimMemberRef {
                    group_external_id: None,
                    principal_uid: 9999,
                }],
                members_remove: vec![],
                display_name: None,
            }),
        )
        .await;

        assert_eq!(patched.status(), StatusCode::NOT_FOUND);
        assert_eq!(state.hosted_auth.scim_group_member_count(), 0);
    }

    #[tokio::test]
    async fn scim_group_patch_rejects_overlapping_add_remove_member_ops() {
        let state = provisioned_state();
        let group_external = "scim:demo:overlap-group";
        let created = create_group(
            State(state.clone()),
            valid_headers(),
            Json(ScimGroupCreateRequest {
                external_id: group_external.to_string(),
                local_gid: 6000,
                display_name: Some("Overlap".to_string()),
            }),
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED);
        let group_id = response_json(created).await["id"]
            .as_str()
            .unwrap()
            .to_string();

        let patched = patch_group(
            State(state.clone()),
            valid_headers(),
            Path(group_id),
            Json(ScimGroupPatchRequest {
                external_id: group_external.to_string(),
                members_add: vec![ScimMemberRef {
                    group_external_id: None,
                    principal_uid: 4242,
                }],
                members_remove: vec![ScimMemberRef {
                    group_external_id: None,
                    principal_uid: 4242,
                }],
                display_name: None,
            }),
        )
        .await;

        assert_eq!(patched.status(), StatusCode::BAD_REQUEST);
        assert_eq!(state.hosted_auth.scim_group_member_count(), 0);
    }

    #[tokio::test]
    async fn scim_create_retry_is_idempotent_without_duplicate_audit() {
        let state = provisioned_state();

        let first = create_user(
            State(state.clone()),
            valid_headers(),
            Json(user_create(RAW_EXTERNAL_ID, 4242)),
        )
        .await;
        let first_id = response_json(first).await["id"]
            .as_str()
            .unwrap()
            .to_string();

        let second = create_user(
            State(state.clone()),
            valid_headers(),
            Json(user_create(RAW_EXTERNAL_ID, 4242)),
        )
        .await;
        let second_id = response_json(second).await["id"]
            .as_str()
            .unwrap()
            .to_string();

        assert_eq!(
            first_id, second_id,
            "retry must return the same resource id"
        );
        assert_eq!(
            state.hosted_auth.scim_user_count(),
            1,
            "retry must not create a duplicate row"
        );

        let provisions = audit_events(&state)
            .await
            .into_iter()
            .filter(|event| event.action == AuditAction::AuthScimUserProvision)
            .count();
        assert_eq!(
            provisions, 1,
            "retry must not duplicate the provision audit"
        );
    }

    #[tokio::test]
    async fn scim_user_deactivate_retry_is_idempotent_without_duplicate_audit() {
        let state = provisioned_state();

        let created = create_user(
            State(state.clone()),
            valid_headers(),
            Json(user_create(RAW_EXTERNAL_ID, 4242)),
        )
        .await;
        let user_id = response_json(created).await["id"]
            .as_str()
            .unwrap()
            .to_string();

        for _ in 0..2 {
            let deactivated = patch_user(
                State(state.clone()),
                valid_headers(),
                Path(user_id.clone()),
                Json(ScimUserPatchRequest {
                    external_id: RAW_EXTERNAL_ID.to_string(),
                    user_name: None,
                    active: Some(false),
                }),
            )
            .await;
            assert_eq!(deactivated.status(), StatusCode::OK);
            assert_eq!(response_json(deactivated).await["active"], false);
        }

        let deactivations = audit_events(&state)
            .await
            .into_iter()
            .filter(|event| event.action == AuditAction::AuthScimUserDeactivate)
            .count();
        assert_eq!(
            deactivations, 1,
            "retry must not duplicate the deactivate audit"
        );
    }

    #[tokio::test]
    async fn scim_group_membership_retry_is_idempotent_without_duplicate_audit() {
        let state = provisioned_state();
        let group_external = "scim:demo:retry-group-external-id";
        let created = create_group(
            State(state.clone()),
            valid_headers(),
            Json(ScimGroupCreateRequest {
                external_id: group_external.to_string(),
                local_gid: 6000,
                display_name: Some("Retry Group".to_string()),
            }),
        )
        .await;
        let group_id = response_json(created).await["id"]
            .as_str()
            .unwrap()
            .to_string();

        for expected_added in [1, 0] {
            let patched = patch_group(
                State(state.clone()),
                valid_headers(),
                Path(group_id.clone()),
                Json(ScimGroupPatchRequest {
                    external_id: group_external.to_string(),
                    members_add: vec![ScimMemberRef {
                        group_external_id: None,
                        principal_uid: 4242,
                    }],
                    members_remove: vec![],
                    display_name: None,
                }),
            )
            .await;
            assert_eq!(patched.status(), StatusCode::OK);
            assert_eq!(
                response_json(patched).await["members_added"],
                expected_added
            );
        }

        for expected_removed in [1, 0] {
            let patched = patch_group(
                State(state.clone()),
                valid_headers(),
                Path(group_id.clone()),
                Json(ScimGroupPatchRequest {
                    external_id: group_external.to_string(),
                    members_add: vec![],
                    members_remove: vec![ScimMemberRef {
                        group_external_id: None,
                        principal_uid: 4242,
                    }],
                    display_name: None,
                }),
            )
            .await;
            assert_eq!(patched.status(), StatusCode::OK);
            assert_eq!(
                response_json(patched).await["members_removed"],
                expected_removed
            );
        }

        let events = audit_events(&state).await;
        let add_audits = events
            .iter()
            .filter(|event| event.action == AuditAction::AuthScimGroupMemberAdd)
            .count();
        let remove_audits = events
            .iter()
            .filter(|event| event.action == AuditAction::AuthScimGroupMemberRemove)
            .count();
        assert_eq!(add_audits, 1, "retry must not duplicate add audit");
        assert_eq!(remove_audits, 1, "retry must not duplicate remove audit");
    }

    #[test]
    fn scim_external_id_hash_is_keyed_by_scim_bearer_token() {
        let raw_external_id = "predictable@example.com";

        let first = scim_external_id_hash_for_token("first-scim-secret", raw_external_id);
        let second = scim_external_id_hash_for_token("second-scim-secret", raw_external_id);
        let plain = hash_hosted_token_secret(raw_external_id);

        assert_eq!(first.len(), 64);
        assert_ne!(first, second);
        assert_ne!(first, plain);
    }

    #[tokio::test]
    async fn scim_public_errors_and_audit_redact_bodies_tokens_and_external_ids() {
        let state = provisioned_state();

        let created = create_user(
            State(state.clone()),
            valid_headers(),
            Json(user_create(RAW_EXTERNAL_ID, 4242)),
        )
        .await;
        let body = response_json(created).await.to_string();

        // Trigger a not-found error path with a real external id in the body.
        let missing = patch_user(
            State(state.clone()),
            valid_headers(),
            Path(Uuid::new_v4().to_string()),
            Json(ScimUserPatchRequest {
                external_id: "scim:demo:unknown-external-id".to_string(),
                user_name: None,
                active: Some(false),
            }),
        )
        .await;
        let error_body = response_json(missing).await.to_string();

        let events = audit_events(&state).await;
        let audit_text = format!("{events:?}");
        for forbidden in [
            RAW_TOKEN,
            RAW_EXTERNAL_ID,
            "scim:demo:unknown-external-id",
            &hash_hosted_token_secret(RAW_EXTERNAL_ID),
        ] {
            assert!(!body.contains(forbidden), "response leaked {forbidden}");
            assert!(!error_body.contains(forbidden), "error leaked {forbidden}");
            assert!(!audit_text.contains(forbidden), "audit leaked {forbidden}");
        }
    }

    #[tokio::test]
    async fn non_scim_credentials_cannot_authenticate_scim_routes() {
        let state = provisioned_state();
        for scheme in ["Bearer", "Stratum-Session", "User"] {
            let mut headers = valid_headers();
            headers.insert(
                "authorization",
                HeaderValue::from_str(&format!("{scheme} {RAW_TOKEN}")).unwrap(),
            );
            let response = create_user(
                State(state.clone()),
                headers,
                Json(user_create(RAW_EXTERNAL_ID, 4242)),
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{scheme} must not authenticate SCIM"
            );
        }
    }
}
