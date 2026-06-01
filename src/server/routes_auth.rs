use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use super::AppState;
use super::ServerRuntimeKind;
use super::ServerState;
use crate::audit::{AuditAction, AuditActor, AuditResource, AuditResourceKind, NewAuditEvent};
use crate::auth::hosted::{
    HostedOidcVerificationError, HostedOidcVerificationRequest, HostedSamlVerificationError,
    HostedSamlVerificationRequest, HostedSessionIdentity, RefreshTokenError, RefreshTokenRecord,
    VerifiedHostedOidcClaims, VerifiedHostedSamlClaims,
};
use crate::backend::{OrgId, RepoId};
use crate::server::repo_context::TenantRepoResolver;

const HOSTED_ACCESS_TOKEN_TTL_SECS: u64 = 15 * 60;
const HOSTED_REFRESH_TOKEN_TTL_SECS: u64 = 30 * 24 * 60 * 60;
const MAX_AUTH_FIELD_BYTES: usize = 4096;
const MAX_PROVIDER_KEY_BYTES: usize = 128;

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub username: String,
    pub uid: u32,
    pub gid: u32,
    pub groups: Vec<u32>,
}

#[derive(Deserialize)]
pub struct OidcLoginRequest {
    pub provider: String,
    pub authorization_code: String,
    pub redirect_uri: Option<String>,
    pub org_id: String,
    pub repo_id: String,
}

#[derive(Deserialize)]
pub struct SamlLoginRequest {
    pub provider: String,
    pub saml_response: String,
    pub relay_state: Option<String>,
    pub org_id: String,
    pub repo_id: String,
}

#[derive(Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
}

#[derive(Deserialize)]
pub struct RefreshRevokeRequest {
    pub refresh_token: String,
}

#[derive(Serialize)]
pub struct HostedTokenResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub token_type: String,
    pub expires_in: u64,
    pub refresh_expires_in: u64,
    pub org_id: String,
    pub repo_id: String,
    pub uid: u32,
    pub username: String,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/auth/login", post(login))
        .route("/auth/oidc/login", post(oidc_login))
        .route("/auth/saml/login", post(saml_login))
        .route("/auth/refresh", post(refresh))
        .route("/auth/refresh/revoke", post(refresh_revoke))
        .route("/health", axum::routing::get(health))
}

pub fn health_routes() -> Router<AppState> {
    Router::new().route("/health", axum::routing::get(health))
}

pub fn hosted_routes() -> Router<AppState> {
    Router::new()
        .route("/auth/oidc/login", post(oidc_login))
        .route("/auth/saml/login", post(saml_login))
        .route("/auth/refresh", post(refresh))
        .route("/auth/refresh/revoke", post(refresh_revoke))
}

async fn login(State(state): State<AppState>, Json(req): Json<LoginRequest>) -> impl IntoResponse {
    match state.core.login(&req.username).await {
        Ok(session) => (
            StatusCode::OK,
            Json(LoginResponse {
                username: session.username.clone(),
                uid: session.uid,
                gid: session.gid,
                groups: session.groups.clone(),
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
            .into_response(),
    }
}

async fn oidc_login(
    State(state): State<AppState>,
    Json(req): Json<OidcLoginRequest>,
) -> impl IntoResponse {
    match oidc_login_inner(&state, req).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(AuthRouteError::Disabled) => {
            public_error(StatusCode::UNAUTHORIZED, "hosted auth provider is disabled")
        }
        Err(AuthRouteError::TenantMismatch) => {
            public_error(StatusCode::UNAUTHORIZED, "hosted auth tenant mismatch")
        }
        Err(AuthRouteError::InvalidRequest) => {
            public_error(StatusCode::BAD_REQUEST, "invalid hosted auth request")
        }
        Err(AuthRouteError::Unauthorized) => {
            public_error(StatusCode::UNAUTHORIZED, "hosted auth verification failed")
        }
        Err(AuthRouteError::Audit) => public_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "hosted auth audit failed",
        ),
    }
}

async fn saml_login(
    State(state): State<AppState>,
    Json(req): Json<SamlLoginRequest>,
) -> impl IntoResponse {
    match saml_login_inner(&state, req).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(AuthRouteError::Disabled) => {
            public_error(StatusCode::UNAUTHORIZED, "hosted auth provider is disabled")
        }
        Err(AuthRouteError::TenantMismatch) => {
            public_error(StatusCode::UNAUTHORIZED, "hosted auth tenant mismatch")
        }
        Err(AuthRouteError::InvalidRequest) => {
            public_error(StatusCode::BAD_REQUEST, "invalid hosted auth request")
        }
        Err(AuthRouteError::Unauthorized) => {
            public_error(StatusCode::UNAUTHORIZED, "hosted auth verification failed")
        }
        Err(AuthRouteError::Audit) => public_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "hosted auth audit failed",
        ),
    }
}

async fn refresh(
    State(state): State<AppState>,
    Json(req): Json<RefreshRequest>,
) -> impl IntoResponse {
    if !bounded_auth_field(&req.refresh_token) {
        return public_error(StatusCode::UNAUTHORIZED, "invalid refresh token");
    }
    let now = current_unix_time();
    let rotated = match state.hosted_auth.rotate_refresh_token(
        &req.refresh_token,
        now,
        now.saturating_add(HOSTED_REFRESH_TOKEN_TTL_SECS),
    ) {
        Ok(rotated) => rotated,
        Err(error) => {
            if append_refresh_denial_audit(&state, &req.refresh_token, error)
                .await
                .is_err()
            {
                return public_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "hosted auth audit failed",
                );
            }
            return public_error(StatusCode::UNAUTHORIZED, "invalid refresh token");
        }
    };
    let Some(previous) = state
        .hosted_auth
        .refresh_token_for_secret(&req.refresh_token)
    else {
        return public_error(StatusCode::UNAUTHORIZED, "invalid refresh token");
    };
    let Some(identity) = state
        .hosted_auth
        .hosted_session_identity(rotated.token.session_id)
    else {
        return public_error(StatusCode::UNAUTHORIZED, "invalid refresh token");
    };
    if append_auth_audit(
        &state,
        refresh_token_event(
            &identity,
            AuditAction::AuthRefreshTokenRotate,
            &previous,
            "rotated",
        )
        .with_detail("successor_token_id", rotated.token.id),
    )
    .await
    .is_err()
    {
        return public_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "hosted auth audit failed",
        );
    }
    if append_auth_audit(&state, refresh_token_issue_event(&identity, &rotated.token))
        .await
        .is_err()
    {
        return public_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "hosted auth audit failed",
        );
    }
    let access = state.hosted_auth.issue_access_token(
        &identity,
        now,
        now.saturating_add(HOSTED_ACCESS_TOKEN_TTL_SECS),
    );

    (
        StatusCode::OK,
        Json(hosted_token_response(
            access.raw_secret,
            rotated.raw_secret,
            &identity,
        )),
    )
        .into_response()
}

async fn refresh_revoke(
    State(state): State<AppState>,
    Json(req): Json<RefreshRevokeRequest>,
) -> impl IntoResponse {
    if !bounded_auth_field(&req.refresh_token) {
        return public_error(StatusCode::UNAUTHORIZED, "invalid refresh token");
    }
    match state
        .hosted_auth
        .revoke_refresh_token_secret(&req.refresh_token, current_unix_time())
    {
        Ok(token) => {
            let Some(identity) = state.hosted_auth.hosted_session_identity(token.session_id) else {
                return public_error(StatusCode::UNAUTHORIZED, "invalid refresh token");
            };
            if append_auth_audit(
                &state,
                refresh_token_event(
                    &identity,
                    AuditAction::AuthRefreshTokenRevoke,
                    &token,
                    "revoked",
                ),
            )
            .await
            .is_err()
            {
                return public_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "hosted auth audit failed",
                );
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Err(
            RefreshTokenError::Invalid
            | RefreshTokenError::Expired
            | RefreshTokenError::ReuseDetected
            | RefreshTokenError::FamilyCompromised,
        ) => public_error(StatusCode::UNAUTHORIZED, "invalid refresh token"),
    }
}

async fn oidc_login_inner(
    state: &ServerState,
    req: OidcLoginRequest,
) -> Result<HostedTokenResponse, AuthRouteError> {
    if !bounded_provider_key(&req.provider)
        || !bounded_auth_field(&req.authorization_code)
        || req
            .redirect_uri
            .as_deref()
            .is_some_and(|redirect_uri| !bounded_auth_field(redirect_uri))
    {
        return Err(AuthRouteError::InvalidRequest);
    }

    let requested_org = OrgId::new(req.org_id).map_err(|_| AuthRouteError::InvalidRequest)?;
    let requested_repo = RepoId::new(req.repo_id).map_err(|_| AuthRouteError::InvalidRequest)?;
    let claims = state
        .hosted_auth
        .verify_oidc_login(HostedOidcVerificationRequest {
            provider: &req.provider,
            authorization_code: &req.authorization_code,
            redirect_uri: req.redirect_uri.as_deref(),
        })
        .map_err(|error| match error {
            HostedOidcVerificationError::Disabled => AuthRouteError::Disabled,
            HostedOidcVerificationError::ProviderDenied { .. } => AuthRouteError::Unauthorized,
        });
    let claims = match claims {
        Ok(claims) => claims,
        Err(error) => {
            let reason = match error {
                AuthRouteError::Disabled => "provider_disabled",
                AuthRouteError::Unauthorized => "provider_denied",
                AuthRouteError::InvalidRequest
                | AuthRouteError::TenantMismatch
                | AuthRouteError::Audit => "verification_failed",
            };
            append_auth_audit(
                state,
                oidc_login_denied_event(
                    &req.provider,
                    reason,
                    Some(&requested_org),
                    Some(&requested_repo),
                    None,
                ),
            )
            .await?;
            return Err(error);
        }
    };

    if claims.org_id != requested_org || claims.repo_id != requested_repo {
        append_auth_audit(
            state,
            oidc_login_denied_event(
                &req.provider,
                "tenant_mismatch",
                Some(&requested_org),
                Some(&requested_repo),
                Some(&claims),
            ),
        )
        .await?;
        return Err(AuthRouteError::TenantMismatch);
    }
    if !state
        .repo_belongs_to_org(&requested_org, &requested_repo)
        .map_err(|_| AuthRouteError::TenantMismatch)?
    {
        append_auth_audit(
            state,
            oidc_login_denied_event(
                &req.provider,
                "repo_not_bound",
                Some(&requested_org),
                Some(&requested_repo),
                Some(&claims),
            ),
        )
        .await?;
        return Err(AuthRouteError::TenantMismatch);
    }

    let now = current_unix_time();
    let identity = hosted_identity_from_claims(claims);
    let access = state.hosted_auth.issue_access_token(
        &identity,
        now,
        now.saturating_add(HOSTED_ACCESS_TOKEN_TTL_SECS),
    );
    let refresh = state.hosted_auth.issue_refresh_token(
        &identity,
        now,
        now.saturating_add(HOSTED_REFRESH_TOKEN_TTL_SECS),
    );
    append_auth_audit(state, oidc_login_success_event(&identity)).await?;
    append_auth_audit(state, refresh_token_issue_event(&identity, &refresh.token)).await?;

    Ok(hosted_token_response(
        access.raw_secret,
        refresh.raw_secret,
        &identity,
    ))
}

async fn saml_login_inner(
    state: &ServerState,
    req: SamlLoginRequest,
) -> Result<HostedTokenResponse, AuthRouteError> {
    if !bounded_provider_key(&req.provider)
        || !bounded_auth_field(&req.saml_response)
        || req
            .relay_state
            .as_deref()
            .is_some_and(|relay_state| !bounded_auth_field(relay_state))
    {
        return Err(AuthRouteError::InvalidRequest);
    }

    let requested_org = OrgId::new(req.org_id).map_err(|_| AuthRouteError::InvalidRequest)?;
    let requested_repo = RepoId::new(req.repo_id).map_err(|_| AuthRouteError::InvalidRequest)?;
    let now = current_unix_time();
    let claims = state
        .hosted_auth
        .verify_saml_login(HostedSamlVerificationRequest {
            provider: &req.provider,
            saml_response: &req.saml_response,
            relay_state: req.relay_state.as_deref(),
            org_id: &requested_org,
            repo_id: &requested_repo,
        })
        .map_err(map_saml_verification_error);
    let claims = match claims {
        Ok(claims) => claims,
        Err((error, reason)) => {
            append_auth_audit(
                state,
                saml_login_denied_event(
                    &req.provider,
                    reason,
                    Some(&requested_org),
                    Some(&requested_repo),
                    None,
                ),
            )
            .await?;
            return Err(error);
        }
    };

    if claims.org_id != requested_org || claims.repo_id != requested_repo {
        append_auth_audit(
            state,
            saml_login_denied_event(
                &req.provider,
                "tenant_mismatch",
                Some(&requested_org),
                Some(&requested_repo),
                Some(&claims),
            ),
        )
        .await?;
        return Err(AuthRouteError::TenantMismatch);
    }
    if !state
        .repo_belongs_to_org(&requested_org, &requested_repo)
        .map_err(|_| AuthRouteError::TenantMismatch)?
    {
        append_auth_audit(
            state,
            saml_login_denied_event(
                &req.provider,
                "repo_not_bound",
                Some(&requested_org),
                Some(&requested_repo),
                Some(&claims),
            ),
        )
        .await?;
        return Err(AuthRouteError::TenantMismatch);
    }

    if let Err((error, reason)) = state
        .hosted_auth
        .record_saml_assertion_replay_for_claims(&claims, now)
        .map_err(map_saml_verification_error)
    {
        append_auth_audit(
            state,
            saml_login_denied_event(
                &req.provider,
                reason,
                Some(&requested_org),
                Some(&requested_repo),
                Some(&claims),
            ),
        )
        .await?;
        return Err(error);
    }

    let identity = hosted_identity_from_saml_claims(claims);
    let access = state.hosted_auth.issue_access_token(
        &identity,
        now,
        now.saturating_add(HOSTED_ACCESS_TOKEN_TTL_SECS),
    );
    let refresh = state.hosted_auth.issue_refresh_token(
        &identity,
        now,
        now.saturating_add(HOSTED_REFRESH_TOKEN_TTL_SECS),
    );
    append_auth_audit(state, saml_login_success_event(&identity)).await?;
    append_auth_audit(state, refresh_token_issue_event(&identity, &refresh.token)).await?;

    Ok(hosted_token_response(
        access.raw_secret,
        refresh.raw_secret,
        &identity,
    ))
}

fn map_saml_verification_error(
    error: HostedSamlVerificationError,
) -> (AuthRouteError, &'static str) {
    match error {
        HostedSamlVerificationError::Disabled => (AuthRouteError::Disabled, "provider_disabled"),
        HostedSamlVerificationError::TenantMismatch => {
            (AuthRouteError::TenantMismatch, "tenant_mismatch")
        }
        HostedSamlVerificationError::InvalidMetadata => {
            (AuthRouteError::Unauthorized, "invalid_metadata")
        }
        HostedSamlVerificationError::InvalidAssertion => {
            (AuthRouteError::Unauthorized, "invalid_assertion")
        }
        HostedSamlVerificationError::AssertionExpired => {
            (AuthRouteError::Unauthorized, "assertion_expired")
        }
        HostedSamlVerificationError::AssertionNotYetValid => {
            (AuthRouteError::Unauthorized, "assertion_not_yet_valid")
        }
        HostedSamlVerificationError::AssertionReplay => {
            (AuthRouteError::Unauthorized, "assertion_replay")
        }
        HostedSamlVerificationError::GroupMappingMismatch => {
            (AuthRouteError::Unauthorized, "group_mapping_mismatch")
        }
        HostedSamlVerificationError::ProviderDenied(_) => {
            (AuthRouteError::Unauthorized, "provider_denied")
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthRouteError {
    Disabled,
    InvalidRequest,
    TenantMismatch,
    Unauthorized,
    Audit,
}

fn hosted_identity_from_claims(claims: VerifiedHostedOidcClaims) -> HostedSessionIdentity {
    HostedSessionIdentity {
        session_id: Uuid::new_v4(),
        org_id: claims.org_id,
        repo_id: claims.repo_id,
        uid: claims.uid,
        username: claims.username,
        gid: claims.gid,
        groups: claims.groups,
        external_identity_id: claims.external_identity_id,
    }
}

fn hosted_identity_from_saml_claims(claims: VerifiedHostedSamlClaims) -> HostedSessionIdentity {
    HostedSessionIdentity {
        session_id: Uuid::new_v4(),
        org_id: claims.org_id,
        repo_id: claims.repo_id,
        uid: claims.uid,
        username: claims.username,
        gid: claims.gid,
        groups: claims.groups,
        external_identity_id: claims.external_identity_id,
    }
}

fn hosted_token_response(
    access_token: String,
    refresh_token: String,
    identity: &HostedSessionIdentity,
) -> HostedTokenResponse {
    HostedTokenResponse {
        access_token,
        refresh_token,
        token_type: "Stratum-Session".to_string(),
        expires_in: HOSTED_ACCESS_TOKEN_TTL_SECS,
        refresh_expires_in: HOSTED_REFRESH_TOKEN_TTL_SECS,
        org_id: identity.org_id.to_string(),
        repo_id: identity.repo_id.to_string(),
        uid: identity.uid,
        username: identity.username.clone(),
    }
}

fn oidc_login_denied_event(
    provider: &str,
    reason: &str,
    requested_org: Option<&OrgId>,
    requested_repo: Option<&RepoId>,
    verified_claims: Option<&VerifiedHostedOidcClaims>,
) -> NewAuditEvent {
    let mut event = NewAuditEvent::new(
        AuditActor::new(0, "hosted-auth"),
        AuditAction::AuthOidcLoginDenied,
        AuditResource::id(AuditResourceKind::AuthProvider, provider),
    )
    .with_detail("reason", reason);
    if let Some(org_id) = requested_org {
        event = event.with_detail("requested_org_id", org_id);
    }
    if let Some(repo_id) = requested_repo {
        event = event.with_detail("requested_repo_id", repo_id);
    }
    if let Some(claims) = verified_claims {
        event = event
            .with_detail("verified_org_id", &claims.org_id)
            .with_detail("verified_repo_id", &claims.repo_id)
            .with_detail("principal_uid", claims.uid);
    }
    event
}

fn oidc_login_success_event(identity: &HostedSessionIdentity) -> NewAuditEvent {
    hosted_session_event(
        identity,
        AuditAction::AuthOidcLoginSuccess,
        AuditResource::id(
            AuditResourceKind::HostedSession,
            identity.session_id.to_string(),
        ),
        "login_success",
    )
}

fn saml_login_denied_event(
    provider: &str,
    reason: &str,
    requested_org: Option<&OrgId>,
    requested_repo: Option<&RepoId>,
    verified_claims: Option<&VerifiedHostedSamlClaims>,
) -> NewAuditEvent {
    let mut event = NewAuditEvent::new(
        AuditActor::new(0, "hosted-auth"),
        AuditAction::AuthSamlLoginDenied,
        AuditResource::id(AuditResourceKind::AuthProvider, provider),
    )
    .with_detail("reason", reason);
    if let Some(org_id) = requested_org {
        event = event.with_detail("requested_org_id", org_id);
    }
    if let Some(repo_id) = requested_repo {
        event = event.with_detail("requested_repo_id", repo_id);
    }
    if let Some(claims) = verified_claims {
        event = event
            .with_detail("verified_org_id", &claims.org_id)
            .with_detail("verified_repo_id", &claims.repo_id)
            .with_detail("principal_uid", claims.uid);
    }
    event
}

fn saml_login_success_event(identity: &HostedSessionIdentity) -> NewAuditEvent {
    hosted_session_event(
        identity,
        AuditAction::AuthSamlLoginSuccess,
        AuditResource::id(
            AuditResourceKind::HostedSession,
            identity.session_id.to_string(),
        ),
        "login_success",
    )
}

fn refresh_token_issue_event(
    identity: &HostedSessionIdentity,
    token: &RefreshTokenRecord,
) -> NewAuditEvent {
    refresh_token_event(
        identity,
        AuditAction::AuthRefreshTokenIssue,
        token,
        "issued",
    )
}

fn refresh_token_event(
    identity: &HostedSessionIdentity,
    action: AuditAction,
    token: &RefreshTokenRecord,
    reason: &str,
) -> NewAuditEvent {
    hosted_session_event(
        identity,
        action,
        AuditResource::id(AuditResourceKind::RefreshToken, token.id.to_string()),
        reason,
    )
    .with_detail("token_family_id", token.family_id)
}

fn hosted_session_event(
    identity: &HostedSessionIdentity,
    action: AuditAction,
    resource: AuditResource,
    reason: &str,
) -> NewAuditEvent {
    NewAuditEvent::new(hosted_audit_actor(identity), action, resource)
        .with_detail("org_id", &identity.org_id)
        .with_detail("repo_id", &identity.repo_id)
        .with_detail("principal_uid", identity.uid)
        .with_detail("reason", reason)
}

fn hosted_audit_actor(identity: &HostedSessionIdentity) -> AuditActor {
    AuditActor::new(identity.uid, identity.username.clone())
}

async fn append_refresh_denial_audit(
    state: &ServerState,
    raw_refresh_token: &str,
    error: RefreshTokenError,
) -> Result<(), AuthRouteError> {
    let action = match error {
        RefreshTokenError::Expired => AuditAction::AuthRefreshTokenExpireDenied,
        RefreshTokenError::ReuseDetected | RefreshTokenError::FamilyCompromised => {
            AuditAction::AuthRefreshTokenReuseDenied
        }
        RefreshTokenError::Invalid => return Ok(()),
    };
    let Some(token) = state
        .hosted_auth
        .refresh_token_for_secret(raw_refresh_token)
    else {
        return Ok(());
    };
    let Some(identity) = state.hosted_auth.hosted_session_identity(token.session_id) else {
        return Ok(());
    };
    append_auth_audit(
        state,
        refresh_token_event(&identity, action, &token, refresh_error_reason(error)),
    )
    .await
}

fn refresh_error_reason(error: RefreshTokenError) -> &'static str {
    match error {
        RefreshTokenError::Invalid => "invalid",
        RefreshTokenError::Expired => "expired",
        RefreshTokenError::ReuseDetected => "reuse_detected",
        RefreshTokenError::FamilyCompromised => "family_compromised",
    }
}

async fn append_auth_audit(
    state: &ServerState,
    event: NewAuditEvent,
) -> Result<(), AuthRouteError> {
    state
        .audit
        .append(event)
        .await
        .map(|_| ())
        .map_err(|_| AuthRouteError::Audit)
}

fn public_error(status: StatusCode, message: &str) -> axum::response::Response {
    (
        status,
        Json(ErrorResponse {
            error: message.to_string(),
        }),
    )
        .into_response()
}

fn bounded_auth_field(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_AUTH_FIELD_BYTES
}

fn bounded_provider_key(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes
        .first()
        .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && bytes.len() <= MAX_PROVIDER_KEY_BYTES
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn current_unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_secs()
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let readiness = health_readiness(&state);
    let Ok(db) = state.db.get() else {
        let core_runtime = match state.db.runtime_kind() {
            ServerRuntimeKind::DurableCloud => "durable-cloud",
            ServerRuntimeKind::LocalState => "local-state",
        };
        return Json(serde_json::json!({
            "status": "ok",
            "version": env!("CARGO_PKG_VERSION"),
            "core_runtime": core_runtime,
            "commits": null,
            "inodes": null,
            "objects": null,
            "readiness": readiness,
        }));
    };

    let commits = db.commit_count().await;
    let inodes = db.inode_count().await;
    let objects = db.object_count().await;

    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "commits": commits,
        "inodes": inodes,
        "objects": objects,
        "readiness": readiness,
    }))
}

fn health_readiness(state: &ServerState) -> serde_json::Value {
    let local_core_required = state.db.runtime_kind() == ServerRuntimeKind::LocalState;
    let local_core_opened = state.db.is_available();
    let durable_object_stores_configured = state.db.runtime_kind()
        == ServerRuntimeKind::DurableCloud
        || state.core.guarded_durable_commit_route().is_some();

    serde_json::json!({
        "db": {
            "local_core_required": local_core_required,
            "local_core_opened": local_core_opened,
            "control_plane_opened": true,
        },
        "object_store": {
            "durable_configured": durable_object_stores_configured,
            "startup_checked": durable_object_stores_configured,
        },
        "recovery_stores": {
            "configured": durable_object_stores_configured,
            "startup_opened": durable_object_stores_configured,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::InMemoryAuditStore;
    use crate::auth::hosted::{
        HostedOidcVerificationError, HostedOidcVerificationRequest, HostedOidcVerifier,
        HostedSamlProviderDenial, HostedSamlVerificationError, HostedSamlVerificationRequest,
        HostedSamlVerifier, VerifiedHostedOidcClaims, VerifiedHostedSamlClaims,
    };
    use crate::auth::session::Session;
    use crate::backend::{OrgId, RepoId};
    use crate::db::StratumDb;
    use crate::idempotency::InMemoryIdempotencyStore;
    use crate::review::InMemoryReviewStore;
    use crate::server::core::LocalCoreRuntime;
    use crate::server::{ServerLocalDb, ServerState};
    use crate::workspace::InMemoryWorkspaceMetadataStore;
    use std::sync::Arc;
    use std::sync::Mutex;

    #[tokio::test]
    async fn login_routes_through_core_runtime() {
        let core_db = StratumDb::open_memory();
        let mut root = Session::root();
        core_db
            .execute_command("adduser durable-user", &mut root)
            .await
            .expect("create user in core db");

        let local_only_db = StratumDb::open_memory();
        let state = Arc::new(ServerState {
            core: LocalCoreRuntime::shared(core_db),
            db: ServerLocalDb::available(Arc::new(local_only_db)),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(InMemoryAuditStore::new()),
            review: Arc::new(InMemoryReviewStore::new()),
            hosted_auth: std::sync::Arc::new(crate::auth::hosted::InMemoryHostedAuthStore::new()),
            tenant_repos: Arc::new(crate::server::repo_context::InMemoryTenantRepoResolver::new()),
            secret_replay_kms: None,
        });

        let response = login(
            State(state),
            Json(LoginRequest {
                username: "durable-user".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn oidc_login_denies_disabled_provider_without_session_or_secret_leak() {
        let state = test_state();
        state.bind_tenant_repo_for_test(org_id("org_oidc"), repo_id("repo_oidc"));

        let response = oidc_login(
            State(state.clone()),
            Json(OidcLoginRequest {
                provider: "disabled-provider".to_string(),
                authorization_code: "secret-auth-code".to_string(),
                redirect_uri: Some("https://sensitive.example/callback".to_string()),
                org_id: "org_oidc".to_string(),
                repo_id: "repo_oidc".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = response_json(response).await;
        let body_text = body.to_string();
        assert_eq!(body["error"], "hosted auth provider is disabled");
        assert!(!body_text.contains("secret-auth-code"));
        assert!(!body_text.contains("sensitive.example"));
        assert_eq!(state.hosted_auth.refresh_token_count(), 0);
        let events = audit_events(&state).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::AuthOidcLoginDenied);
        assert_eq!(events[0].resource.kind, AuditResourceKind::AuthProvider);
        assert_eq!(
            events[0].details.get("reason").map(String::as_str),
            Some("provider_disabled")
        );
        let event_text = format!("{events:?}");
        assert!(!event_text.contains("secret-auth-code"));
        assert!(!event_text.contains("sensitive.example"));
    }

    #[tokio::test]
    async fn oidc_login_rejects_invalid_provider_key_without_audit_leak() {
        let state = test_state();
        state.bind_tenant_repo_for_test(org_id("org_oidc"), repo_id("repo_oidc"));

        let response = oidc_login(
            State(state.clone()),
            Json(OidcLoginRequest {
                provider: "https://issuer.example/secret-provider".to_string(),
                authorization_code: "secret-auth-code".to_string(),
                redirect_uri: Some("https://sensitive.example/callback".to_string()),
                org_id: "org_oidc".to_string(),
                repo_id: "repo_oidc".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body_text = response_json(response).await.to_string();
        assert!(!body_text.contains("secret-auth-code"));
        assert!(!body_text.contains("issuer.example"));
        assert!(!body_text.contains("sensitive.example"));
        assert!(audit_events(&state).await.is_empty());
    }

    #[tokio::test]
    async fn oidc_login_rejects_tenant_mismatch_without_local_fallback() {
        let state = test_state_with_verifier(FakeOidcVerifier::success(verified_claims(
            "org_verified",
            "repo_verified",
        )));
        state.bind_tenant_repo_for_test(org_id("org_verified"), repo_id("repo_verified"));
        let local_db = state.db.get().expect("local db is available");
        let mut root = Session::root();
        local_db
            .execute_command("adduser oidc-user", &mut root)
            .await
            .expect("local user exists but must not be used as fallback");

        let response = oidc_login(
            State(state),
            Json(OidcLoginRequest {
                provider: "fake".to_string(),
                authorization_code: "secret-auth-code".to_string(),
                redirect_uri: None,
                org_id: "org_requested".to_string(),
                repo_id: "repo_verified".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = response_json(response).await;
        assert_eq!(body["error"], "hosted auth tenant mismatch");
    }

    #[tokio::test]
    async fn oidc_login_success_issues_tenant_scoped_session_and_refresh() {
        let claims = verified_claims("org_success", "repo_success");
        let state = test_state_with_verifier(FakeOidcVerifier::success(claims.clone()));
        state.bind_tenant_repo_for_test(claims.org_id.clone(), claims.repo_id.clone());

        let response = oidc_login(
            State(state.clone()),
            Json(OidcLoginRequest {
                provider: "fake".to_string(),
                authorization_code: "secret-auth-code".to_string(),
                redirect_uri: None,
                org_id: claims.org_id.to_string(),
                repo_id: claims.repo_id.to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        let access_token = body["access_token"].as_str().expect("access token");
        let refresh_token = body["refresh_token"].as_str().expect("refresh token");
        assert_eq!(body["token_type"], "Stratum-Session");
        assert_eq!(body["org_id"], claims.org_id.as_str());
        assert_eq!(body["repo_id"], claims.repo_id.as_str());
        assert_eq!(body["uid"], claims.uid);
        assert_eq!(body["username"], claims.username);
        assert_ne!(access_token, refresh_token);
        assert_eq!(state.hosted_auth.refresh_token_count(), 1);

        let identity = state
            .hosted_auth
            .validate_access_token_at(access_token, current_unix_time())
            .expect("issued access token validates");
        assert_eq!(identity.org_id, claims.org_id);
        assert_eq!(identity.repo_id, claims.repo_id);

        let events = audit_events(&state).await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].action, AuditAction::AuthOidcLoginSuccess);
        assert_eq!(events[0].resource.kind, AuditResourceKind::HostedSession);
        assert_eq!(events[1].action, AuditAction::AuthRefreshTokenIssue);
        assert_eq!(events[1].resource.kind, AuditResourceKind::RefreshToken);
        let event_text = format!("{events:?}");
        assert!(!event_text.contains(access_token));
        assert!(!event_text.contains(refresh_token));
        assert!(!event_text.contains("secret-auth-code"));
    }

    #[tokio::test]
    async fn saml_login_denies_disabled_provider_without_session_or_secret_leak() {
        let state = test_state();
        state.bind_tenant_repo_for_test(org_id("org_saml"), repo_id("repo_saml"));

        let response = saml_login(
            State(state.clone()),
            Json(SamlLoginRequest {
                provider: "disabled-provider".to_string(),
                saml_response: "<Assertion>NameID secret-nameid</Assertion>".to_string(),
                relay_state: Some("https://sensitive.example/relay".to_string()),
                org_id: "org_saml".to_string(),
                repo_id: "repo_saml".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body_text = response_json(response).await.to_string();
        assert!(!body_text.contains("secret-nameid"));
        assert!(!body_text.contains("sensitive.example"));
        assert_eq!(state.hosted_auth.refresh_token_count(), 0);
        let events = audit_events(&state).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::AuthSamlLoginDenied);
        assert_eq!(events[0].resource.kind, AuditResourceKind::AuthProvider);
        assert_eq!(
            events[0].details.get("reason").map(String::as_str),
            Some("provider_disabled")
        );
        let event_text = format!("{events:?}");
        assert!(!event_text.contains("secret-nameid"));
        assert!(!event_text.contains("sensitive.example"));
    }

    #[tokio::test]
    async fn saml_login_rejects_malformed_request_before_verification_without_leak() {
        let state = test_state_with_saml_verifier(FakeSamlVerifier::success(saml_claims(
            "org_saml",
            "repo_saml",
            "assertion-malformed",
        )));
        state.bind_tenant_repo_for_test(org_id("org_saml"), repo_id("repo_saml"));

        let response = saml_login(
            State(state.clone()),
            Json(SamlLoginRequest {
                provider: "fake".to_string(),
                saml_response: "secret-assertion".repeat(500),
                relay_state: Some("https://sensitive.example/relay".to_string()),
                org_id: "org_saml".to_string(),
                repo_id: "repo_saml".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body_text = response_json(response).await.to_string();
        assert!(!body_text.contains("secret-assertion"));
        assert!(!body_text.contains("sensitive.example"));
        assert!(audit_events(&state).await.is_empty());
        assert_eq!(state.hosted_auth.refresh_token_count(), 0);
    }

    #[tokio::test]
    async fn saml_login_rejects_tenant_mismatch_without_local_fallback() {
        let state = test_state_with_saml_verifier(FakeSamlVerifier::success(saml_claims(
            "org_verified",
            "repo_verified",
            "assertion-tenant",
        )));
        state.bind_tenant_repo_for_test(org_id("org_verified"), repo_id("repo_verified"));
        let local_db = state.db.get().expect("local db is available");
        let mut root = Session::root();
        local_db
            .execute_command("adduser saml-user", &mut root)
            .await
            .expect("local user exists but must not be used as fallback");

        let response = saml_login(
            State(state.clone()),
            Json(SamlLoginRequest {
                provider: "fake".to_string(),
                saml_response: "secret-saml-response".to_string(),
                relay_state: None,
                org_id: "org_requested".to_string(),
                repo_id: "repo_verified".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = response_json(response).await;
        assert_eq!(body["error"], "hosted auth tenant mismatch");
        assert_eq!(state.hosted_auth.refresh_token_count(), 0);

        let retry = saml_login_body(state, "org_verified", "repo_verified").await;
        assert_eq!(retry["token_type"], "Stratum-Session");
    }

    #[tokio::test]
    async fn saml_login_rejects_unbound_repo_without_burning_assertion() {
        let claims = saml_claims("org_unbound", "repo_unbound", "assertion-unbound");
        let state = test_state_with_saml_verifier(FakeSamlVerifier::success(claims.clone()));

        let response = saml_login(
            State(state.clone()),
            Json(SamlLoginRequest {
                provider: "fake".to_string(),
                saml_response: "secret-saml-response".to_string(),
                relay_state: None,
                org_id: claims.org_id.to_string(),
                repo_id: claims.repo_id.to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = response_json(response).await;
        assert_eq!(body["error"], "hosted auth tenant mismatch");
        assert_eq!(state.hosted_auth.refresh_token_count(), 0);

        state.bind_tenant_repo_for_test(claims.org_id.clone(), claims.repo_id.clone());
        let retry = saml_login_body(state, "org_unbound", "repo_unbound").await;
        assert_eq!(retry["token_type"], "Stratum-Session");
    }

    #[tokio::test]
    async fn saml_login_maps_verification_denials_to_bounded_public_errors() {
        for (error, reason) in [
            (
                HostedSamlVerificationError::AssertionExpired,
                "assertion_expired",
            ),
            (
                HostedSamlVerificationError::AssertionNotYetValid,
                "assertion_not_yet_valid",
            ),
            (
                HostedSamlVerificationError::InvalidAssertion,
                "invalid_assertion",
            ),
            (
                HostedSamlVerificationError::GroupMappingMismatch,
                "group_mapping_mismatch",
            ),
        ] {
            let state = test_state_with_saml_verifier(FakeSamlVerifier::failure(error));
            state.bind_tenant_repo_for_test(org_id("org_saml"), repo_id("repo_saml"));

            let response = saml_login(
                State(state.clone()),
                Json(SamlLoginRequest {
                    provider: "fake".to_string(),
                    saml_response: "<Assertion>secret-nameid</Assertion>".to_string(),
                    relay_state: Some("https://sensitive.example/relay".to_string()),
                    org_id: "org_saml".to_string(),
                    repo_id: "repo_saml".to_string(),
                }),
            )
            .await
            .into_response();

            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            let body_text = response_json(response).await.to_string();
            assert!(!body_text.contains("secret-nameid"));
            assert!(!body_text.contains("sensitive.example"));
            let events = audit_events(&state).await;
            assert_eq!(events.len(), 1);
            assert_eq!(
                events[0].details.get("reason").map(String::as_str),
                Some(reason)
            );
        }
    }

    #[tokio::test]
    async fn saml_login_rejects_audience_acs_or_entity_mismatch_without_sensitive_details() {
        let state = test_state_with_saml_verifier(FakeSamlVerifier::failure(
            HostedSamlVerificationError::InvalidAssertion,
        ));
        state.bind_tenant_repo_for_test(org_id("org_saml"), repo_id("repo_saml"));

        let response = saml_login(
            State(state.clone()),
            Json(SamlLoginRequest {
                provider: "fake".to_string(),
                saml_response:
                    "<Assertion>audience=https://secret-sp.example acs=https://secret-acs.example</Assertion>"
                        .to_string(),
                relay_state: Some("https://sensitive.example/relay".to_string()),
                org_id: "org_saml".to_string(),
                repo_id: "repo_saml".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body_text = response_json(response).await.to_string();
        let events = audit_events(&state).await;
        assert_eq!(
            events[0].details.get("reason").map(String::as_str),
            Some("invalid_assertion")
        );
        for forbidden in [
            "secret-sp.example",
            "secret-acs.example",
            "sensitive.example",
        ] {
            assert!(!body_text.contains(forbidden), "leaked {forbidden}");
            assert!(
                !format!("{events:?}").contains(forbidden),
                "audit leaked {forbidden}"
            );
        }
    }

    #[tokio::test]
    async fn saml_login_rejects_assertion_replay() {
        let claims = saml_claims("org_saml", "repo_saml", "assertion-replay");
        let state = test_state_with_saml_verifier(FakeSamlVerifier::success(claims.clone()));
        state.bind_tenant_repo_for_test(claims.org_id.clone(), claims.repo_id.clone());

        let first = saml_login_body(state.clone(), "org_saml", "repo_saml").await;
        assert_eq!(first["token_type"], "Stratum-Session");

        let replay = saml_login(
            State(state.clone()),
            Json(SamlLoginRequest {
                provider: "fake".to_string(),
                saml_response: "same-secret-saml-response".to_string(),
                relay_state: None,
                org_id: "org_saml".to_string(),
                repo_id: "repo_saml".to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(replay.status(), StatusCode::UNAUTHORIZED);
        let events = audit_events(&state).await;
        assert!(
            events
                .iter()
                .any(|event| event.action == AuditAction::AuthSamlLoginDenied
                    && event.details.get("reason").map(String::as_str) == Some("assertion_replay"))
        );
        assert_eq!(state.hosted_auth.refresh_token_count(), 1);
    }

    #[tokio::test]
    async fn saml_login_success_issues_tenant_scoped_session_and_refresh() {
        let claims = saml_claims("org_success", "repo_success", "assertion-success");
        let state = test_state_with_saml_verifier(FakeSamlVerifier::success(claims.clone()));
        state.bind_tenant_repo_for_test(claims.org_id.clone(), claims.repo_id.clone());

        let response = saml_login(
            State(state.clone()),
            Json(SamlLoginRequest {
                provider: "fake".to_string(),
                saml_response: "secret-saml-response".to_string(),
                relay_state: Some("relay-secret".to_string()),
                org_id: claims.org_id.to_string(),
                repo_id: claims.repo_id.to_string(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        let access_token = body["access_token"].as_str().expect("access token");
        let refresh_token = body["refresh_token"].as_str().expect("refresh token");
        assert_eq!(body["token_type"], "Stratum-Session");
        assert_eq!(body["org_id"], claims.org_id.as_str());
        assert_eq!(body["repo_id"], claims.repo_id.as_str());
        assert_eq!(body["uid"], claims.uid);
        assert_eq!(body["username"], claims.username);
        assert_ne!(access_token, refresh_token);
        assert_eq!(state.hosted_auth.refresh_token_count(), 1);

        let identity = state
            .hosted_auth
            .validate_access_token_at(access_token, current_unix_time())
            .expect("issued access token validates");
        assert_eq!(identity.org_id, claims.org_id);
        assert_eq!(identity.repo_id, claims.repo_id);

        let events = audit_events(&state).await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].action, AuditAction::AuthSamlLoginSuccess);
        assert_eq!(events[0].resource.kind, AuditResourceKind::HostedSession);
        assert_eq!(events[1].action, AuditAction::AuthRefreshTokenIssue);
        assert_eq!(events[1].resource.kind, AuditResourceKind::RefreshToken);
        let event_text = format!("{events:?}");
        for forbidden in [
            access_token,
            refresh_token,
            "secret-saml-response",
            "relay-secret",
        ] {
            assert!(!event_text.contains(forbidden), "leaked {forbidden}");
        }
    }

    #[tokio::test]
    async fn saml_public_errors_and_audit_redact_sensitive_verifier_material() {
        let state = test_state_with_saml_verifier(FakeSamlVerifier::failure(
            HostedSamlVerificationError::ProviderDenied(HostedSamlProviderDenial),
        ));
        state.bind_tenant_repo_for_test(org_id("org_redact"), repo_id("repo_redact"));

        let response = saml_login(
            State(state.clone()),
            Json(SamlLoginRequest {
                provider: "fake".to_string(),
                saml_response: "<Assertion><NameID>secret-nameid</NameID><ds:X509Certificate>secret-cert</ds:X509Certificate></Assertion>".to_string(),
                relay_state: Some("https://sensitive.example/relay?token=secret-token".to_string()),
                org_id: "org_redact".to_string(),
                repo_id: "repo_redact".to_string(),
            }),
        )
        .await
        .into_response();
        let body_text = response_json(response).await.to_string();
        let events = audit_events(&state).await;
        let event_text = format!("{events:?}");
        for forbidden in [
            "secret-nameid",
            "secret-cert",
            "secret-token",
            "sensitive.example",
        ] {
            assert!(!body_text.contains(forbidden), "leaked {forbidden}");
            assert!(!event_text.contains(forbidden), "audit leaked {forbidden}");
        }
    }

    #[tokio::test]
    async fn refresh_rotates_token_and_invalidates_previous_refresh() {
        let state = test_state_with_logged_in_oidc("org_refresh", "repo_refresh");
        let login = oidc_login_body(state.clone(), "org_refresh", "repo_refresh").await;
        let original_refresh = login["refresh_token"].as_str().unwrap().to_string();

        let response = refresh(
            State(state.clone()),
            Json(RefreshRequest {
                refresh_token: original_refresh.clone(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let rotated = response_json(response).await;
        let rotated_refresh = rotated["refresh_token"].as_str().unwrap();
        assert_ne!(rotated_refresh, original_refresh);
        let events = audit_events(&state).await;
        assert_eq!(
            events
                .iter()
                .filter(|event| event.action == AuditAction::AuthRefreshTokenRotate)
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.action == AuditAction::AuthRefreshTokenIssue)
                .count(),
            2
        );
        let event_text = format!("{events:?}");
        assert!(!event_text.contains(&original_refresh));
        assert!(!event_text.contains(rotated_refresh));

        let stale = refresh(
            State(state),
            Json(RefreshRequest {
                refresh_token: original_refresh,
            }),
        )
        .await
        .into_response();
        assert_eq!(stale.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn refresh_revoke_blocks_future_refresh() {
        let state = test_state_with_logged_in_oidc("org_revoke", "repo_revoke");
        let login = oidc_login_body(state.clone(), "org_revoke", "repo_revoke").await;
        let refresh_token = login["refresh_token"].as_str().unwrap().to_string();

        let response = refresh_revoke(
            State(state.clone()),
            Json(RefreshRevokeRequest {
                refresh_token: refresh_token.clone(),
            }),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let events = audit_events(&state).await;
        assert!(
            events
                .iter()
                .any(|event| event.action == AuditAction::AuthRefreshTokenRevoke)
        );
        assert!(!format!("{events:?}").contains(&refresh_token));

        let after_revoke = refresh(State(state), Json(RefreshRequest { refresh_token }))
            .await
            .into_response();
        assert_eq!(after_revoke.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn refresh_revoke_stale_token_blocks_successor_refresh() {
        let state = test_state_with_logged_in_oidc("org_revoke_stale", "repo_revoke_stale");
        let login = oidc_login_body(state.clone(), "org_revoke_stale", "repo_revoke_stale").await;
        let original_refresh = login["refresh_token"].as_str().unwrap().to_string();
        let rotated = refresh(
            State(state.clone()),
            Json(RefreshRequest {
                refresh_token: original_refresh.clone(),
            }),
        )
        .await
        .into_response();
        assert_eq!(rotated.status(), StatusCode::OK);
        let rotated = response_json(rotated).await;
        let successor_refresh = rotated["refresh_token"].as_str().unwrap().to_string();

        let revoke_stale = refresh_revoke(
            State(state.clone()),
            Json(RefreshRevokeRequest {
                refresh_token: original_refresh,
            }),
        )
        .await
        .into_response();
        assert_eq!(revoke_stale.status(), StatusCode::NO_CONTENT);

        let successor_after_stale_revoke = refresh(
            State(state),
            Json(RefreshRequest {
                refresh_token: successor_refresh,
            }),
        )
        .await
        .into_response();
        assert_eq!(
            successor_after_stale_revoke.status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn refresh_expiry_denies_without_rotation() {
        let state = test_state();
        let identity = hosted_identity("org_expiry", "repo_expiry");
        let issued = state.hosted_auth.issue_refresh_token(&identity, 10, 20);

        let response = refresh(
            State(state.clone()),
            Json(RefreshRequest {
                refresh_token: issued.raw_secret,
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let events = audit_events(&state).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::AuthRefreshTokenExpireDenied);
        assert_eq!(
            events[0].details.get("reason").map(String::as_str),
            Some("expired")
        );
        assert_eq!(state.hosted_auth.refresh_token_count(), 1);
        assert_eq!(
            state
                .hosted_auth
                .refresh_token(issued.token.id)
                .expect("token exists")
                .rotated_at_unix,
            None
        );
    }

    #[tokio::test]
    async fn stale_refresh_reuse_fails_closed() {
        let state = test_state_with_logged_in_oidc("org_stale", "repo_stale");
        let login = oidc_login_body(state.clone(), "org_stale", "repo_stale").await;
        let original_refresh = login["refresh_token"].as_str().unwrap().to_string();
        let rotated = refresh(
            State(state.clone()),
            Json(RefreshRequest {
                refresh_token: original_refresh.clone(),
            }),
        )
        .await
        .into_response();
        let rotated = response_json(rotated).await;
        let successor_refresh = rotated["refresh_token"].as_str().unwrap().to_string();

        let stale = refresh(
            State(state.clone()),
            Json(RefreshRequest {
                refresh_token: original_refresh,
            }),
        )
        .await
        .into_response();
        assert_eq!(stale.status(), StatusCode::UNAUTHORIZED);
        let events = audit_events(&state).await;
        assert_eq!(
            events
                .iter()
                .filter(|event| event.action == AuditAction::AuthRefreshTokenReuseDenied)
                .count(),
            1
        );
        assert!(!format!("{events:?}").contains(&successor_refresh));

        let successor_after_reuse = refresh(
            State(state),
            Json(RefreshRequest {
                refresh_token: successor_refresh,
            }),
        )
        .await
        .into_response();
        assert_eq!(successor_after_reuse.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn auth_public_errors_redact_provider_and_token_material() {
        let state = test_state_with_verifier(FakeOidcVerifier::failure(
            HostedOidcVerificationError::ProviderDenied {
                message: "provider said id_token=secret-id-token access_token=secret-access-token https://issuer.example/.well-known/jwks.json".to_string(),
            },
        ));
        state.bind_tenant_repo_for_test(org_id("org_redact"), repo_id("repo_redact"));

        let response = oidc_login(
            State(state.clone()),
            Json(OidcLoginRequest {
                provider: "fake".to_string(),
                authorization_code: "secret-auth-code".to_string(),
                redirect_uri: Some("https://sensitive.example/callback".to_string()),
                org_id: "org_redact".to_string(),
                repo_id: "repo_redact".to_string(),
            }),
        )
        .await
        .into_response();
        let body_text = response_json(response).await.to_string();
        let events = audit_events(&state).await;
        let event_text = format!("{events:?}");
        for forbidden in [
            "secret-auth-code",
            "secret-id-token",
            "secret-access-token",
            "issuer.example",
            "sensitive.example",
        ] {
            assert!(!body_text.contains(forbidden), "leaked {forbidden}");
            assert!(!event_text.contains(forbidden), "audit leaked {forbidden}");
        }

        let refresh_response = refresh(
            State(state),
            Json(RefreshRequest {
                refresh_token: "secret-refresh-token".to_string(),
            }),
        )
        .await
        .into_response();
        let refresh_text = response_json(refresh_response).await.to_string();
        assert!(!refresh_text.contains("secret-refresh-token"));
    }

    #[tokio::test]
    async fn health_returns_durable_runtime_body_without_local_db() {
        let stores = crate::backend::StratumStores::local_memory();
        let state = Arc::new(ServerState {
            core: Arc::new(crate::server::core::DurableCoreRuntime::new(
                crate::backend::RepoId::new("repo_durable_health").expect("valid repo id"),
                stores.clone(),
            )),
            db: ServerLocalDb::unavailable(),
            workspaces: stores.workspace_metadata,
            idempotency: stores.idempotency,
            audit: stores.audit,
            review: stores.review,
            hosted_auth: std::sync::Arc::new(crate::auth::hosted::InMemoryHostedAuthStore::new()),
            tenant_repos: Arc::new(crate::server::repo_context::InMemoryTenantRepoResolver::new()),
            secret_replay_kms: None,
        });

        let response = health(State(state)).await.into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read health response body");
        let body: serde_json::Value =
            serde_json::from_slice(&body).expect("health response is json");

        assert_eq!(
            body,
            serde_json::json!({
                "status": "ok",
                "version": env!("CARGO_PKG_VERSION"),
                "core_runtime": "durable-cloud",
                "commits": null,
                "inodes": null,
                "objects": null,
                "readiness": {
                    "db": {
                        "local_core_required": false,
                        "local_core_opened": false,
                        "control_plane_opened": true,
                    },
                    "object_store": {
                        "durable_configured": true,
                        "startup_checked": true,
                    },
                    "recovery_stores": {
                        "configured": true,
                        "startup_opened": true,
                    },
                },
            })
        );
    }

    fn test_state() -> Arc<ServerState> {
        let db = Arc::new(StratumDb::open_memory());
        Arc::new(ServerState {
            core: LocalCoreRuntime::shared_from_arc(db.clone()),
            db: ServerLocalDb::available(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(InMemoryAuditStore::new()),
            review: Arc::new(InMemoryReviewStore::new()),
            hosted_auth: std::sync::Arc::new(crate::auth::hosted::InMemoryHostedAuthStore::new()),
            tenant_repos: Arc::new(crate::server::repo_context::InMemoryTenantRepoResolver::new()),
            secret_replay_kms: None,
        })
    }

    fn test_state_with_verifier(verifier: FakeOidcVerifier) -> Arc<ServerState> {
        let state = test_state();
        state
            .hosted_auth
            .set_oidc_verifier_for_test(Arc::new(verifier));
        state
    }

    fn test_state_with_saml_verifier(verifier: FakeSamlVerifier) -> Arc<ServerState> {
        let state = test_state();
        state
            .hosted_auth
            .set_saml_verifier_for_test(Arc::new(verifier));
        state
    }

    fn test_state_with_logged_in_oidc(org_id: &str, repo_id: &str) -> Arc<ServerState> {
        let claims = verified_claims(org_id, repo_id);
        let state = test_state_with_verifier(FakeOidcVerifier::success(claims.clone()));
        state.bind_tenant_repo_for_test(claims.org_id.clone(), claims.repo_id.clone());
        state
    }

    async fn oidc_login_body(
        state: Arc<ServerState>,
        org_id: &str,
        repo_id: &str,
    ) -> serde_json::Value {
        let response = oidc_login(
            State(state),
            Json(OidcLoginRequest {
                provider: "fake".to_string(),
                authorization_code: "secret-auth-code".to_string(),
                redirect_uri: None,
                org_id: org_id.to_string(),
                repo_id: repo_id.to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        response_json(response).await
    }

    async fn saml_login_body(
        state: Arc<ServerState>,
        org_id: &str,
        repo_id: &str,
    ) -> serde_json::Value {
        let response = saml_login(
            State(state),
            Json(SamlLoginRequest {
                provider: "fake".to_string(),
                saml_response: "secret-saml-response".to_string(),
                relay_state: None,
                org_id: org_id.to_string(),
                repo_id: repo_id.to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        response_json(response).await
    }

    async fn response_json(response: axum::response::Response) -> serde_json::Value {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        serde_json::from_slice(&body).expect("response body is json")
    }

    async fn audit_events(state: &ServerState) -> Vec<crate::audit::AuditEvent> {
        state.audit.list_recent(100).await.expect("audit events")
    }

    fn verified_claims(org_id: &str, repo_id: &str) -> VerifiedHostedOidcClaims {
        VerifiedHostedOidcClaims {
            org_id: crate::backend::OrgId::new(org_id).unwrap(),
            repo_id: crate::backend::RepoId::new(repo_id).unwrap(),
            uid: 4242,
            username: "oidc-user".to_string(),
            gid: 4242,
            groups: vec![4242, 7],
            external_identity_id: "fake|oidc-user".to_string(),
        }
    }

    fn saml_claims(org_id: &str, repo_id: &str, assertion_id: &str) -> VerifiedHostedSamlClaims {
        VerifiedHostedSamlClaims {
            org_id: crate::backend::OrgId::new(org_id).unwrap(),
            repo_id: crate::backend::RepoId::new(repo_id).unwrap(),
            uid: 5151,
            username: "saml-user".to_string(),
            gid: 5151,
            groups: vec![5151, 8],
            external_identity_id: "saml:fake:saml-user".to_string(),
            assertion_id_hash: assertion_id.to_string(),
            not_before_unix: 1,
            expires_at_unix: current_unix_time().saturating_add(3600),
            audience: "https://sp.example/entity".to_string(),
            acs_url: "https://sp.example/auth/saml/acs".to_string(),
            issuer_entity_id: "https://idp.example/entity".to_string(),
            provider_key: "fake".to_string(),
        }
    }

    fn hosted_identity(org_id: &str, repo_id: &str) -> crate::auth::hosted::HostedSessionIdentity {
        let claims = verified_claims(org_id, repo_id);
        crate::auth::hosted::HostedSessionIdentity {
            session_id: uuid::Uuid::new_v4(),
            org_id: claims.org_id,
            repo_id: claims.repo_id,
            uid: claims.uid,
            username: claims.username,
            gid: claims.gid,
            groups: claims.groups,
            external_identity_id: claims.external_identity_id,
        }
    }

    fn org_id(value: &str) -> OrgId {
        OrgId::new(value).unwrap()
    }

    fn repo_id(value: &str) -> RepoId {
        RepoId::new(value).unwrap()
    }

    fn current_unix_time() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_secs()
    }

    struct FakeOidcVerifier {
        result: Mutex<Result<VerifiedHostedOidcClaims, HostedOidcVerificationError>>,
    }

    impl FakeOidcVerifier {
        fn success(claims: VerifiedHostedOidcClaims) -> Self {
            Self {
                result: Mutex::new(Ok(claims)),
            }
        }

        fn failure(error: HostedOidcVerificationError) -> Self {
            Self {
                result: Mutex::new(Err(error)),
            }
        }
    }

    impl HostedOidcVerifier for FakeOidcVerifier {
        fn verify(
            &self,
            _request: HostedOidcVerificationRequest<'_>,
        ) -> Result<VerifiedHostedOidcClaims, HostedOidcVerificationError> {
            self.result
                .lock()
                .expect("fake verifier lock poisoned")
                .clone()
        }
    }

    struct FakeSamlVerifier {
        result: Mutex<Result<VerifiedHostedSamlClaims, HostedSamlVerificationError>>,
    }

    impl FakeSamlVerifier {
        fn success(claims: VerifiedHostedSamlClaims) -> Self {
            Self {
                result: Mutex::new(Ok(claims)),
            }
        }

        fn failure(error: HostedSamlVerificationError) -> Self {
            Self {
                result: Mutex::new(Err(error)),
            }
        }
    }

    impl HostedSamlVerifier for FakeSamlVerifier {
        fn verify(
            &self,
            _request: HostedSamlVerificationRequest<'_>,
        ) -> Result<VerifiedHostedSamlClaims, HostedSamlVerificationError> {
            self.result
                .lock()
                .expect("fake verifier lock poisoned")
                .clone()
        }
    }

    #[tokio::test]
    async fn health_returns_local_db_counts_when_available() {
        let db = Arc::new(StratumDb::open_memory());
        let expected_commits = db.commit_count().await;
        let expected_inodes = db.inode_count().await;
        let expected_objects = db.object_count().await;
        let state = Arc::new(ServerState {
            core: LocalCoreRuntime::shared_from_arc(db.clone()),
            db: ServerLocalDb::available(db),
            workspaces: Arc::new(InMemoryWorkspaceMetadataStore::new()),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            audit: Arc::new(InMemoryAuditStore::new()),
            review: Arc::new(InMemoryReviewStore::new()),
            hosted_auth: std::sync::Arc::new(crate::auth::hosted::InMemoryHostedAuthStore::new()),
            tenant_repos: Arc::new(crate::server::repo_context::InMemoryTenantRepoResolver::new()),
            secret_replay_kms: None,
        });

        let response = health(State(state)).await.into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read health response body");
        let body: serde_json::Value =
            serde_json::from_slice(&body).expect("health response is json");

        assert_eq!(body["status"], "ok");
        assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(body["commits"], expected_commits);
        assert_eq!(body["inodes"], expected_inodes);
        assert_eq!(body["objects"], expected_objects);
        assert_eq!(body["readiness"]["db"]["local_core_required"], true);
        assert_eq!(body["readiness"]["db"]["local_core_opened"], true);
        assert_eq!(body["readiness"]["db"]["control_plane_opened"], true);
        assert_eq!(
            body["readiness"]["object_store"]["durable_configured"],
            false
        );
        assert_eq!(body["readiness"]["object_store"]["startup_checked"], false);
        assert_eq!(body["readiness"]["recovery_stores"]["configured"], false);
        assert_eq!(
            body["readiness"]["recovery_stores"]["startup_opened"],
            false
        );
        assert!(body.get("core_runtime").is_none());
    }
}
