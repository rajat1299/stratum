//! Runtime backend selection contract.
//!
//! This module validates the operator-facing backend mode and the planned
//! durable backend prerequisites. It intentionally does not wire the server to
//! Postgres or S3/R2 yet.

use regex::Regex;
use std::fmt;
use std::sync::OnceLock;

use crate::error::VfsError;

pub const BACKEND_ENV: &str = "STRATUM_BACKEND";
pub const POSTGRES_URL_ENV: &str = "STRATUM_POSTGRES_URL";
pub const R2_BUCKET_ENV: &str = "STRATUM_R2_BUCKET";
pub const R2_ENDPOINT_ENV: &str = "STRATUM_R2_ENDPOINT";
pub const R2_ACCESS_KEY_ID_ENV: &str = "STRATUM_R2_ACCESS_KEY_ID";
pub const R2_SECRET_ACCESS_KEY_ENV: &str = "STRATUM_R2_SECRET_ACCESS_KEY";
pub const R2_REGION_ENV: &str = "STRATUM_R2_REGION";
pub const R2_PREFIX_ENV: &str = "STRATUM_R2_PREFIX";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendRuntimeMode {
    Local,
    Durable,
}

impl BackendRuntimeMode {
    fn from_env_value(value: &str) -> Result<Self, VfsError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "local" => Ok(Self::Local),
            "durable" => Ok(Self::Durable),
            _ => Err(VfsError::InvalidArgs {
                message: format!("invalid {BACKEND_ENV}: {value}; expected `local` or `durable`"),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Durable => "durable",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct BackendRuntimeConfig {
    mode: BackendRuntimeMode,
    durable: Option<DurableBackendRuntimeConfig>,
}

impl BackendRuntimeConfig {
    pub fn from_env() -> Result<Self, VfsError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, VfsError> {
        let mode =
            BackendRuntimeMode::from_env_value(lookup(BACKEND_ENV).as_deref().unwrap_or("local"))?;
        match mode {
            BackendRuntimeMode::Local => Ok(Self {
                mode,
                durable: None,
            }),
            BackendRuntimeMode::Durable => Ok(Self {
                mode,
                durable: Some(DurableBackendRuntimeConfig::from_lookup(lookup)?),
            }),
        }
    }

    pub fn mode(&self) -> BackendRuntimeMode {
        self.mode
    }

    pub fn durable(&self) -> Option<&DurableBackendRuntimeConfig> {
        self.durable.as_ref()
    }

    pub fn ensure_supported_for_server(&self) -> Result<(), VfsError> {
        match self.mode {
            BackendRuntimeMode::Local => Ok(()),
            BackendRuntimeMode::Durable => Err(VfsError::NotSupported {
                message: "durable backend runtime is validated but not wired into stratum-server yet; use STRATUM_BACKEND=local until the Postgres/R2 runtime cutover lands"
                    .to_string(),
            }),
        }
    }
}

impl fmt::Debug for BackendRuntimeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackendRuntimeConfig")
            .field("mode", &self.mode)
            .field("durable", &self.durable)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct DurableBackendRuntimeConfig {
    postgres_url_configured: bool,
    object_store: DurableObjectStoreRuntimeConfig,
}

impl DurableBackendRuntimeConfig {
    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, VfsError> {
        let mut missing = Vec::new();
        let postgres_url = required_value(&mut lookup, POSTGRES_URL_ENV, &mut missing);
        let bucket = required_value(&mut lookup, R2_BUCKET_ENV, &mut missing);
        let endpoint = required_value(&mut lookup, R2_ENDPOINT_ENV, &mut missing);
        let access_key_id = required_value(&mut lookup, R2_ACCESS_KEY_ID_ENV, &mut missing);
        let secret_access_key = required_value(&mut lookup, R2_SECRET_ACCESS_KEY_ENV, &mut missing);

        if !missing.is_empty() {
            return Err(VfsError::InvalidArgs {
                message: format!(
                    "missing required durable backend environment variables: {}",
                    missing.join(", ")
                ),
            });
        }

        let postgres_url = postgres_url.expect("missing durable env should return earlier");
        if postgres_url_contains_password(&postgres_url) {
            return Err(VfsError::InvalidArgs {
                message: format!(
                    "{POSTGRES_URL_ENV} must not include a password; use PGPASSWORD, PGPASSFILE, PGSERVICE, or the deployment secret manager"
                ),
            });
        }
        let endpoint = endpoint.expect("missing durable env should return earlier");
        if endpoint_has_sensitive_parts(&endpoint) {
            return Err(VfsError::InvalidArgs {
                message: format!(
                    "{R2_ENDPOINT_ENV} must not include userinfo or secret-bearing query parameters"
                ),
            });
        }

        let region =
            optional_value(&mut lookup, R2_REGION_ENV).unwrap_or_else(|| "auto".to_string());
        let prefix =
            optional_value(&mut lookup, R2_PREFIX_ENV).unwrap_or_else(|| "stratum".to_string());

        Ok(Self {
            postgres_url_configured: true,
            object_store: DurableObjectStoreRuntimeConfig {
                bucket: bucket.expect("missing durable env should return earlier"),
                endpoint,
                access_key_id_configured: access_key_id.is_some(),
                secret_access_key_configured: secret_access_key.is_some(),
                region,
                prefix,
            },
        })
    }

    pub fn postgres_url_configured(&self) -> bool {
        self.postgres_url_configured
    }

    pub fn object_store(&self) -> &DurableObjectStoreRuntimeConfig {
        &self.object_store
    }
}

impl fmt::Debug for DurableBackendRuntimeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableBackendRuntimeConfig")
            .field("postgres_url_configured", &self.postgres_url_configured)
            .field("object_store", &self.object_store)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct DurableObjectStoreRuntimeConfig {
    pub bucket: String,
    pub endpoint: String,
    pub access_key_id_configured: bool,
    pub secret_access_key_configured: bool,
    pub region: String,
    pub prefix: String,
}

impl fmt::Debug for DurableObjectStoreRuntimeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableObjectStoreRuntimeConfig")
            .field("bucket", &self.bucket)
            .field("endpoint", &sanitize_endpoint_for_debug(&self.endpoint))
            .field("access_key_id_configured", &self.access_key_id_configured)
            .field(
                "secret_access_key_configured",
                &self.secret_access_key_configured,
            )
            .field("region", &self.region)
            .field("prefix", &self.prefix)
            .finish()
    }
}

fn required_value(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &'static str,
    missing: &mut Vec<&'static str>,
) -> Option<String> {
    match lookup(name).map(|value| value.trim().to_string()) {
        Some(value) if !value.is_empty() => Some(value),
        _ => {
            missing.push(name);
            None
        }
    }
}

fn optional_value(lookup: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Option<String> {
    lookup(name)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn postgres_url_contains_password(value: &str) -> bool {
    uri_userinfo_contains_password(value)
        || password_param_regex().is_match(value)
        || query_contains_sensitive_key(value, &["password"])
}

fn uri_userinfo_contains_password(value: &str) -> bool {
    let Some(scheme_index) = value.find("://") else {
        return false;
    };
    let after_scheme = &value[(scheme_index + 3)..];
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    let Some(at_index) = authority.rfind('@') else {
        return false;
    };
    authority[..at_index].contains(':')
}

fn endpoint_has_sensitive_parts(value: &str) -> bool {
    uri_contains_userinfo(value)
        || query_contains_sensitive_key(
            value,
            &[
                "access_key",
                "access_key_id",
                "accesskey",
                "authorization",
                "awsaccesskeyid",
                "password",
                "secret",
                "secret_access_key",
                "signature",
                "token",
                "x-amz-credential",
                "x-amz-security-token",
                "x-amz-signature",
            ],
        )
}

fn uri_contains_userinfo(value: &str) -> bool {
    let Some(scheme_index) = value.find("://") else {
        return false;
    };
    let after_scheme = &value[(scheme_index + 3)..];
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    authority.contains('@')
}

fn query_contains_sensitive_key(value: &str, sensitive_keys: &[&str]) -> bool {
    let Some(query) = value.split_once('?').map(|(_, query)| query) else {
        return false;
    };
    let query = query.split('#').next().unwrap_or_default();
    query
        .split('&')
        .filter(|part| !part.is_empty())
        .any(|part| {
            let key = part.split_once('=').map_or(part, |(key, _)| key);
            let decoded = urlencoding::decode(key)
                .map(|key| key.into_owned())
                .unwrap_or_else(|_| key.to_string());
            let normalized = decoded.trim().to_ascii_lowercase();
            sensitive_keys
                .iter()
                .any(|candidate| normalized == *candidate)
        })
}

fn sanitize_endpoint_for_debug(endpoint: &str) -> String {
    let without_query = endpoint.split(['?', '#']).next().unwrap_or(endpoint);
    let Some(scheme_index) = without_query.find("://") else {
        return without_query.to_string();
    };
    let scheme_end = scheme_index + 3;
    let after_scheme = &without_query[scheme_end..];
    let slash_index = after_scheme.find('/');
    let (authority, path) = match slash_index {
        Some(index) => (&after_scheme[..index], &after_scheme[index..]),
        None => (after_scheme, ""),
    };
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    format!("{}{}{}", &without_query[..scheme_end], authority, path)
}

fn password_param_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)(^|[\s?&])password\s*=").expect("password detection regex should compile")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn lookup(entries: &[(&str, &str)]) -> impl FnMut(&str) -> Option<String> {
        let values: BTreeMap<String, String> = entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        move |name| values.get(name).cloned()
    }

    fn durable_entries() -> Vec<(&'static str, &'static str)> {
        vec![
            (BACKEND_ENV, "durable"),
            (POSTGRES_URL_ENV, "postgresql://stratum-db.internal/stratum"),
            (R2_BUCKET_ENV, "stratum-prod"),
            (R2_ENDPOINT_ENV, "https://account.r2.cloudflarestorage.com"),
            (R2_ACCESS_KEY_ID_ENV, "test-access-key-id"),
            (R2_SECRET_ACCESS_KEY_ENV, "test-secret-access-key"),
        ]
    }

    #[test]
    fn defaults_to_local_backend() {
        let config = BackendRuntimeConfig::from_lookup(lookup(&[])).unwrap();

        assert_eq!(config.mode(), BackendRuntimeMode::Local);
        assert!(config.durable().is_none());
        config.ensure_supported_for_server().unwrap();
    }

    #[test]
    fn accepts_durable_backend_when_required_env_is_present() {
        let config = BackendRuntimeConfig::from_lookup(lookup(&durable_entries())).unwrap();

        let durable = config.durable().expect("durable config should be present");
        assert_eq!(config.mode(), BackendRuntimeMode::Durable);
        assert!(durable.postgres_url_configured());
        assert_eq!(durable.object_store().bucket, "stratum-prod");
        assert_eq!(durable.object_store().region, "auto");
        assert_eq!(durable.object_store().prefix, "stratum");
        assert!(matches!(
            config.ensure_supported_for_server(),
            Err(VfsError::NotSupported { .. })
        ));
    }

    #[test]
    fn durable_backend_accepts_optional_r2_region_and_prefix() {
        let mut entries = durable_entries();
        entries.push((R2_REGION_ENV, "us-east-1"));
        entries.push((R2_PREFIX_ENV, "hosted"));

        let config = BackendRuntimeConfig::from_lookup(lookup(&entries)).unwrap();
        let object_store = config.durable().unwrap().object_store();

        assert_eq!(object_store.region, "us-east-1");
        assert_eq!(object_store.prefix, "hosted");
    }

    #[test]
    fn rejects_unknown_backend() {
        let err = BackendRuntimeConfig::from_lookup(lookup(&[(BACKEND_ENV, "postgres")]))
            .expect_err("unknown backend should fail");

        assert!(matches!(err, VfsError::InvalidArgs { .. }));
        assert!(err.to_string().contains("expected `local` or `durable`"));
    }

    #[test]
    fn local_backend_does_not_capture_durable_env_values() {
        let config = BackendRuntimeConfig::from_lookup(lookup(&[
            (BACKEND_ENV, "local"),
            (
                POSTGRES_URL_ENV,
                "postgresql://user:raw-db-password-123@localhost/stratum",
            ),
            (R2_ACCESS_KEY_ID_ENV, "test-access-key-id"),
            (R2_SECRET_ACCESS_KEY_ENV, "test-secret-access-key"),
        ]))
        .unwrap();

        let debug = format!("{config:?}");
        assert_eq!(config.mode(), BackendRuntimeMode::Local);
        assert!(config.durable().is_none());
        assert!(!debug.contains("raw-db-password-123"));
        assert!(!debug.contains("test-access-key-id"));
        assert!(!debug.contains("test-secret-access-key"));
    }

    #[test]
    fn durable_backend_reports_missing_required_env_by_name_only() {
        let err = BackendRuntimeConfig::from_lookup(lookup(&[(BACKEND_ENV, "durable")]))
            .expect_err("missing durable env should fail");

        let message = err.to_string();
        assert!(message.contains(POSTGRES_URL_ENV));
        assert!(message.contains(R2_SECRET_ACCESS_KEY_ENV));
        assert!(!message.contains("test-secret-access-key"));
    }

    #[test]
    fn durable_backend_treats_empty_required_env_as_missing() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != R2_BUCKET_ENV);
        entries.push((R2_BUCKET_ENV, "   "));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("empty bucket should fail");

        assert!(err.to_string().contains(R2_BUCKET_ENV));
    }

    #[test]
    fn durable_backend_rejects_postgres_uri_password() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != POSTGRES_URL_ENV);
        entries.push((
            POSTGRES_URL_ENV,
            "postgresql://user:raw-db-password-123@localhost/stratum",
        ));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("password-bearing URL should fail");

        assert!(err.to_string().contains("must not include a password"));
        assert!(!err.to_string().contains("raw-db-password-123"));
    }

    #[test]
    fn durable_backend_rejects_postgres_query_password() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != POSTGRES_URL_ENV);
        entries.push((
            POSTGRES_URL_ENV,
            "postgresql://localhost/stratum?password=raw-db-password-123",
        ));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("query password should fail");

        assert!(err.to_string().contains("must not include a password"));
        assert!(!err.to_string().contains("raw-db-password-123"));
    }

    #[test]
    fn durable_backend_rejects_percent_encoded_postgres_query_password_key() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != POSTGRES_URL_ENV);
        entries.push((
            POSTGRES_URL_ENV,
            "postgresql://localhost/stratum?pass%77ord=raw-db-password-123",
        ));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("encoded query password should fail");

        assert!(err.to_string().contains("must not include a password"));
        assert!(!err.to_string().contains("raw-db-password-123"));
    }

    #[test]
    fn durable_backend_rejects_postgres_keyword_password() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != POSTGRES_URL_ENV);
        entries.push((
            POSTGRES_URL_ENV,
            "host=localhost password = raw-db-password-123 dbname=stratum",
        ));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("keyword password should fail");

        assert!(err.to_string().contains("must not include a password"));
        assert!(!err.to_string().contains("raw-db-password-123"));
    }

    #[test]
    fn durable_backend_rejects_secret_bearing_r2_endpoint() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != R2_ENDPOINT_ENV);
        entries.push((
            R2_ENDPOINT_ENV,
            "https://example.invalid?tok%65n=raw-endpoint-token",
        ));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("secret-bearing endpoint should fail");

        assert!(
            err.to_string()
                .contains("must not include userinfo or secret-bearing query parameters")
        );
        assert!(!err.to_string().contains("raw-endpoint-token"));
    }

    #[test]
    fn durable_backend_rejects_r2_endpoint_userinfo() {
        let mut entries = durable_entries();
        entries.retain(|(key, _)| *key != R2_ENDPOINT_ENV);
        entries.push((
            R2_ENDPOINT_ENV,
            "https://user:raw-endpoint-password@example.invalid",
        ));

        let err = BackendRuntimeConfig::from_lookup(lookup(&entries))
            .expect_err("userinfo endpoint should fail");

        assert!(
            err.to_string()
                .contains("must not include userinfo or secret-bearing query parameters")
        );
        assert!(!err.to_string().contains("raw-endpoint-password"));
    }

    #[test]
    fn debug_output_redacts_durable_secret_values() {
        let config = BackendRuntimeConfig::from_lookup(lookup(&durable_entries())).unwrap();

        let debug = format!("{config:?}");
        assert!(debug.contains("access_key_id_configured: true"));
        assert!(debug.contains("secret_access_key_configured: true"));
        assert!(!debug.contains("test-access-key-id"));
        assert!(!debug.contains("test-secret-access-key"));
        assert!(!debug.contains("postgresql://stratum-db.internal/stratum"));
    }
}
