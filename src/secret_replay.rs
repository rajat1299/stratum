use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::VfsError;

const ENVELOPE_VERSION: u16 = 1;
const AES_256_GCM_KEY_BYTES: usize = 32;
const AES_256_GCM_NONCE_BYTES: usize = 12;
pub const SECRET_REPLAY_KMS_KEY_ID_MAX_BYTES: usize = 255;
const SECRET_REPLAY_KMS_UNAVAILABLE: &str = "secret replay KMS is unavailable";
const SECRET_REPLAY_DECRYPT_FAILED: &str = "secret replay decrypt failed";

pub type SharedSecretReplayKms = Arc<dyn SecretReplayKms>;

pub trait SecretReplayKms: Send + Sync {
    fn key_id(&self) -> &str;

    fn key_hash(&self) -> &str;

    fn encrypt_json(
        &self,
        aad: &SecretReplayAad,
        body: &serde_json::Value,
    ) -> Result<SecretReplayEnvelope, VfsError>;

    fn decrypt_json(
        &self,
        aad: &SecretReplayAad,
        envelope: &SecretReplayEnvelope,
    ) -> Result<serde_json::Value, VfsError>;
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretReplayAad {
    pub scope: String,
    pub key_hash: String,
    pub request_fingerprint: String,
    pub route: String,
    pub status_code: u16,
    pub replay_classification: String,
}

impl fmt::Debug for SecretReplayAad {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretReplayAad")
            .field("scope_configured", &!self.scope.is_empty())
            .field("key_hash_configured", &!self.key_hash.is_empty())
            .field(
                "request_fingerprint_configured",
                &!self.request_fingerprint.is_empty(),
            )
            .field("route_configured", &!self.route.is_empty())
            .field("status_code", &self.status_code)
            .field("replay_classification", &self.replay_classification)
            .finish()
    }
}

impl SecretReplayAad {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, VfsError> {
        serde_json::to_vec(self).map_err(|_| VfsError::InvalidArgs {
            message: SECRET_REPLAY_DECRYPT_FAILED.to_string(),
        })
    }

    pub fn aad_hash(&self) -> Result<String, VfsError> {
        Ok(hex_sha256(&self.canonical_bytes()?))
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SecretReplayEnvelope {
    pub version: u16,
    pub key_id: String,
    pub nonce_b64: String,
    pub ciphertext_b64: String,
    pub aad_hash: String,
    pub encrypted_at_unix_seconds: u64,
}

impl fmt::Debug for SecretReplayEnvelope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretReplayEnvelope")
            .field("version", &self.version)
            .field("key_id_configured", &!self.key_id.is_empty())
            .field("ciphertext_len", &self.ciphertext_b64.len())
            .field("aad_hash_configured", &!self.aad_hash.is_empty())
            .finish()
    }
}

pub struct LocalAeadSecretReplayKms {
    key_id: String,
    key_hash: String,
    key: LessSafeKey,
    rng: SystemRandom,
}

impl LocalAeadSecretReplayKms {
    pub fn new(
        key_id: impl Into<String>,
        key_material: [u8; AES_256_GCM_KEY_BYTES],
    ) -> Result<Self, VfsError> {
        let key_id = normalize_secret_replay_key_id(key_id)?;
        let unbound = UnboundKey::new(&AES_256_GCM, &key_material)
            .map_err(|_| secret_replay_kms_unavailable())?;
        Ok(Self {
            key_id,
            key_hash: hex_sha256(&key_material),
            key: LessSafeKey::new(unbound),
            rng: SystemRandom::new(),
        })
    }
}

impl fmt::Debug for LocalAeadSecretReplayKms {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalAeadSecretReplayKms")
            .field("key_id_configured", &!self.key_id.is_empty())
            .field("key_hash_configured", &!self.key_hash.is_empty())
            .finish_non_exhaustive()
    }
}

impl SecretReplayKms for LocalAeadSecretReplayKms {
    fn key_id(&self) -> &str {
        &self.key_id
    }

    fn key_hash(&self) -> &str {
        &self.key_hash
    }

    fn encrypt_json(
        &self,
        aad: &SecretReplayAad,
        body: &serde_json::Value,
    ) -> Result<SecretReplayEnvelope, VfsError> {
        let aad_bytes = aad.canonical_bytes()?;
        let mut plaintext =
            serde_json::to_vec(body).map_err(|_| secret_replay_kms_unavailable())?;
        let mut nonce_bytes = [0u8; AES_256_GCM_NONCE_BYTES];
        self.rng
            .fill(&mut nonce_bytes)
            .map_err(|_| secret_replay_kms_unavailable())?;
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);
        self.key
            .seal_in_place_append_tag(nonce, Aad::from(&aad_bytes), &mut plaintext)
            .map_err(|_| secret_replay_kms_unavailable())?;

        Ok(SecretReplayEnvelope {
            version: ENVELOPE_VERSION,
            key_id: self.key_id.clone(),
            nonce_b64: BASE64.encode(nonce_bytes),
            ciphertext_b64: BASE64.encode(plaintext),
            aad_hash: hex_sha256(&aad_bytes),
            encrypted_at_unix_seconds: current_unix_seconds(),
        })
    }

    fn decrypt_json(
        &self,
        aad: &SecretReplayAad,
        envelope: &SecretReplayEnvelope,
    ) -> Result<serde_json::Value, VfsError> {
        if envelope.version != ENVELOPE_VERSION
            || envelope.key_id != self.key_id
            || envelope.aad_hash != aad.aad_hash()?
        {
            return Err(secret_replay_decrypt_failed());
        }
        let nonce_bytes = BASE64
            .decode(&envelope.nonce_b64)
            .map_err(|_| secret_replay_decrypt_failed())?;
        let nonce_bytes: [u8; AES_256_GCM_NONCE_BYTES] = nonce_bytes
            .try_into()
            .map_err(|_| secret_replay_decrypt_failed())?;
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);
        let aad_bytes = aad.canonical_bytes()?;
        let mut ciphertext = BASE64
            .decode(&envelope.ciphertext_b64)
            .map_err(|_| secret_replay_decrypt_failed())?;
        let plaintext = self
            .key
            .open_in_place(nonce, Aad::from(&aad_bytes), &mut ciphertext)
            .map_err(|_| secret_replay_decrypt_failed())?;
        serde_json::from_slice(plaintext).map_err(|_| secret_replay_decrypt_failed())
    }
}

pub fn local_aead_key_from_b64(value: &str) -> Result<[u8; AES_256_GCM_KEY_BYTES], VfsError> {
    let bytes = BASE64
        .decode(value.trim())
        .map_err(|_| secret_replay_kms_unavailable())?;
    bytes
        .try_into()
        .map_err(|_| secret_replay_kms_unavailable())
}

pub fn normalize_secret_replay_key_id(value: impl Into<String>) -> Result<String, VfsError> {
    let key_id = value.into().trim().to_string();
    if key_id.is_empty() || key_id.len() > SECRET_REPLAY_KMS_KEY_ID_MAX_BYTES {
        return Err(secret_replay_kms_unavailable());
    }
    Ok(key_id)
}

pub fn secret_replay_kms_unavailable() -> VfsError {
    VfsError::InvalidArgs {
        message: SECRET_REPLAY_KMS_UNAVAILABLE.to_string(),
    }
}

pub fn secret_replay_decrypt_failed() -> VfsError {
    VfsError::CorruptStore {
        message: SECRET_REPLAY_DECRYPT_FAILED.to_string(),
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn current_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_aad() -> SecretReplayAad {
        SecretReplayAad {
            scope: "workspace:ws1:tokens:issue".to_string(),
            key_hash: "key-hash".to_string(),
            request_fingerprint: "raw-request-fp-secret".to_string(),
            route: "POST /workspaces/{id}/tokens".to_string(),
            status_code: 201,
            replay_classification: "secret_bearing".to_string(),
        }
    }

    fn test_kms() -> LocalAeadSecretReplayKms {
        LocalAeadSecretReplayKms::new("test-key", [7u8; 32]).unwrap()
    }

    #[test]
    fn local_aead_encrypt_decrypt_round_trip_requires_same_aad() {
        let kms = test_kms();
        let aad = test_aad();
        let body = serde_json::json!({
            "workspace_token": "raw-workspace-token",
            "token_id": "token-1"
        });

        let envelope = kms.encrypt_json(&aad, &body).unwrap();
        let replay = kms.decrypt_json(&aad, &envelope).unwrap();
        assert_eq!(replay, body);

        let mut wrong_aad = aad.clone();
        wrong_aad.scope = "workspace:other:tokens:issue".to_string();
        let err = kms.decrypt_json(&wrong_aad, &envelope).unwrap_err();
        assert_eq!(
            err.to_string(),
            "stratum: corrupt store: secret replay decrypt failed"
        );
    }

    #[test]
    fn unknown_key_id_fails_with_redacted_error() {
        let kms = test_kms();
        let aad = test_aad();
        let body = serde_json::json!({"workspace_token": "raw-workspace-token"});
        let mut envelope = kms.encrypt_json(&aad, &body).unwrap();
        envelope.key_id = "removed-key".to_string();

        let err = kms.decrypt_json(&aad, &envelope).unwrap_err();
        assert_eq!(
            err.to_string(),
            "stratum: corrupt store: secret replay decrypt failed"
        );
    }

    #[test]
    fn key_id_is_bounded_and_normalized() {
        let kms = LocalAeadSecretReplayKms::new(" test-key ", [7u8; 32]).unwrap();
        assert_eq!(kms.key_id(), "test-key");

        let err = LocalAeadSecretReplayKms::new("x".repeat(256), [7u8; 32]).unwrap_err();
        assert_eq!(err.to_string(), "stratum: secret replay KMS is unavailable");
    }

    #[test]
    fn debug_redacts_key_plaintext_nonce_and_ciphertext() {
        let kms = test_kms();
        let aad = test_aad();
        let body = serde_json::json!({"workspace_token": "raw-workspace-token"});
        let envelope = kms.encrypt_json(&aad, &body).unwrap();

        let rendered = format!("{kms:?} {aad:?} {envelope:?}");
        assert!(!rendered.contains("raw-workspace-token"));
        assert!(!rendered.contains("workspace:ws1"));
        assert!(!rendered.contains("raw-request-fp-secret"));
        assert!(!rendered.contains("key-hash"));
        assert!(!rendered.contains(&BASE64.encode([7u8; 32])));
        assert!(!rendered.contains(&envelope.nonce_b64));
        assert!(!rendered.contains(&envelope.ciphertext_b64));
    }
}
