use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_config::Region;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use std::path::{Path, PathBuf};

use crate::error::VfsError;

#[derive(Debug, Clone)]
pub struct R2BlobStoreConfig {
    pub bucket: String,
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    pub prefix: String,
}

impl R2BlobStoreConfig {
    pub fn from_env() -> Option<Self> {
        let bucket = std::env::var("STRATUM_R2_BUCKET").ok()?;
        let endpoint = std::env::var("STRATUM_R2_ENDPOINT").ok()?;
        let access_key_id = std::env::var("STRATUM_R2_ACCESS_KEY_ID").ok()?;
        let secret_access_key = std::env::var("STRATUM_R2_SECRET_ACCESS_KEY").ok()?;
        let region = std::env::var("STRATUM_R2_REGION").unwrap_or_else(|_| "auto".to_string());
        let prefix = std::env::var("STRATUM_R2_PREFIX").unwrap_or_else(|_| "stratum".to_string());
        Some(Self {
            bucket,
            endpoint,
            access_key_id,
            secret_access_key,
            region,
            prefix,
        })
    }
}

#[async_trait]
pub trait RemoteBlobStore: Send + Sync {
    async fn put_bytes(&self, key: &str, data: Vec<u8>) -> Result<(), VfsError>;
    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>, VfsError>;

    async fn put_content_blob(&self, hash: &str, data: Vec<u8>) -> Result<(), VfsError> {
        self.put_bytes(&format!("blobs/{hash}"), data).await
    }

    async fn put_commit_object(&self, hash: &str, data: Vec<u8>) -> Result<(), VfsError> {
        self.put_bytes(&format!("commits/{hash}.bin"), data).await
    }

    async fn put_snapshot_bundle(&self, name: &str, data: Vec<u8>) -> Result<(), VfsError> {
        self.put_bytes(&format!("snapshots/{name}.bin"), data).await
    }
}

pub struct LocalBlobStore {
    base_dir: PathBuf,
}

impl LocalBlobStore {
    pub fn new(base_dir: impl AsRef<Path>) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
        }
    }

    fn key_path(&self, key: &str) -> PathBuf {
        self.base_dir.join(key)
    }
}

#[async_trait]
impl RemoteBlobStore for LocalBlobStore {
    async fn put_bytes(&self, key: &str, data: Vec<u8>) -> Result<(), VfsError> {
        let path = self.key_path(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(path, data).await?;
        Ok(())
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>, VfsError> {
        Ok(tokio::fs::read(self.key_path(key)).await?)
    }
}

pub struct R2BlobStore {
    client: Client,
    bucket: String,
    prefix: String,
}

impl R2BlobStore {
    pub async fn new(config: R2BlobStoreConfig) -> Result<Self, VfsError> {
        let credentials = Credentials::new(
            config.access_key_id,
            config.secret_access_key,
            None,
            None,
            "stratum-r2",
        );

        let shared_config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(config.endpoint)
            .region(Region::new(config.region))
            .credentials_provider(credentials)
            .load()
            .await;

        Ok(Self {
            client: Client::new(&shared_config),
            bucket: config.bucket,
            prefix: config.prefix.trim_matches('/').to_string(),
        })
    }

    fn object_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key)
        }
    }
}

#[async_trait]
impl RemoteBlobStore for R2BlobStore {
    async fn put_bytes(&self, key: &str, data: Vec<u8>) -> Result<(), VfsError> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(self.object_key(key))
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| VfsError::IoError(std::io::Error::other(e.to_string())))?;
        Ok(())
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>, VfsError> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.object_key(key))
            .send()
            .await
            .map_err(|e| {
                if e.as_service_error()
                    .is_some_and(|error| error.is_no_such_key())
                {
                    VfsError::ObjectNotFound {
                        id: "remote object".to_string(),
                    }
                } else {
                    VfsError::IoError(std::io::Error::other(e.to_string()))
                }
            })?;

        let bytes = output
            .body
            .collect()
            .await
            .map_err(|e| VfsError::IoError(std::io::Error::other(e.to_string())))?;
        Ok(bytes.into_bytes().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::blob_object::{BlobObjectStore, InMemoryObjectMetadataStore};
    use crate::backend::{ObjectStore, ObjectWrite, RepoId};
    use crate::store::{ObjectId, ObjectKind};
    use std::sync::Arc;
    use uuid::Uuid;

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("stratum-remote-blob-{name}-{}", Uuid::new_v4()))
    }

    fn r2_tests_enabled() -> bool {
        std::env::var("STRATUM_R2_TEST_ENABLED").as_deref() == Ok("1")
            || std::env::var("STRATUM_R2_TEST_REQUIRED").as_deref() == Ok("1")
    }

    fn r2_live_test_config() -> Result<Option<R2BlobStoreConfig>, VfsError> {
        if !r2_tests_enabled() {
            return Ok(None);
        }

        let required_vars = [
            "STRATUM_R2_BUCKET",
            "STRATUM_R2_ENDPOINT",
            "STRATUM_R2_ACCESS_KEY_ID",
            "STRATUM_R2_SECRET_ACCESS_KEY",
        ];
        let missing = required_vars
            .iter()
            .copied()
            .filter(|name| std::env::var(name).map_or(true, |value| value.is_empty()))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(VfsError::InvalidArgs {
                message: format!(
                    "missing required R2 object-store environment variables: {}",
                    missing.join(", ")
                ),
            });
        }

        let mut config = R2BlobStoreConfig::from_env().ok_or_else(|| VfsError::InvalidArgs {
            message: "missing required R2 object-store environment variables".to_string(),
        })?;
        // Live tests use unique prefixes so object lifecycle cleanup can remain
        // a future production concern instead of coupling this gate to deletes.
        let test_prefix = format!("tests/{}", Uuid::new_v4());
        config.prefix = if config.prefix.trim_matches('/').is_empty() {
            test_prefix
        } else {
            format!("{}/{}", config.prefix.trim_matches('/'), test_prefix)
        };
        Ok(Some(config))
    }

    #[tokio::test]
    async fn local_blob_store_should_round_trip_nested_namespaced_keys() {
        let base_dir = temp_dir("nested");
        let store = LocalBlobStore::new(&base_dir);
        let key = "repos/repo_test/objects/blob/abcdef0123456789";
        let bytes = b"nested object bytes".to_vec();

        store.put_bytes(key, bytes.clone()).await.unwrap();
        let loaded = store.get_bytes(key).await.unwrap();

        assert_eq!(loaded, bytes);
        let _ = tokio::fs::remove_dir_all(base_dir).await;
    }

    #[tokio::test]
    async fn r2_blob_store_live_integration() -> Result<(), VfsError> {
        let Some(config) = r2_live_test_config()? else {
            println!(
                "Skipping R2 blob-store live integration; set STRATUM_R2_TEST_ENABLED=1 to run."
            );
            return Ok(());
        };

        let store = Arc::new(R2BlobStore::new(config).await?);
        let key = "direct/round-trip.bin";
        let bytes = b"r2 live integration bytes\x00\x01\xfe".to_vec();

        store.put_bytes(key, bytes.clone()).await?;
        let loaded = store.get_bytes(key).await?;
        assert_eq!(loaded, bytes);

        let missing = store
            .get_bytes("direct/missing.bin")
            .await
            .expect_err("missing R2 key should be reported as ObjectNotFound");
        assert!(matches!(missing, VfsError::ObjectNotFound { .. }));

        let object_store =
            BlobObjectStore::new(store, Arc::new(InMemoryObjectMetadataStore::new()));
        let repo_id = RepoId::new("repo_r2_live")?;
        let object_bytes = b"raw object bytes through BlobObjectStore\x00\x01\x02\xff".to_vec();
        let object_id = ObjectId::from_bytes(&object_bytes);
        let write = ObjectWrite {
            repo_id: repo_id.clone(),
            id: object_id,
            kind: ObjectKind::Blob,
            bytes: object_bytes.clone(),
        };

        let stored = object_store.put(write).await?;
        let loaded = object_store
            .get(&repo_id, object_id, ObjectKind::Blob)
            .await?
            .expect("object metadata should exist after put");

        assert_eq!(stored.id, object_id);
        assert_eq!(stored.kind, ObjectKind::Blob);
        assert_eq!(stored.bytes, object_bytes);
        assert_eq!(loaded.id, object_id);
        assert_eq!(loaded.kind, ObjectKind::Blob);
        assert_eq!(loaded.bytes, object_bytes);

        Ok(())
    }
}
