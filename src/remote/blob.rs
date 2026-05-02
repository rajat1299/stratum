use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_config::Region;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;

use crate::error::VfsError;

#[derive(Clone)]
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

impl fmt::Debug for R2BlobStoreConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("R2BlobStoreConfig")
            .field("bucket", &self.bucket)
            .field("endpoint", &sanitize_endpoint_for_debug(&self.endpoint))
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .field("region", &self.region)
            .field("prefix", &self.prefix)
            .finish()
    }
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

#[async_trait]
pub trait RemoteBlobStore: Send + Sync {
    async fn put_bytes_with_condition(
        &self,
        key: &str,
        data: Vec<u8>,
        condition: BlobPutCondition,
    ) -> Result<BlobPutOutcome, VfsError>;

    async fn put_bytes(&self, key: &str, data: Vec<u8>) -> Result<(), VfsError> {
        self.put_bytes_with_condition(key, data, BlobPutCondition::None)
            .await?;
        Ok(())
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>, VfsError>;
    async fn delete_bytes(&self, key: &str) -> Result<(), VfsError>;
    async fn list_keys(&self, prefix: &str) -> Result<Vec<RemoteBlobListing>, VfsError>;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobPutCondition {
    None,
    IfAbsent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobPutOutcome {
    Written,
    AlreadyExists,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteBlobListing {
    pub key: String,
    pub size: Option<u64>,
    pub modified_at: Option<SystemTime>,
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

    fn key_path(&self, key: &str) -> Result<PathBuf, VfsError> {
        validate_blob_key(key)?;
        Ok(self.base_dir.join(key))
    }
}

#[async_trait]
impl RemoteBlobStore for LocalBlobStore {
    async fn put_bytes_with_condition(
        &self,
        key: &str,
        data: Vec<u8>,
        condition: BlobPutCondition,
    ) -> Result<BlobPutOutcome, VfsError> {
        let path = self.key_path(key)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        match condition {
            BlobPutCondition::None => tokio::fs::write(path, data).await?,
            BlobPutCondition::IfAbsent => {
                let result = tokio::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .await;
                match result {
                    Ok(mut file) => {
                        use tokio::io::AsyncWriteExt;
                        let write_result = async {
                            file.write_all(&data).await?;
                            file.flush().await
                        }
                        .await;
                        if let Err(error) = write_result {
                            drop(file);
                            let _ = tokio::fs::remove_file(&path).await;
                            return Err(error.into());
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        return Ok(BlobPutOutcome::AlreadyExists);
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Ok(BlobPutOutcome::Written)
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>, VfsError> {
        Ok(tokio::fs::read(self.key_path(key)?).await?)
    }

    async fn delete_bytes(&self, key: &str) -> Result<(), VfsError> {
        match tokio::fs::remove_file(self.key_path(key)?).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<RemoteBlobListing>, VfsError> {
        validate_blob_prefix(prefix)?;
        let mut listings = Vec::new();
        let mut dirs = vec![self.base_dir.clone()];
        while let Some(dir) = dirs.pop() {
            let mut read_dir = match tokio::fs::read_dir(&dir).await {
                Ok(read_dir) => read_dir,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            while let Some(entry) = read_dir.next_entry().await? {
                let file_type = entry.file_type().await?;
                let path = entry.path();
                if file_type.is_dir() {
                    dirs.push(path);
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }
                let key = local_path_to_key(&self.base_dir, &path)?;
                if !key.starts_with(prefix) {
                    continue;
                }
                let metadata = entry.metadata().await.ok();
                listings.push(RemoteBlobListing {
                    key,
                    size: metadata.as_ref().map(|metadata| metadata.len()),
                    modified_at: metadata.and_then(|metadata| metadata.modified().ok()),
                });
            }
        }
        listings.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(listings)
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

    fn object_key(&self, key: &str) -> Result<String, VfsError> {
        validate_blob_key(key)?;
        Ok(self.prefixed_key(key))
    }

    fn list_prefix(&self, prefix: &str) -> Result<String, VfsError> {
        validate_blob_prefix(prefix)?;
        if self.prefix.is_empty() {
            Ok(prefix.to_string())
        } else if prefix.is_empty() {
            Ok(format!("{}/", self.prefix))
        } else {
            Ok(format!("{}/{}", self.prefix, prefix))
        }
    }

    fn prefixed_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key)
        }
    }

    fn strip_store_prefix(&self, key: &str) -> Option<String> {
        if self.prefix.is_empty() {
            Some(key.to_string())
        } else {
            key.strip_prefix(&format!("{}/", self.prefix))
                .map(ToOwned::to_owned)
        }
    }
}

#[async_trait]
impl RemoteBlobStore for R2BlobStore {
    async fn put_bytes_with_condition(
        &self,
        key: &str,
        data: Vec<u8>,
        condition: BlobPutCondition,
    ) -> Result<BlobPutOutcome, VfsError> {
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.object_key(key)?)
            .body(ByteStream::from(data));
        if condition == BlobPutCondition::IfAbsent {
            request = request.if_none_match("*");
        }
        match request.send().await {
            Ok(_) => Ok(BlobPutOutcome::Written),
            Err(error) => match s3_error_code_or_status(&error).as_deref() {
                Some("PreconditionFailed") | Some("412") => Ok(BlobPutOutcome::AlreadyExists),
                Some("ConditionalRequestConflict") | Some("409") => {
                    Err(VfsError::ObjectWriteConflict {
                        message: format!("conditional object write conflicted for key {key}"),
                    })
                }
                _ => Err(VfsError::IoError(std::io::Error::other(error.to_string()))),
            },
        }
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>, VfsError> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.object_key(key)?)
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

    async fn delete_bytes(&self, key: &str) -> Result<(), VfsError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(self.object_key(key)?)
            .send()
            .await
            .map_err(|e| VfsError::IoError(std::io::Error::other(e.to_string())))?;
        Ok(())
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<RemoteBlobListing>, VfsError> {
        let remote_prefix = self.list_prefix(prefix)?;
        let mut listings = Vec::new();
        let mut continuation_token = None::<String>;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(remote_prefix.clone());
            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let output = request
                .send()
                .await
                .map_err(|e| VfsError::IoError(std::io::Error::other(e.to_string())))?;

            for object in output.contents() {
                let Some(remote_key) = object.key() else {
                    continue;
                };
                let Some(key) = self.strip_store_prefix(remote_key) else {
                    continue;
                };
                listings.push(RemoteBlobListing {
                    key,
                    size: object.size().and_then(|size| u64::try_from(size).ok()),
                    modified_at: object
                        .last_modified()
                        .and_then(|modified_at| SystemTime::try_from(*modified_at).ok()),
                });
            }

            if !output.is_truncated().unwrap_or(false) {
                break;
            }
            let Some(next_token) = output.next_continuation_token().map(ToOwned::to_owned) else {
                break;
            };
            continuation_token = Some(next_token);
        }

        listings.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(listings)
    }
}

fn validate_blob_prefix(prefix: &str) -> Result<(), VfsError> {
    if prefix.is_empty() {
        return Ok(());
    }
    let Some(trimmed) = prefix.strip_suffix('/') else {
        return validate_blob_key(prefix);
    };
    if trimmed.is_empty() || trimmed.ends_with('/') {
        return Err(VfsError::InvalidPath {
            path: prefix.to_string(),
        });
    }
    validate_blob_key(trimmed)
}

fn validate_blob_key(key: &str) -> Result<(), VfsError> {
    if key.is_empty() {
        return Err(VfsError::InvalidPath {
            path: key.to_string(),
        });
    }
    if key
        .split('/')
        .any(|segment| matches!(segment, "" | "." | ".."))
    {
        return Err(VfsError::InvalidPath {
            path: key.to_string(),
        });
    }
    Ok(())
}

fn local_path_to_key(base_dir: &Path, path: &Path) -> Result<String, VfsError> {
    let relative = path
        .strip_prefix(base_dir)
        .map_err(|_| VfsError::InvalidPath {
            path: path.display().to_string(),
        })?;
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            _ => {
                return Err(VfsError::InvalidPath {
                    path: path.display().to_string(),
                });
            }
        }
    }
    Ok(parts.join("/"))
}

fn s3_error_code_or_status<E>(error: &aws_sdk_s3::error::SdkError<E>) -> Option<String>
where
    E: ProvideErrorMetadata,
{
    error.code().map(ToOwned::to_owned).or_else(|| {
        error
            .raw_response()
            .map(|response| response.status().as_u16().to_string())
    })
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
    async fn local_blob_store_should_conditionally_create_without_overwriting() {
        let base_dir = temp_dir("conditional");
        let store = LocalBlobStore::new(&base_dir);
        let key = "objects/blob.bin";

        let first = store
            .put_bytes_with_condition(key, b"first".to_vec(), BlobPutCondition::IfAbsent)
            .await
            .unwrap();
        let second = store
            .put_bytes_with_condition(key, b"second".to_vec(), BlobPutCondition::IfAbsent)
            .await
            .unwrap();
        let loaded_after_if_absent = store.get_bytes(key).await.unwrap();
        let overwrite = store
            .put_bytes_with_condition(key, b"third".to_vec(), BlobPutCondition::None)
            .await
            .unwrap();
        let loaded_after_overwrite = store.get_bytes(key).await.unwrap();

        assert_eq!(first, BlobPutOutcome::Written);
        assert_eq!(second, BlobPutOutcome::AlreadyExists);
        assert_eq!(loaded_after_if_absent, b"first");
        assert_eq!(overwrite, BlobPutOutcome::Written);
        assert_eq!(loaded_after_overwrite, b"third");
        let _ = tokio::fs::remove_dir_all(base_dir).await;
    }

    #[tokio::test]
    async fn local_blob_store_should_delete_and_list_prefix_recursively() {
        let base_dir = temp_dir("delete-list");
        let store = LocalBlobStore::new(&base_dir);

        store
            .put_bytes("objects/a.bin", b"removed".to_vec())
            .await
            .unwrap();
        store
            .put_bytes("objects/nested/b.bin", b"kept".to_vec())
            .await
            .unwrap();
        store
            .put_bytes("other/c.bin", b"ignored".to_vec())
            .await
            .unwrap();

        store.delete_bytes("objects/a.bin").await.unwrap();
        store.delete_bytes("objects/missing.bin").await.unwrap();
        let listing = store.list_keys("objects").await.unwrap();

        assert_eq!(listing.len(), 1);
        assert_eq!(listing[0].key, "objects/nested/b.bin");
        assert_eq!(listing[0].size, Some(4));
        assert!(listing[0].modified_at.is_some());
        let _ = tokio::fs::remove_dir_all(base_dir).await;
    }

    #[tokio::test]
    async fn local_blob_store_should_reject_parent_directory_keys() {
        let base_dir = temp_dir("path-traversal");
        let store = LocalBlobStore::new(&base_dir);

        let err = store
            .put_bytes("../escape.bin", b"escape".to_vec())
            .await
            .expect_err("parent directory keys should be rejected");

        assert!(matches!(err, VfsError::InvalidPath { .. }));
        for invalid_key in ["/absolute.bin", "a/./b.bin", "a//b.bin", "a/../b.bin"] {
            let err = store
                .put_bytes(invalid_key, b"invalid".to_vec())
                .await
                .expect_err("lexically invalid keys should be rejected");
            assert!(matches!(err, VfsError::InvalidPath { .. }));
        }
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

        let conditional_key = "direct/conditional.bin";
        let conditional_bytes = b"conditional create bytes".to_vec();
        let conditional_first = store
            .put_bytes_with_condition(
                conditional_key,
                conditional_bytes.clone(),
                BlobPutCondition::IfAbsent,
            )
            .await?;
        let conditional_second = store
            .put_bytes_with_condition(
                conditional_key,
                b"should not overwrite".to_vec(),
                BlobPutCondition::IfAbsent,
            )
            .await?;
        let conditional_loaded = store.get_bytes(conditional_key).await?;
        assert_eq!(conditional_first, BlobPutOutcome::Written);
        assert_eq!(conditional_second, BlobPutOutcome::AlreadyExists);
        assert_eq!(conditional_loaded, conditional_bytes);

        let direct_listing = store.list_keys("direct/").await?;
        assert!(
            direct_listing
                .iter()
                .any(|listing| listing.key == key && listing.size == Some(bytes.len() as u64))
        );
        assert!(direct_listing.iter().any(|listing| {
            listing.key == conditional_key && listing.size == Some(conditional_bytes.len() as u64)
        }));

        store.delete_bytes(key).await?;
        store.delete_bytes(conditional_key).await?;
        store.delete_bytes("direct/missing-delete.bin").await?;
        let direct_listing_after_delete = store.list_keys("direct/").await?;
        assert!(
            !direct_listing_after_delete
                .iter()
                .any(|listing| listing.key == key || listing.key == conditional_key)
        );

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

    #[test]
    fn r2_config_debug_redacts_credentials() {
        let config = R2BlobStoreConfig {
            bucket: "stratum-test".to_string(),
            endpoint: "https://user:raw-endpoint-password@example.r2.cloudflarestorage.com/api?token=raw-endpoint-token".to_string(),
            access_key_id: "visible-access-key-id".to_string(),
            secret_access_key: "visible-secret-access-key".to_string(),
            region: "auto".to_string(),
            prefix: "stratum".to_string(),
        };

        let debug = format!("{config:?}");
        assert!(debug.contains("stratum-test"));
        assert!(debug.contains("https://example.r2.cloudflarestorage.com"));
        assert!(debug.contains("/api"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("raw-endpoint-password"));
        assert!(!debug.contains("raw-endpoint-token"));
        assert!(!debug.contains("visible-access-key-id"));
        assert!(!debug.contains("visible-secret-access-key"));
    }
}
