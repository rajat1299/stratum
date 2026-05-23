use crate::error::VfsError;
use crate::sparse_cache::{
    DirtyEntryState, DirtyOperation, SparseCache, WritebackState, normalize_cache_path,
    object_id_from_hex, sparse_cache_error, to_i64, u64_from_i64,
};
use crate::store::ObjectId;
use crate::vcs::{CommitId, RefName};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

const WRITE_BACK_DISABLED: &str = "sparse write-back disabled";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SparseWriteBackMode {
    Disabled,
    EnabledForTests,
}

#[derive(Clone, PartialEq, Eq)]
pub struct SparseWriteBackPlannerInput {
    pub view_id: i64,
    pub mode: SparseWriteBackMode,
    pub base_ref: RefName,
    pub session_ref: RefName,
    pub author: String,
    pub timestamp: u64,
}

impl fmt::Debug for SparseWriteBackPlannerInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SparseWriteBackPlannerInput")
            .field("view_id", &self.view_id)
            .field("mode", &self.mode)
            .field("base_ref_present", &true)
            .field("session_ref_present", &true)
            .field("author_present", &!self.author.is_empty())
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SparseWriteBackFlushPlan {
    pub mode: SparseWriteBackMode,
    pub execution_allowed: bool,
    pub blocked_reason: Option<String>,
    pub queued_count: u64,
    pub intent_count: u64,
    pub changed_paths: Vec<String>,
    pub intents: Vec<SparseDurableMutationIntent>,
}

impl fmt::Debug for SparseWriteBackFlushPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SparseWriteBackFlushPlan")
            .field("mode", &self.mode)
            .field("execution_allowed", &self.execution_allowed)
            .field("blocked_reason", &self.blocked_reason)
            .field("queued_count", &self.queued_count)
            .field("intent_count", &self.intent_count)
            .field("changed_path_count", &self.changed_paths.len())
            .field("changed_paths", &"<redacted>")
            .field("intents", &self.intents)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SparseDurableMutationIntent {
    pub queue_id: i64,
    pub dirty_id: i64,
    pub inode_id: u64,
    pub operation_id: String,
    pub fingerprint: String,
    pub base_ref: RefName,
    pub session_ref: RefName,
    pub operation: SparseDurableMutationOperationIntent,
    pub author: String,
    pub timestamp: u64,
    pub source_commit_id: Option<CommitId>,
    pub source_ref_name: Option<RefName>,
    pub source_ref_version: Option<u64>,
    pub queue_source_identity_present: bool,
}

impl fmt::Debug for SparseDurableMutationIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SparseDurableMutationIntent")
            .field("queue_id_present", &true)
            .field("dirty_id_present", &true)
            .field("inode_id_present", &true)
            .field("operation_id_present", &!self.operation_id.is_empty())
            .field("fingerprint_present", &!self.fingerprint.is_empty())
            .field("base_ref_present", &true)
            .field("session_ref_present", &true)
            .field("operation", &self.operation)
            .field("author_present", &!self.author.is_empty())
            .field("timestamp", &self.timestamp)
            .field("source_commit_id_present", &self.source_commit_id.is_some())
            .field("source_ref_name_present", &self.source_ref_name.is_some())
            .field("source_ref_version", &self.source_ref_version)
            .field(
                "queue_source_identity_present",
                &self.queue_source_identity_present,
            )
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum SparseDurableMutationOperationIntent {
    WriteFile {
        path: String,
        content_object_id: ObjectId,
        content_len: u64,
        mode: u16,
        uid: u32,
        gid: u32,
        mime_type: Option<String>,
        custom_attrs: BTreeMap<String, String>,
    },
}

impl fmt::Debug for SparseDurableMutationOperationIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WriteFile {
                path: _,
                content_object_id: _,
                content_len,
                mode,
                uid,
                gid,
                mime_type,
                custom_attrs,
            } => formatter
                .debug_struct("WriteFile")
                .field("path", &"<redacted>")
                .field("content_object_id", &"<redacted>")
                .field("content_len", content_len)
                .field("mode", mode)
                .field("uid", uid)
                .field("gid", gid)
                .field("mime_type_present", &mime_type.is_some())
                .field("custom_attr_count", &custom_attrs.len())
                .finish(),
        }
    }
}

pub fn plan_sparse_writeback_flush(
    cache: &SparseCache,
    input: SparseWriteBackPlannerInput,
) -> Result<SparseWriteBackFlushPlan, VfsError> {
    if input.mode == SparseWriteBackMode::Disabled {
        let summary = pending_writeback_summary(cache, input.view_id)?;
        return Ok(SparseWriteBackFlushPlan {
            mode: input.mode,
            execution_allowed: false,
            blocked_reason: Some(WRITE_BACK_DISABLED.to_string()),
            queued_count: summary.queued_count,
            intent_count: 0,
            changed_paths: summary.changed_paths,
            intents: Vec::new(),
        });
    }

    let rows = pending_writeback_rows(cache, input.view_id)?;
    let changed_paths = rows.iter().map(|row| row.path.clone()).collect::<Vec<_>>();
    let queued_count = rows.len() as u64;

    let mut intents = Vec::with_capacity(rows.len());
    for row in rows {
        intents.push(row.into_intent(&input)?);
    }

    Ok(SparseWriteBackFlushPlan {
        mode: input.mode,
        execution_allowed: true,
        blocked_reason: None,
        queued_count,
        intent_count: intents.len() as u64,
        changed_paths,
        intents,
    })
}

#[derive(Clone)]
struct PendingWritebackRow {
    queue_id: i64,
    dirty_id: i64,
    view_id: i64,
    inode_id: u64,
    path: String,
    operation: DirtyOperation,
    dirty_state: DirtyEntryState,
    queue_state: WritebackState,
    source_commit_id: Option<CommitId>,
    source_ref_name: Option<RefName>,
    source_ref_version: Option<u64>,
    content_len: u64,
    content_object_id: ObjectId,
    mode: u32,
    uid: u32,
    gid: u32,
    mime_type: Option<String>,
    custom_attrs: BTreeMap<String, String>,
    queued_operation_id: String,
    source_identity: String,
    source_identity_present: bool,
}

impl PendingWritebackRow {
    fn into_intent(
        self,
        input: &SparseWriteBackPlannerInput,
    ) -> Result<SparseDurableMutationIntent, VfsError> {
        let normalized_path = normalize_cache_path(&self.path)?;
        let custom_attrs_identity = canonical_custom_attrs(&self.custom_attrs);
        let source_commit =
            source_commit_hex(self.source_commit_id).unwrap_or_else(|| "none".to_string());
        let source_ref_name = self
            .source_ref_name
            .as_ref()
            .map(|name| name.as_str().to_string())
            .unwrap_or_else(|| "none".to_string());
        let source_ref_version = self
            .source_ref_version
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "none".to_string());
        let content_object_id = self.content_object_id.to_hex();
        let operation_id = stable_digest([
            "sparse-writeback-operation".to_string(),
            self.view_id.to_string(),
            self.queue_id.to_string(),
            self.dirty_id.to_string(),
            self.inode_id.to_string(),
            digest_part(input.base_ref.as_str()),
            digest_part(input.session_ref.as_str()),
            digest_part(&input.author),
            input.timestamp.to_string(),
            digest_part(&self.queued_operation_id),
            digest_part(&self.source_identity),
            digest_part(&normalized_path),
            self.content_len.to_string(),
            content_object_id.clone(),
            self.mode.to_string(),
            self.uid.to_string(),
            self.gid.to_string(),
            digest_part(self.mime_type.as_deref().unwrap_or("none")),
            digest_part(&custom_attrs_identity),
            source_commit.clone(),
            digest_part(&source_ref_name),
            source_ref_version.clone(),
        ]);
        let fingerprint = stable_digest([
            "sparse-writeback-fingerprint".to_string(),
            operation_id.clone(),
            digest_part(input.base_ref.as_str()),
            digest_part(input.session_ref.as_str()),
            digest_part(&input.author),
            input.timestamp.to_string(),
            digest_part(&self.queued_operation_id),
            digest_part(&self.source_identity),
            digest_part(&normalized_path),
            content_object_id,
            self.content_len.to_string(),
            self.mode.to_string(),
            self.uid.to_string(),
            self.gid.to_string(),
            digest_part(self.mime_type.as_deref().unwrap_or("none")),
            digest_part(&custom_attrs_identity),
            source_commit,
            digest_part(&source_ref_name),
            source_ref_version,
        ]);

        Ok(SparseDurableMutationIntent {
            queue_id: self.queue_id,
            dirty_id: self.dirty_id,
            inode_id: self.inode_id,
            operation_id,
            fingerprint,
            base_ref: input.base_ref.clone(),
            session_ref: input.session_ref.clone(),
            operation: match self.operation {
                DirtyOperation::WriteFile => SparseDurableMutationOperationIntent::WriteFile {
                    path: normalized_path,
                    content_object_id: self.content_object_id,
                    content_len: self.content_len,
                    mode: u16::try_from(self.mode).map_err(|_| sparse_cache_error())?,
                    uid: self.uid,
                    gid: self.gid,
                    mime_type: self.mime_type,
                    custom_attrs: self.custom_attrs,
                },
            },
            author: input.author.clone(),
            timestamp: input.timestamp,
            source_commit_id: self.source_commit_id,
            source_ref_name: self.source_ref_name,
            source_ref_version: self.source_ref_version,
            queue_source_identity_present: self.source_identity_present,
        })
    }
}

struct PendingWritebackSummary {
    queued_count: u64,
    changed_paths: Vec<String>,
}

fn pending_writeback_summary(
    cache: &SparseCache,
    view_id: i64,
) -> Result<PendingWritebackSummary, VfsError> {
    let mut statement = cache
        .connection
        .prepare(
            "SELECT d.path
            FROM sparse_cache_writeback_queue q
            INNER JOIN sparse_cache_dirty_entries d ON d.dirty_id = q.dirty_id
            WHERE d.view_id = ?1
              AND d.state = 'queued'
              AND q.state = 'pending'
            ORDER BY q.queue_id",
        )
        .map_err(|_| sparse_cache_error())?;
    let changed_paths = statement
        .query_map([view_id], |row| row.get::<_, String>(0))
        .map_err(|_| sparse_cache_error())?
        .map(|path| {
            path.map_err(|_| sparse_cache_error())
                .and_then(|path| normalize_cache_path(&path))
        })
        .collect::<Result<Vec<_>, VfsError>>()?;
    Ok(PendingWritebackSummary {
        queued_count: changed_paths.len() as u64,
        changed_paths,
    })
}

fn pending_writeback_rows(
    cache: &SparseCache,
    view_id: i64,
) -> Result<Vec<PendingWritebackRow>, VfsError> {
    let mut statement = cache
        .connection
        .prepare(
            "SELECT q.queue_id,
                    d.dirty_id,
                    d.view_id,
                    d.inode_id,
                    d.path,
                    d.operation,
                    d.state,
                    q.state,
                    d.base_commit_id,
                    d.base_ref_name,
                    d.base_ref_version,
                    d.content_len,
                    d.content_object_id,
                    i.mode,
                    i.uid,
                    i.gid,
                    i.mime_type,
                    i.custom_attrs_json,
                    q.operation_id,
                    q.source_identity
            FROM sparse_cache_writeback_queue q
            INNER JOIN sparse_cache_dirty_entries d ON d.dirty_id = q.dirty_id
            INNER JOIN sparse_cache_inodes i
                ON i.view_id = d.view_id AND i.inode_id = d.inode_id
            WHERE d.view_id = ?1
              AND d.state = 'queued'
              AND q.state = 'pending'
            ORDER BY q.queue_id",
        )
        .map_err(|_| sparse_cache_error())?;
    let rows = statement
        .query_map([view_id], pending_writeback_row_from_sql)
        .map_err(|_| sparse_cache_error())?
        .map(|row| {
            row.map_err(|_| sparse_cache_error())
                .and_then(|row| row)
                .and_then(validate_pending_row)
        })
        .collect::<Result<Vec<_>, VfsError>>()?;
    Ok(rows)
}

fn pending_writeback_row_from_sql(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<Result<PendingWritebackRow, VfsError>> {
    let operation: String = row.get(5)?;
    let dirty_state: String = row.get(6)?;
    let queue_state: String = row.get(7)?;
    let commit_id: Option<String> = row.get(8)?;
    let ref_name: Option<String> = row.get(9)?;
    let ref_version: Option<i64> = row.get(10)?;
    let custom_attrs_json: String = row.get(17)?;
    let queued_operation_id: String = row.get(18)?;
    let source_identity: String = row.get(19)?;

    Ok((|| {
        Ok(PendingWritebackRow {
            queue_id: row.get(0).map_err(|_| sparse_cache_error())?,
            dirty_id: row.get(1).map_err(|_| sparse_cache_error())?,
            view_id: row.get(2).map_err(|_| sparse_cache_error())?,
            inode_id: u64_from_i64(row.get(3).map_err(|_| sparse_cache_error())?)?,
            path: row.get(4).map_err(|_| sparse_cache_error())?,
            operation: dirty_operation_from_sql(&operation)?,
            dirty_state: dirty_state_from_sql(&dirty_state)?,
            queue_state: writeback_state_from_sql(&queue_state)?,
            source_commit_id: commit_id
                .as_deref()
                .map(object_id_from_hex)
                .transpose()?
                .map(CommitId::from),
            source_ref_name: ref_name
                .as_deref()
                .map(|value| RefName::new(value).map_err(|_| sparse_cache_error()))
                .transpose()?,
            source_ref_version: ref_version.map(u64_from_i64).transpose()?,
            content_len: u64_from_i64(row.get(11).map_err(|_| sparse_cache_error())?)?,
            content_object_id: object_id_from_hex(
                &row.get::<_, String>(12).map_err(|_| sparse_cache_error())?,
            )?,
            mode: u32::try_from(row.get::<_, i64>(13).map_err(|_| sparse_cache_error())?)
                .map_err(|_| sparse_cache_error())?,
            uid: u32::try_from(row.get::<_, i64>(14).map_err(|_| sparse_cache_error())?)
                .map_err(|_| sparse_cache_error())?,
            gid: u32::try_from(row.get::<_, i64>(15).map_err(|_| sparse_cache_error())?)
                .map_err(|_| sparse_cache_error())?,
            mime_type: row.get(16).map_err(|_| sparse_cache_error())?,
            custom_attrs: serde_json::from_str(&custom_attrs_json)
                .map_err(|_| sparse_cache_error())?,
            queued_operation_id,
            source_identity: source_identity.clone(),
            source_identity_present: !source_identity.is_empty(),
        })
    })())
}

fn validate_pending_row(mut row: PendingWritebackRow) -> Result<PendingWritebackRow, VfsError> {
    if row.dirty_state != DirtyEntryState::Queued || row.queue_state != WritebackState::Pending {
        return Err(sparse_cache_error());
    }
    if !matches!(row.operation, DirtyOperation::WriteFile) {
        return Err(sparse_cache_error());
    }
    row.path = normalize_cache_path(&row.path)?;
    if row.mode > u32::from(u16::MAX) {
        return Err(sparse_cache_error());
    }
    let _ = to_i64(row.content_len)?;
    Ok(row)
}

fn dirty_operation_from_sql(value: &str) -> Result<DirtyOperation, VfsError> {
    match value {
        "write_file" => Ok(DirtyOperation::WriteFile),
        _ => Err(sparse_cache_error()),
    }
}

fn dirty_state_from_sql(value: &str) -> Result<DirtyEntryState, VfsError> {
    match value {
        "dirty" => Ok(DirtyEntryState::Dirty),
        "queued" => Ok(DirtyEntryState::Queued),
        "flushed" => Ok(DirtyEntryState::Flushed),
        "failed" => Ok(DirtyEntryState::Failed),
        _ => Err(sparse_cache_error()),
    }
}

fn writeback_state_from_sql(value: &str) -> Result<WritebackState, VfsError> {
    match value {
        "pending" => Ok(WritebackState::Pending),
        "running" => Ok(WritebackState::Running),
        "flushed" => Ok(WritebackState::Flushed),
        "failed" => Ok(WritebackState::Failed),
        "disabled" => Ok(WritebackState::Disabled),
        _ => Err(sparse_cache_error()),
    }
}

fn source_commit_hex(commit_id: Option<CommitId>) -> Option<String> {
    commit_id.map(CommitId::to_hex)
}

fn digest_part(value: &str) -> String {
    stable_digest(["sparse-writeback-part".to_string(), value.to_string()])
}

fn canonical_custom_attrs(custom_attrs: &BTreeMap<String, String>) -> String {
    custom_attrs
        .iter()
        .map(|(key, value)| format!("{}:{}={}", key.len(), key, digest_part(value)))
        .collect::<Vec<_>>()
        .join(";")
}

fn stable_digest(parts: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        let part = part.as_ref();
        hasher.update(part.len().to_string().as_bytes());
        hasher.update(b":");
        hasher.update(part.as_bytes());
        hasher.update(b";");
    }
    let digest = hasher.finalize();
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::RepoId;
    use crate::error::VfsError;
    use crate::sparse_cache::{
        CacheViewIdentity, CachedInode, CachedNodeKind, SparseCache, WritebackState,
    };
    use crate::store::{ObjectId, ObjectKind};
    use crate::vcs::{CommitId, RefName};
    use std::collections::BTreeMap;

    #[test]
    fn disabled_writeback_mode_blocks_planning_without_claiming_queue() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let base_object_id = object_id(b"disabled base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/secret/disabled.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"disabled local bytes",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-secret-operation",
            "queued-secret-source",
            WritebackState::Pending,
            110,
        )?;

        let plan = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                view_id,
                mode: SparseWriteBackMode::Disabled,
                base_ref: RefName::new("main")?,
                session_ref: RefName::new("agent/test/session")?,
                author: "test-author".to_string(),
                timestamp: 120,
            },
        )?;

        assert!(!plan.execution_allowed);
        assert_eq!(
            plan.blocked_reason.as_deref(),
            Some("sparse write-back disabled")
        );
        assert_eq!(plan.queued_count, 1);
        assert_eq!(plan.intent_count, 0);
        assert!(plan.intents.is_empty());
        assert_eq!(plan.changed_paths, vec!["/secret/disabled.txt"]);
        assert_eq!(cache.writeback_progress(view_id)?.pending, 1);
        assert_eq!(
            cache.dirty_entry_for_inode(view_id, 2)?.unwrap().state,
            crate::sparse_cache::DirtyEntryState::Queued
        );

        Ok(())
    }

    #[test]
    fn enabled_test_mode_builds_durable_write_file_intent_from_dirty_entry() -> Result<(), VfsError>
    {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let base_object_id = object_id(b"enabled base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "src/../README.md",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"enabled local bytes",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-secret-operation",
            "queued-secret-source",
            WritebackState::Pending,
            110,
        )?;

        let input = SparseWriteBackPlannerInput {
            view_id,
            mode: SparseWriteBackMode::EnabledForTests,
            base_ref: RefName::new("main")?,
            session_ref: RefName::new("agent/test/session")?,
            author: "test-author".to_string(),
            timestamp: 120,
        };
        let plan = plan_sparse_writeback_flush(&cache, input.clone())?;
        let repeat = plan_sparse_writeback_flush(&cache, input)?;

        assert!(plan.execution_allowed);
        assert_eq!(plan.queued_count, 1);
        assert_eq!(plan.intent_count, 1);
        assert_eq!(plan.changed_paths, vec!["/README.md"]);
        assert_eq!(plan.intents, repeat.intents);

        let intent = &plan.intents[0];
        assert_eq!(intent.queue_id, 1);
        assert_eq!(intent.dirty_id, dirty.dirty_id);
        assert_eq!(intent.inode_id, 2);
        assert_eq!(intent.base_ref, RefName::new("main")?);
        assert_eq!(intent.session_ref, RefName::new("agent/test/session")?);
        assert_eq!(intent.author, "test-author");
        assert_eq!(intent.timestamp, 120);
        assert_eq!(intent.source_commit_id, cache_view_identity().commit_id);
        assert_eq!(intent.source_ref_name, cache_view_identity().ref_name);
        assert_eq!(intent.source_ref_version, cache_view_identity().ref_version);
        assert_eq!(intent.queue_source_identity_present, true);
        assert_eq!(intent.operation_id.len(), 64);
        assert_eq!(intent.fingerprint.len(), 64);
        assert_eq!(
            intent.operation,
            SparseDurableMutationOperationIntent::WriteFile {
                path: "/README.md".to_string(),
                content_object_id: object_id(b"enabled local bytes"),
                content_len: 19,
                mode: 0o100644,
                uid: 501,
                gid: 20,
                mime_type: None,
                custom_attrs: BTreeMap::new(),
            }
        );

        assert_eq!(cache.writeback_progress(view_id)?.pending, 1);

        Ok(())
    }

    #[test]
    fn intent_identity_changes_with_durable_operation_inputs() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let base_object_id = object_id(b"identity base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/identity.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"identity local bytes",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-identity-operation",
            "queued-identity-source",
            WritebackState::Pending,
            110,
        )?;
        let input = SparseWriteBackPlannerInput {
            view_id,
            mode: SparseWriteBackMode::EnabledForTests,
            base_ref: RefName::new("main")?,
            session_ref: RefName::new("agent/test/session")?,
            author: "test-author".to_string(),
            timestamp: 120,
        };
        let baseline = plan_sparse_writeback_flush(&cache, input.clone())?.intents[0].clone();

        let different_session = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                session_ref: RefName::new("agent/test/other-session")?,
                ..input.clone()
            },
        )?
        .intents[0]
            .clone();
        assert_ne!(different_session.operation_id, baseline.operation_id);
        assert_ne!(different_session.fingerprint, baseline.fingerprint);

        cache
            .connection
            .execute(
                "UPDATE sparse_cache_writeback_queue SET operation_id = ?1 WHERE dirty_id = ?2",
                rusqlite::params!["queued-identity-operation-2", dirty.dirty_id],
            )
            .expect("change queued operation id");
        let queued_operation =
            plan_sparse_writeback_flush(&cache, input.clone())?.intents[0].clone();
        assert_ne!(queued_operation.operation_id, baseline.operation_id);
        assert_ne!(queued_operation.fingerprint, baseline.fingerprint);

        cache
            .connection
            .execute(
                "UPDATE sparse_cache_writeback_queue SET source_identity = ?1 WHERE dirty_id = ?2",
                rusqlite::params!["queued-identity-source-2", dirty.dirty_id],
            )
            .expect("change queued source identity");
        let source = plan_sparse_writeback_flush(&cache, input.clone())?.intents[0].clone();
        assert_ne!(source.operation_id, queued_operation.operation_id);
        assert_ne!(source.fingerprint, queued_operation.fingerprint);

        cache
            .connection
            .execute(
                "UPDATE sparse_cache_dirty_entries SET path = ?1 WHERE dirty_id = ?2",
                rusqlite::params!["/nested/../identity-renamed.txt", dirty.dirty_id],
            )
            .expect("change queued path");
        let path_plan = plan_sparse_writeback_flush(&cache, input.clone())?;
        let path = path_plan.intents[0].clone();
        assert_eq!(path_plan.changed_paths, vec!["/identity-renamed.txt"]);
        assert_ne!(path.operation_id, source.operation_id);
        assert_ne!(path.fingerprint, source.fingerprint);

        cache
            .connection
            .execute(
                "UPDATE sparse_cache_inodes SET mode = ?1, uid = ?2 WHERE view_id = ?3 AND inode_id = ?4",
                rusqlite::params![0o100600_i64, 502_i64, view_id, 2_i64],
            )
            .expect("change queued metadata");
        let metadata = plan_sparse_writeback_flush(&cache, input)?.intents[0].clone();
        assert_ne!(metadata.operation_id, path.operation_id);
        assert_ne!(metadata.fingerprint, path.fingerprint);

        Ok(())
    }

    #[test]
    fn enabled_test_mode_rejects_corrupt_queued_metadata_without_lossy_defaults()
    -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let base_object_id = object_id(b"corrupt metadata base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/corrupt.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"corrupt local bytes",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-corrupt-operation",
            "queued-corrupt-source",
            WritebackState::Pending,
            110,
        )?;
        cache
            .connection
            .execute(
                "UPDATE sparse_cache_inodes SET custom_attrs_json = '{' WHERE view_id = ?1 AND inode_id = ?2",
                rusqlite::params![view_id, 2_i64],
            )
            .expect("corrupt queued inode metadata");

        let error = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                view_id,
                mode: SparseWriteBackMode::EnabledForTests,
                base_ref: RefName::new("main")?,
                session_ref: RefName::new("agent/test/session")?,
                author: "test-author".to_string(),
                timestamp: 120,
            },
        )
        .expect_err("corrupt queued metadata should reject planning");

        assert!(matches!(error, VfsError::CorruptStore { .. }));
        assert_eq!(cache.writeback_progress(view_id)?.pending, 1);

        Ok(())
    }

    #[test]
    fn disabled_mode_blocks_even_when_execution_metadata_is_corrupt() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&cache_view_identity())?;
        let base_object_id = object_id(b"disabled corrupt metadata base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/disabled-corrupt.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"disabled corrupt local bytes",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "queued-disabled-corrupt-operation",
            "queued-disabled-corrupt-source",
            WritebackState::Pending,
            110,
        )?;
        cache
            .connection
            .execute(
                "UPDATE sparse_cache_inodes SET custom_attrs_json = '{' WHERE view_id = ?1 AND inode_id = ?2",
                rusqlite::params![view_id, 2_i64],
            )
            .expect("corrupt queued inode metadata");

        let plan = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                view_id,
                mode: SparseWriteBackMode::Disabled,
                base_ref: RefName::new("main")?,
                session_ref: RefName::new("agent/test/session")?,
                author: "test-author".to_string(),
                timestamp: 120,
            },
        )?;

        assert!(!plan.execution_allowed);
        assert_eq!(plan.queued_count, 1);
        assert_eq!(plan.intent_count, 0);
        assert!(plan.intents.is_empty());
        assert_eq!(plan.changed_paths, vec!["/disabled-corrupt.txt"]);
        assert_eq!(cache.writeback_progress(view_id)?.pending, 1);

        Ok(())
    }

    #[test]
    fn flush_plan_debug_redacts_paths_repo_ids_object_ids_and_content() -> Result<(), VfsError> {
        let cache = SparseCache::open_in_memory()?;
        let view_id = cache.insert_view(&CacheViewIdentity {
            repo_id: RepoId::new("secret_repo")?,
            root_tree_id: object_id(b"secret root tree"),
            commit_id: Some(CommitId::from(object_id(b"secret commit"))),
            ref_name: Some(RefName::new("main")?),
            ref_version: Some(7),
        })?;
        let base_object_id = object_id(b"secret base object");
        put_dirty_file_inode(&cache, view_id, 2, base_object_id)?;
        let dirty = cache.write_dirty_file(
            view_id,
            2,
            "/secret/path.txt",
            Some(base_object_id),
            Some(ObjectKind::Blob),
            b"secret local content",
            100,
        )?;
        cache.enqueue_writeback(
            dirty.dirty_id,
            "secret-operation-id",
            "secret-source-identity",
            WritebackState::Pending,
            110,
        )?;

        let plan = plan_sparse_writeback_flush(
            &cache,
            SparseWriteBackPlannerInput {
                view_id,
                mode: SparseWriteBackMode::EnabledForTests,
                base_ref: RefName::new("main")?,
                session_ref: RefName::new("agent/test/session")?,
                author: "secret-author".to_string(),
                timestamp: 120,
            },
        )?;
        let debug = format!("{plan:?}");

        assert!(debug.contains("SparseWriteBackFlushPlan"));
        assert!(debug.contains("changed_path_count"));
        for secret in [
            "secret",
            "/secret/path.txt",
            "secret_repo",
            &base_object_id.to_hex(),
            &object_id(b"secret local content").to_hex(),
            "secret-operation-id",
            "secret-source-identity",
            "secret-author",
        ] {
            assert!(!debug.contains(secret), "debug leaked {secret}");
        }

        Ok(())
    }

    fn cache_view_identity() -> CacheViewIdentity {
        CacheViewIdentity {
            repo_id: RepoId::new("local").unwrap(),
            root_tree_id: object_id(b"root tree"),
            commit_id: Some(CommitId::from(object_id(b"commit"))),
            ref_name: Some(RefName::new("main").unwrap()),
            ref_version: Some(7),
        }
    }

    fn put_dirty_file_inode(
        cache: &SparseCache,
        view_id: i64,
        inode_id: u64,
        base_object_id: ObjectId,
    ) -> Result<(), VfsError> {
        cache.put_inode(&CachedInode {
            view_id,
            inode_id,
            node_kind: CachedNodeKind::File,
            object_id: Some(base_object_id),
            object_kind: Some(ObjectKind::Blob),
            mode: 0o100644,
            uid: 501,
            gid: 20,
            nlink: 1,
            size: 0,
            size_known: true,
            block_size: 4096,
            blocks: 0,
            mtime_secs: 1,
            mtime_nanos: 2,
            ctime_secs: 3,
            ctime_nanos: 4,
            mime_type: None,
            custom_attrs: BTreeMap::new(),
            lookup_count: 0,
        })
    }

    fn object_id(bytes: &[u8]) -> ObjectId {
        ObjectId::from_bytes(bytes)
    }
}
