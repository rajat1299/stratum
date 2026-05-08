use async_trait::async_trait;
use std::sync::Arc;

use crate::auth::session::Session;
use crate::auth::{ROOT_UID, Uid, WHEEL_GID};
use crate::backend::committed_read::DurableCommittedFsReader;
use crate::backend::core_transaction::{
    DurableCoreCommitExecutorSkeleton, DurableCoreCommitMetadataPreflight,
    DurableCoreCommitParentState, DurableCoreStepSemantics, DurableCoreTransactionStep,
};
use crate::backend::{CommitRecord, RefExpectation, RefRecord, RefUpdate, RepoId, StratumStores};
use crate::db::{DbVcsRef, StratumDb};
use crate::error::VfsError;
use crate::fs::{GrepResult, LsEntry, MetadataUpdate, MetadataUpdateResult, StatInfo};
use crate::store::ObjectId;
use crate::store::commit::CommitObject;
use crate::vcs::{CommitId, MAIN_REF, RefName};

pub(crate) type SharedCoreRuntime = Arc<dyn CoreDb>;

pub(crate) type ProtectedPathPredicate = Arc<dyn Fn(&str) -> bool + Send + Sync>;

const DURABLE_CORE_ROUTE_NOT_SUPPORTED: &str =
    "durable core runtime route execution is not supported yet";
const DURABLE_MUTABLE_WORKSPACE_NOT_SUPPORTED: &str =
    "durable mutable workspace route execution is not supported yet";

#[async_trait]
pub(crate) trait CoreDb: Send + Sync {
    async fn login(&self, username: &str) -> Result<Session, VfsError>;
    async fn authenticate_token(&self, raw_token: &str) -> Result<Session, VfsError>;
    async fn session_for_uid(&self, uid: Uid) -> Result<Session, VfsError>;

    async fn cat_with_stat_as(
        &self,
        path: &str,
        session: &Session,
    ) -> Result<(Vec<u8>, StatInfo), VfsError>;
    async fn ls_as(&self, path: Option<&str>, session: &Session) -> Result<Vec<LsEntry>, VfsError>;
    async fn stat_as(&self, path: &str, session: &Session) -> Result<StatInfo, VfsError>;
    async fn tree_as(&self, path: Option<&str>, session: &Session) -> Result<String, VfsError>;
    async fn find_as(
        &self,
        path: Option<&str>,
        pattern: Option<&str>,
        session: &Session,
    ) -> Result<Vec<String>, VfsError>;
    async fn grep_as(
        &self,
        pattern: &str,
        path: Option<&str>,
        recursive: bool,
        session: &Session,
    ) -> Result<Vec<GrepResult>, VfsError>;

    async fn check_write_file_as(&self, path: &str, session: &Session) -> Result<(), VfsError>;
    async fn final_existing_write_path_as(
        &self,
        path: &str,
        session: &Session,
    ) -> Result<Option<String>, VfsError>;
    async fn check_set_metadata_as(&self, path: &str, session: &Session) -> Result<(), VfsError>;
    async fn write_file_as(
        &self,
        path: &str,
        content: Vec<u8>,
        session: &Session,
    ) -> Result<(), VfsError>;
    async fn set_metadata_as(
        &self,
        path: &str,
        update: MetadataUpdate,
        session: &Session,
    ) -> Result<MetadataUpdateResult, VfsError>;
    async fn check_mkdir_p_as(&self, path: &str, session: &Session) -> Result<(), VfsError>;
    async fn mkdir_p_as(&self, path: &str, session: &Session) -> Result<(), VfsError>;
    async fn check_rm_as(
        &self,
        path: &str,
        recursive: bool,
        session: &Session,
    ) -> Result<(), VfsError>;
    async fn rm_as(&self, path: &str, recursive: bool, session: &Session) -> Result<(), VfsError>;
    async fn check_cp_replay_as(
        &self,
        src: &str,
        dst: &str,
        session: &Session,
    ) -> Result<(), VfsError>;
    async fn check_mv_replay_as(
        &self,
        src: &str,
        dst: &str,
        session: &Session,
    ) -> Result<(), VfsError>;
    async fn check_cp_as(&self, src: &str, dst: &str, session: &Session) -> Result<(), VfsError>;
    async fn check_mv_as(&self, src: &str, dst: &str, session: &Session) -> Result<(), VfsError>;
    async fn cp_as(&self, src: &str, dst: &str, session: &Session) -> Result<(), VfsError>;
    async fn mv_as(&self, src: &str, dst: &str, session: &Session) -> Result<(), VfsError>;

    async fn resolve_commit_hash(&self, hash_prefix: &str) -> Result<String, VfsError>;
    async fn changed_paths_for_revert(&self, hash_prefix: &str) -> Result<Vec<String>, VfsError>;
    async fn list_refs(&self) -> Result<Vec<DbVcsRef>, VfsError>;
    async fn create_ref(&self, name: &str, target: &str) -> Result<DbVcsRef, VfsError>;
    async fn update_ref(
        &self,
        name: &str,
        expected_target: &str,
        expected_version: u64,
        target: &str,
    ) -> Result<DbVcsRef, VfsError>;
    fn guarded_durable_commit_route(&self) -> Option<GuardedDurableCommitRoute> {
        None
    }
    async fn commit_as(&self, message: &str, session: &Session) -> Result<String, VfsError>;
    async fn vcs_log_as(&self, session: &Session) -> Result<Vec<CommitObject>, VfsError>;
    async fn revert_as_with_path_check(
        &self,
        hash_prefix: &str,
        session: &Session,
        is_protected_path: ProtectedPathPredicate,
    ) -> Result<String, VfsError>;
    async fn vcs_status_as(&self, session: &Session) -> Result<String, VfsError>;
    async fn vcs_diff_as(&self, path: Option<&str>, session: &Session) -> Result<String, VfsError>;
}

#[derive(Clone)]
pub(crate) struct GuardedDurableCommitRoute {
    runtime: DurableCoreRuntime,
}

impl GuardedDurableCommitRoute {
    pub(crate) fn new(repo_id: RepoId, stores: StratumStores) -> Self {
        Self {
            runtime: DurableCoreRuntime::new(repo_id, stores),
        }
    }

    pub(crate) fn repo_id(&self) -> &RepoId {
        self.runtime.repo_id()
    }

    pub(crate) fn stores(&self) -> &StratumStores {
        &self.runtime.stores
    }

    pub(crate) async fn commit_metadata_preflight(
        &self,
    ) -> Result<DurableCoreCommitMetadataPreflight, VfsError> {
        self.runtime.commit_metadata_preflight().await
    }

    pub(crate) async fn list_refs(&self) -> Result<Vec<DbVcsRef>, VfsError> {
        self.runtime.durable_list_refs().await
    }

    pub(crate) async fn create_ref(&self, name: &str, target: &str) -> Result<DbVcsRef, VfsError> {
        self.runtime.durable_create_ref(name, target).await
    }

    pub(crate) async fn update_ref(
        &self,
        name: &str,
        expected_target: &str,
        expected_version: u64,
        target: &str,
    ) -> Result<DbVcsRef, VfsError> {
        self.runtime
            .durable_update_ref(name, expected_target, expected_version, target)
            .await
    }

    pub(crate) async fn vcs_log_as(
        &self,
        session: &Session,
    ) -> Result<Vec<CommitObject>, VfsError> {
        self.runtime.durable_vcs_log_as(session).await
    }

    pub(crate) async fn cat_with_stat_as(
        &self,
        path: &str,
        session: &Session,
    ) -> Result<(Vec<u8>, StatInfo), VfsError> {
        self.runtime.cat_with_stat_as(path, session).await
    }

    pub(crate) async fn ls_as(
        &self,
        path: Option<&str>,
        session: &Session,
    ) -> Result<Vec<LsEntry>, VfsError> {
        self.runtime.ls_as(path, session).await
    }

    pub(crate) async fn stat_as(
        &self,
        path: &str,
        session: &Session,
    ) -> Result<StatInfo, VfsError> {
        self.runtime.stat_as(path, session).await
    }

    pub(crate) async fn tree_as(
        &self,
        path: Option<&str>,
        session: &Session,
    ) -> Result<String, VfsError> {
        self.runtime.tree_as(path, session).await
    }

    pub(crate) async fn find_as(
        &self,
        path: Option<&str>,
        pattern: Option<&str>,
        session: &Session,
    ) -> Result<Vec<String>, VfsError> {
        self.runtime.find_as(path, pattern, session).await
    }

    pub(crate) async fn grep_as(
        &self,
        pattern: &str,
        path: Option<&str>,
        recursive: bool,
        session: &Session,
    ) -> Result<Vec<GrepResult>, VfsError> {
        self.runtime
            .grep_as(pattern, path, recursive, session)
            .await
    }

    pub(crate) fn mutable_workspace_not_supported(&self) -> VfsError {
        self.runtime.mutable_workspace_not_supported()
    }
}

#[derive(Clone)]
pub(crate) struct LocalCoreRuntime {
    db: Arc<StratumDb>,
    guarded_durable_commit_route: Option<GuardedDurableCommitRoute>,
}

impl LocalCoreRuntime {
    #[cfg(test)]
    pub(crate) fn new(db: StratumDb) -> Self {
        Self {
            db: Arc::new(db),
            guarded_durable_commit_route: None,
        }
    }

    pub(crate) fn from_shared(db: Arc<StratumDb>) -> Self {
        Self {
            db,
            guarded_durable_commit_route: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn shared(db: StratumDb) -> SharedCoreRuntime {
        Arc::new(Self::new(db))
    }

    pub(crate) fn shared_with_guarded_durable_commit_route(
        db: StratumDb,
        repo_id: RepoId,
        stores: StratumStores,
    ) -> SharedCoreRuntime {
        Arc::new(Self {
            db: Arc::new(db),
            guarded_durable_commit_route: Some(GuardedDurableCommitRoute::new(repo_id, stores)),
        })
    }

    pub(crate) fn shared_from_arc(db: Arc<StratumDb>) -> SharedCoreRuntime {
        Arc::new(Self::from_shared(db))
    }
}

#[derive(Clone)]
pub(crate) struct DurableCoreRuntime {
    repo_id: RepoId,
    stores: StratumStores,
}

impl DurableCoreRuntime {
    pub(crate) fn new(repo_id: RepoId, stores: StratumStores) -> Self {
        Self { repo_id, stores }
    }

    pub(crate) fn repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "durable core runtime is intentionally inspected only in tests until routed"
        )
    )]
    pub(crate) fn transaction_write_path(&self) -> &'static [DurableCoreTransactionStep] {
        DurableCoreStepSemantics::ordered_write_path()
    }

    pub(crate) fn commit_transaction_skeleton(&self) -> DurableCoreCommitExecutorSkeleton {
        DurableCoreCommitExecutorSkeleton::new()
    }

    pub(crate) async fn commit_metadata_preflight(
        &self,
    ) -> Result<DurableCoreCommitMetadataPreflight, VfsError> {
        let target_ref = Self::parse_durable_ref_name(MAIN_REF)?;
        let Some(current) = self.stores.refs.get(&self.repo_id, &target_ref).await? else {
            return Ok(DurableCoreCommitMetadataPreflight::for_main(
                DurableCoreCommitParentState::Unborn,
            ));
        };

        if self
            .stores
            .commits
            .contains(&self.repo_id, current.target)
            .await?
        {
            return Ok(DurableCoreCommitMetadataPreflight::for_main(
                DurableCoreCommitParentState::Existing {
                    target: current.target,
                    version: current.version,
                },
            ));
        }

        let still_current = self.stores.refs.get(&self.repo_id, &target_ref).await?;
        if !matches!(
            still_current.as_ref(),
            Some(record) if record.target == current.target && record.version == current.version
        ) {
            return Err(Self::durable_ref_cas_mismatch());
        }

        Err(Self::durable_missing_parent_metadata())
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "durable core runtime is intentionally fail-closed until routed"
        )
    )]
    pub(crate) fn route_execution_enabled(&self) -> bool {
        false
    }

    fn route_not_supported(&self) -> VfsError {
        let _ = (&self.repo_id, &self.stores);
        VfsError::NotSupported {
            message: DURABLE_CORE_ROUTE_NOT_SUPPORTED.to_string(),
        }
    }

    fn mutable_workspace_not_supported(&self) -> VfsError {
        VfsError::NotSupported {
            message: DURABLE_MUTABLE_WORKSPACE_NOT_SUPPORTED.to_string(),
        }
    }

    fn committed_reader(&self) -> DurableCommittedFsReader<'_> {
        DurableCommittedFsReader::new(
            &self.repo_id,
            self.stores.refs.as_ref(),
            self.stores.commits.as_ref(),
            self.stores.objects.as_ref(),
        )
    }

    fn parse_durable_ref_name(name: &str) -> Result<RefName, VfsError> {
        RefName::new(name).map_err(|_| VfsError::InvalidArgs {
            message: "invalid ref name".to_string(),
        })
    }

    fn parse_durable_commit_id(value: &str, label: &'static str) -> Result<CommitId, VfsError> {
        ObjectId::from_hex(value)
            .map(CommitId::from)
            .map_err(|_| VfsError::InvalidArgs {
                message: format!("invalid {label}"),
            })
    }

    fn durable_ref_cas_mismatch() -> VfsError {
        VfsError::InvalidArgs {
            message: "ref compare-and-swap mismatch".to_string(),
        }
    }

    fn durable_ref_already_exists() -> VfsError {
        VfsError::AlreadyExists {
            path: "ref".to_string(),
        }
    }

    fn durable_missing_parent_metadata() -> VfsError {
        VfsError::CorruptStore {
            message: "durable commit parent metadata is missing".to_string(),
        }
    }

    fn durable_metadata_store_unavailable() -> VfsError {
        VfsError::CorruptStore {
            message: "durable VCS metadata store unavailable".to_string(),
        }
    }

    fn sanitize_durable_metadata_store_error(_error: VfsError) -> VfsError {
        Self::durable_metadata_store_unavailable()
    }

    fn db_vcs_ref_from_record(record: RefRecord) -> DbVcsRef {
        DbVcsRef {
            name: record.name.into_string(),
            target: record.target.to_hex(),
            version: record.version.value(),
        }
    }

    fn sanitize_durable_ref_update_error(error: VfsError) -> VfsError {
        match error {
            VfsError::InvalidArgs { message }
                if message.starts_with("ref compare-and-swap mismatch") =>
            {
                Self::durable_ref_cas_mismatch()
            }
            _ => Self::durable_metadata_store_unavailable(),
        }
    }

    fn sanitize_durable_ref_create_error(error: VfsError) -> VfsError {
        match error {
            VfsError::InvalidArgs { message }
                if message.starts_with("ref compare-and-swap mismatch") =>
            {
                Self::durable_ref_already_exists()
            }
            _ => Self::durable_metadata_store_unavailable(),
        }
    }

    fn require_vcs_log_admin(session: &Session) -> Result<(), VfsError> {
        if session.scope.is_some() {
            return Err(VfsError::PermissionDenied {
                path: "admin operation".to_string(),
            });
        }

        let principal_admin = session.uid == ROOT_UID || session.groups.contains(&WHEEL_GID);
        if !principal_admin {
            return Err(VfsError::PermissionDenied {
                path: "admin operation".to_string(),
            });
        }

        if let Some(delegate) = &session.delegate {
            let delegate_admin = delegate.uid == ROOT_UID || delegate.groups.contains(&WHEEL_GID);
            if !delegate_admin {
                return Err(VfsError::PermissionDenied {
                    path: "admin operation".to_string(),
                });
            }
        }

        Ok(())
    }

    fn commit_object_from_record(record: CommitRecord) -> Result<CommitObject, VfsError> {
        let parent = record.parents.first().copied().map(CommitId::object_id);

        Ok(CommitObject {
            id: record.id.object_id(),
            tree: record.root_tree,
            parent,
            timestamp: record.timestamp,
            message: record.message,
            author: record.author,
            changed_paths: record.changed_paths,
        })
    }

    async fn durable_list_refs(&self) -> Result<Vec<DbVcsRef>, VfsError> {
        let refs = self
            .stores
            .refs
            .list(&self.repo_id)
            .await
            .map_err(Self::sanitize_durable_metadata_store_error)?;
        Ok(refs.into_iter().map(Self::db_vcs_ref_from_record).collect())
    }

    async fn durable_create_ref(&self, name: &str, target: &str) -> Result<DbVcsRef, VfsError> {
        let name = Self::parse_durable_ref_name(name)?;
        let target = Self::parse_durable_commit_id(target, "ref target commit id")?;

        if self
            .stores
            .refs
            .get(&self.repo_id, &name)
            .await
            .map_err(Self::sanitize_durable_metadata_store_error)?
            .is_some()
        {
            return Err(Self::durable_ref_already_exists());
        }

        if !self
            .stores
            .commits
            .contains(&self.repo_id, target)
            .await
            .map_err(Self::sanitize_durable_metadata_store_error)?
        {
            if self
                .stores
                .refs
                .get(&self.repo_id, &name)
                .await
                .map_err(Self::sanitize_durable_metadata_store_error)?
                .is_some()
            {
                return Err(Self::durable_ref_already_exists());
            }
            return Err(VfsError::ObjectNotFound {
                id: target.to_hex(),
            });
        }

        let created = self
            .stores
            .refs
            .update(RefUpdate {
                repo_id: self.repo_id.clone(),
                name,
                target,
                expectation: RefExpectation::MustNotExist,
            })
            .await
            .map_err(Self::sanitize_durable_ref_create_error)?;
        Ok(Self::db_vcs_ref_from_record(created))
    }

    async fn durable_update_ref(
        &self,
        name: &str,
        expected_target: &str,
        expected_version: u64,
        target: &str,
    ) -> Result<DbVcsRef, VfsError> {
        let name = Self::parse_durable_ref_name(name)?;
        let expected_target =
            Self::parse_durable_commit_id(expected_target, "expected ref target commit id")?;
        let target = Self::parse_durable_commit_id(target, "ref target commit id")?;

        let Some(current) = self
            .stores
            .refs
            .get(&self.repo_id, &name)
            .await
            .map_err(Self::sanitize_durable_metadata_store_error)?
        else {
            return Err(Self::durable_ref_cas_mismatch());
        };
        if current.target != expected_target || current.version.value() != expected_version {
            return Err(Self::durable_ref_cas_mismatch());
        }

        if !self
            .stores
            .commits
            .contains(&self.repo_id, target)
            .await
            .map_err(Self::sanitize_durable_metadata_store_error)?
        {
            let still_current = self
                .stores
                .refs
                .get(&self.repo_id, &name)
                .await
                .map_err(Self::sanitize_durable_metadata_store_error)?;
            if !matches!(
                still_current.as_ref(),
                Some(record) if record.target == expected_target && record.version == current.version
            ) {
                return Err(Self::durable_ref_cas_mismatch());
            }
            return Err(VfsError::ObjectNotFound {
                id: target.to_hex(),
            });
        }

        let updated = self
            .stores
            .refs
            .update(RefUpdate {
                repo_id: self.repo_id.clone(),
                name,
                target,
                expectation: RefExpectation::Matches {
                    target: expected_target,
                    version: current.version,
                },
            })
            .await
            .map_err(Self::sanitize_durable_ref_update_error)?;
        Ok(Self::db_vcs_ref_from_record(updated))
    }

    async fn durable_vcs_log_as(&self, session: &Session) -> Result<Vec<CommitObject>, VfsError> {
        Self::require_vcs_log_admin(session)?;
        let commits = self
            .stores
            .commits
            .list(&self.repo_id)
            .await
            .map_err(Self::sanitize_durable_metadata_store_error)?;
        commits
            .into_iter()
            .map(Self::commit_object_from_record)
            .collect()
    }
}

#[async_trait]
impl CoreDb for LocalCoreRuntime {
    async fn login(&self, username: &str) -> Result<Session, VfsError> {
        self.db.login(username).await
    }

    async fn authenticate_token(&self, raw_token: &str) -> Result<Session, VfsError> {
        self.db.authenticate_token(raw_token).await
    }

    async fn session_for_uid(&self, uid: Uid) -> Result<Session, VfsError> {
        self.db.session_for_uid(uid).await
    }

    async fn cat_with_stat_as(
        &self,
        path: &str,
        session: &Session,
    ) -> Result<(Vec<u8>, StatInfo), VfsError> {
        if let Some(capability) = &self.guarded_durable_commit_route {
            return capability.cat_with_stat_as(path, session).await;
        }
        self.db.cat_with_stat_as(path, session).await
    }

    async fn ls_as(&self, path: Option<&str>, session: &Session) -> Result<Vec<LsEntry>, VfsError> {
        if let Some(capability) = &self.guarded_durable_commit_route {
            return capability.ls_as(path, session).await;
        }
        self.db.ls_as(path, session).await
    }

    async fn stat_as(&self, path: &str, session: &Session) -> Result<StatInfo, VfsError> {
        if let Some(capability) = &self.guarded_durable_commit_route {
            return capability.stat_as(path, session).await;
        }
        self.db.stat_as(path, session).await
    }

    async fn tree_as(&self, path: Option<&str>, session: &Session) -> Result<String, VfsError> {
        if let Some(capability) = &self.guarded_durable_commit_route {
            return capability.tree_as(path, session).await;
        }
        self.db.tree_as(path, session).await
    }

    async fn find_as(
        &self,
        path: Option<&str>,
        pattern: Option<&str>,
        session: &Session,
    ) -> Result<Vec<String>, VfsError> {
        if let Some(capability) = &self.guarded_durable_commit_route {
            return capability.find_as(path, pattern, session).await;
        }
        self.db.find_as(path, pattern, session).await
    }

    async fn grep_as(
        &self,
        pattern: &str,
        path: Option<&str>,
        recursive: bool,
        session: &Session,
    ) -> Result<Vec<GrepResult>, VfsError> {
        if let Some(capability) = &self.guarded_durable_commit_route {
            return capability.grep_as(pattern, path, recursive, session).await;
        }
        self.db.grep_as(pattern, path, recursive, session).await
    }

    async fn check_write_file_as(&self, path: &str, session: &Session) -> Result<(), VfsError> {
        self.db.check_write_file_as(path, session).await
    }

    async fn final_existing_write_path_as(
        &self,
        path: &str,
        session: &Session,
    ) -> Result<Option<String>, VfsError> {
        self.db.final_existing_write_path_as(path, session).await
    }

    async fn check_set_metadata_as(&self, path: &str, session: &Session) -> Result<(), VfsError> {
        self.db.check_set_metadata_as(path, session).await
    }

    async fn write_file_as(
        &self,
        path: &str,
        content: Vec<u8>,
        session: &Session,
    ) -> Result<(), VfsError> {
        self.db.write_file_as(path, content, session).await
    }

    async fn set_metadata_as(
        &self,
        path: &str,
        update: MetadataUpdate,
        session: &Session,
    ) -> Result<MetadataUpdateResult, VfsError> {
        self.db.set_metadata_as(path, update, session).await
    }

    async fn check_mkdir_p_as(&self, path: &str, session: &Session) -> Result<(), VfsError> {
        self.db.check_mkdir_p_as(path, session).await
    }

    async fn mkdir_p_as(&self, path: &str, session: &Session) -> Result<(), VfsError> {
        self.db.mkdir_p_as(path, session).await
    }

    async fn check_rm_as(
        &self,
        path: &str,
        recursive: bool,
        session: &Session,
    ) -> Result<(), VfsError> {
        self.db.check_rm_as(path, recursive, session).await
    }

    async fn rm_as(&self, path: &str, recursive: bool, session: &Session) -> Result<(), VfsError> {
        self.db.rm_as(path, recursive, session).await
    }

    async fn check_cp_replay_as(
        &self,
        src: &str,
        dst: &str,
        session: &Session,
    ) -> Result<(), VfsError> {
        self.db.check_cp_replay_as(src, dst, session).await
    }

    async fn check_mv_replay_as(
        &self,
        src: &str,
        dst: &str,
        session: &Session,
    ) -> Result<(), VfsError> {
        self.db.check_mv_replay_as(src, dst, session).await
    }

    async fn check_cp_as(&self, src: &str, dst: &str, session: &Session) -> Result<(), VfsError> {
        self.db.check_cp_as(src, dst, session).await
    }

    async fn check_mv_as(&self, src: &str, dst: &str, session: &Session) -> Result<(), VfsError> {
        self.db.check_mv_as(src, dst, session).await
    }

    async fn cp_as(&self, src: &str, dst: &str, session: &Session) -> Result<(), VfsError> {
        self.db.cp_as(src, dst, session).await
    }

    async fn mv_as(&self, src: &str, dst: &str, session: &Session) -> Result<(), VfsError> {
        self.db.mv_as(src, dst, session).await
    }

    async fn resolve_commit_hash(&self, hash_prefix: &str) -> Result<String, VfsError> {
        self.db.resolve_commit_hash(hash_prefix).await
    }

    async fn changed_paths_for_revert(&self, hash_prefix: &str) -> Result<Vec<String>, VfsError> {
        self.db.changed_paths_for_revert(hash_prefix).await
    }

    async fn list_refs(&self) -> Result<Vec<DbVcsRef>, VfsError> {
        if let Some(capability) = &self.guarded_durable_commit_route {
            return capability.list_refs().await;
        }
        self.db.list_refs().await
    }

    async fn create_ref(&self, name: &str, target: &str) -> Result<DbVcsRef, VfsError> {
        self.db.create_ref(name, target).await
    }

    async fn update_ref(
        &self,
        name: &str,
        expected_target: &str,
        expected_version: u64,
        target: &str,
    ) -> Result<DbVcsRef, VfsError> {
        self.db
            .update_ref(name, expected_target, expected_version, target)
            .await
    }

    fn guarded_durable_commit_route(&self) -> Option<GuardedDurableCommitRoute> {
        self.guarded_durable_commit_route.clone()
    }

    async fn commit_as(&self, message: &str, session: &Session) -> Result<String, VfsError> {
        self.db.commit_as(message, session).await
    }

    async fn vcs_log_as(&self, session: &Session) -> Result<Vec<CommitObject>, VfsError> {
        if let Some(capability) = &self.guarded_durable_commit_route {
            return capability.vcs_log_as(session).await;
        }
        self.db.vcs_log_as(session).await
    }

    async fn revert_as_with_path_check(
        &self,
        hash_prefix: &str,
        session: &Session,
        is_protected_path: ProtectedPathPredicate,
    ) -> Result<String, VfsError> {
        if let Some(capability) = &self.guarded_durable_commit_route {
            let _ = (hash_prefix, session, is_protected_path);
            return Err(capability.mutable_workspace_not_supported());
        }
        self.db
            .revert_as_with_path_check(hash_prefix, session, move |path| is_protected_path(path))
            .await
    }

    async fn vcs_status_as(&self, session: &Session) -> Result<String, VfsError> {
        if let Some(capability) = &self.guarded_durable_commit_route {
            let _ = session;
            return Err(capability.mutable_workspace_not_supported());
        }
        self.db.vcs_status_as(session).await
    }

    async fn vcs_diff_as(&self, path: Option<&str>, session: &Session) -> Result<String, VfsError> {
        if let Some(capability) = &self.guarded_durable_commit_route {
            let _ = (path, session);
            return Err(capability.mutable_workspace_not_supported());
        }
        self.db.vcs_diff_as(path, session).await
    }
}

#[async_trait]
impl CoreDb for DurableCoreRuntime {
    async fn login(&self, _username: &str) -> Result<Session, VfsError> {
        Err(self.route_not_supported())
    }

    async fn authenticate_token(&self, _raw_token: &str) -> Result<Session, VfsError> {
        Err(self.route_not_supported())
    }

    async fn session_for_uid(&self, _uid: Uid) -> Result<Session, VfsError> {
        Err(self.route_not_supported())
    }

    async fn cat_with_stat_as(
        &self,
        path: &str,
        session: &Session,
    ) -> Result<(Vec<u8>, StatInfo), VfsError> {
        self.committed_reader()
            .cat_with_stat_as(path, session)
            .await
    }

    async fn ls_as(&self, path: Option<&str>, session: &Session) -> Result<Vec<LsEntry>, VfsError> {
        self.committed_reader().ls_as(path, session).await
    }

    async fn stat_as(&self, path: &str, session: &Session) -> Result<StatInfo, VfsError> {
        self.committed_reader().stat_as(path, session).await
    }

    async fn tree_as(&self, path: Option<&str>, session: &Session) -> Result<String, VfsError> {
        self.committed_reader().tree_as(path, session).await
    }

    async fn find_as(
        &self,
        path: Option<&str>,
        pattern: Option<&str>,
        session: &Session,
    ) -> Result<Vec<String>, VfsError> {
        self.committed_reader()
            .find_as(path, pattern, session)
            .await
    }

    async fn grep_as(
        &self,
        pattern: &str,
        path: Option<&str>,
        recursive: bool,
        session: &Session,
    ) -> Result<Vec<GrepResult>, VfsError> {
        self.committed_reader()
            .grep_as(pattern, path, recursive, session)
            .await
    }

    async fn check_write_file_as(&self, _path: &str, _session: &Session) -> Result<(), VfsError> {
        Err(self.route_not_supported())
    }

    async fn final_existing_write_path_as(
        &self,
        _path: &str,
        _session: &Session,
    ) -> Result<Option<String>, VfsError> {
        Err(self.route_not_supported())
    }

    async fn check_set_metadata_as(&self, _path: &str, _session: &Session) -> Result<(), VfsError> {
        Err(self.route_not_supported())
    }

    async fn write_file_as(
        &self,
        _path: &str,
        _content: Vec<u8>,
        _session: &Session,
    ) -> Result<(), VfsError> {
        Err(self.route_not_supported())
    }

    async fn set_metadata_as(
        &self,
        _path: &str,
        _update: MetadataUpdate,
        _session: &Session,
    ) -> Result<MetadataUpdateResult, VfsError> {
        Err(self.route_not_supported())
    }

    async fn check_mkdir_p_as(&self, _path: &str, _session: &Session) -> Result<(), VfsError> {
        Err(self.route_not_supported())
    }

    async fn mkdir_p_as(&self, _path: &str, _session: &Session) -> Result<(), VfsError> {
        Err(self.route_not_supported())
    }

    async fn check_rm_as(
        &self,
        _path: &str,
        _recursive: bool,
        _session: &Session,
    ) -> Result<(), VfsError> {
        Err(self.route_not_supported())
    }

    async fn rm_as(
        &self,
        _path: &str,
        _recursive: bool,
        _session: &Session,
    ) -> Result<(), VfsError> {
        Err(self.route_not_supported())
    }

    async fn check_cp_replay_as(
        &self,
        _src: &str,
        _dst: &str,
        _session: &Session,
    ) -> Result<(), VfsError> {
        Err(self.route_not_supported())
    }

    async fn check_mv_replay_as(
        &self,
        _src: &str,
        _dst: &str,
        _session: &Session,
    ) -> Result<(), VfsError> {
        Err(self.route_not_supported())
    }

    async fn check_cp_as(
        &self,
        _src: &str,
        _dst: &str,
        _session: &Session,
    ) -> Result<(), VfsError> {
        Err(self.route_not_supported())
    }

    async fn check_mv_as(
        &self,
        _src: &str,
        _dst: &str,
        _session: &Session,
    ) -> Result<(), VfsError> {
        Err(self.route_not_supported())
    }

    async fn cp_as(&self, _src: &str, _dst: &str, _session: &Session) -> Result<(), VfsError> {
        Err(self.route_not_supported())
    }

    async fn mv_as(&self, _src: &str, _dst: &str, _session: &Session) -> Result<(), VfsError> {
        Err(self.route_not_supported())
    }

    async fn resolve_commit_hash(&self, _hash_prefix: &str) -> Result<String, VfsError> {
        Err(self.route_not_supported())
    }

    async fn changed_paths_for_revert(&self, _hash_prefix: &str) -> Result<Vec<String>, VfsError> {
        Err(self.route_not_supported())
    }

    async fn list_refs(&self) -> Result<Vec<DbVcsRef>, VfsError> {
        self.durable_list_refs().await
    }

    async fn create_ref(&self, name: &str, target: &str) -> Result<DbVcsRef, VfsError> {
        self.durable_create_ref(name, target).await
    }

    async fn update_ref(
        &self,
        name: &str,
        expected_target: &str,
        expected_version: u64,
        target: &str,
    ) -> Result<DbVcsRef, VfsError> {
        self.durable_update_ref(name, expected_target, expected_version, target)
            .await
    }

    async fn commit_as(&self, _message: &str, _session: &Session) -> Result<String, VfsError> {
        let skeleton = self.commit_transaction_skeleton();
        if skeleton.live_execution_enabled() {
            return Err(skeleton.unsupported_live_execution_error());
        }
        Err(self.route_not_supported())
    }

    async fn vcs_log_as(&self, session: &Session) -> Result<Vec<CommitObject>, VfsError> {
        self.durable_vcs_log_as(session).await
    }

    async fn revert_as_with_path_check(
        &self,
        _hash_prefix: &str,
        _session: &Session,
        _is_protected_path: ProtectedPathPredicate,
    ) -> Result<String, VfsError> {
        Err(self.mutable_workspace_not_supported())
    }

    async fn vcs_status_as(&self, _session: &Session) -> Result<String, VfsError> {
        Err(self.mutable_workspace_not_supported())
    }

    async fn vcs_diff_as(
        &self,
        _path: Option<&str>,
        _session: &Session,
    ) -> Result<String, VfsError> {
        Err(self.mutable_workspace_not_supported())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::session::Session;
    use crate::backend::core_transaction::DurableCoreStepSemantics;
    use crate::backend::{
        CommitRecord, CommitStore, ObjectWrite, RefExpectation, RefStore, RefUpdate, RepoId,
        StratumStores,
    };
    use crate::store::tree::{TreeEntry, TreeEntryKind, TreeObject};
    use crate::store::{ObjectId, ObjectKind};
    use crate::vcs::{CommitId, MAIN_REF, RefName};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    mod durable_core_runtime {
        use super::*;

        fn commit_id(seed: &str) -> CommitId {
            CommitId::from(ObjectId::from_bytes(seed.as_bytes()))
        }

        fn commit_record(repo_id: &RepoId, id: CommitId, label: &str) -> CommitRecord {
            CommitRecord {
                repo_id: repo_id.clone(),
                id,
                root_tree: ObjectId::from_bytes(format!("root-tree-{label}").as_bytes()),
                parents: Vec::new(),
                timestamp: 1,
                message: format!("commit-{label}"),
                author: "agent".to_string(),
                changed_paths: Vec::new(),
            }
        }

        fn tree_entry(name: &str, kind: TreeEntryKind, id: ObjectId, mode: u16) -> TreeEntry {
            TreeEntry {
                name: name.to_string(),
                kind,
                id,
                mode,
                uid: ROOT_UID,
                gid: crate::auth::ROOT_GID,
                mime_type: None,
                custom_attrs: Default::default(),
            }
        }

        async fn put_object(
            stores: &StratumStores,
            repo_id: &RepoId,
            kind: ObjectKind,
            bytes: Vec<u8>,
        ) -> ObjectId {
            let id = ObjectId::from_bytes(&bytes);
            stores
                .objects
                .put(ObjectWrite {
                    repo_id: repo_id.clone(),
                    id,
                    kind,
                    bytes,
                })
                .await
                .unwrap();
            id
        }

        async fn seed_committed_tree(
            stores: &StratumStores,
            repo_id: &RepoId,
            root_tree: ObjectId,
            label: &str,
        ) -> CommitId {
            let commit_id = commit_id(label);
            stores
                .commits
                .insert(CommitRecord {
                    repo_id: repo_id.clone(),
                    id: commit_id,
                    root_tree,
                    parents: Vec::new(),
                    timestamp: 1_725_000_001,
                    message: format!("durable {label}"),
                    author: "root".to_string(),
                    changed_paths: Vec::new(),
                })
                .await
                .unwrap();
            stores
                .refs
                .update(RefUpdate {
                    repo_id: repo_id.clone(),
                    name: RefName::new(MAIN_REF).unwrap(),
                    target: commit_id,
                    expectation: RefExpectation::MustNotExist,
                })
                .await
                .unwrap();
            commit_id
        }

        fn assert_message_omits(message: &str, forbidden_values: &[&str]) {
            for forbidden in forbidden_values {
                assert!(
                    !message.contains(forbidden),
                    "durable update-ref error leaked sensitive input {forbidden:?}: {message}"
                );
            }
        }

        fn assert_duplicate_ref_error_redacted(error: VfsError, forbidden_values: &[&str]) {
            let rendered = error.to_string();
            let VfsError::AlreadyExists { path } = error else {
                panic!("duplicate ref should return AlreadyExists");
            };
            assert_eq!(path, "ref");
            assert_message_omits(&path, forbidden_values);
            assert_message_omits(&rendered, forbidden_values);
        }

        fn leaky_metadata_store_error() -> VfsError {
            VfsError::CorruptStore {
                message: "postgres://secret@metadata.example/ref-store failed".to_string(),
            }
        }

        fn assert_metadata_store_error_redacted(error: VfsError) {
            let rendered = error.to_string();
            let VfsError::CorruptStore { message } = error else {
                panic!("store failure should return CorruptStore");
            };
            assert_eq!(message, "durable VCS metadata store unavailable");
            assert_message_omits(&rendered, &["postgres://secret", "metadata.example"]);
        }

        struct LeakyCommitStore;

        #[async_trait]
        impl CommitStore for LeakyCommitStore {
            async fn insert(&self, _record: CommitRecord) -> Result<CommitRecord, VfsError> {
                Err(leaky_metadata_store_error())
            }

            async fn get(
                &self,
                _repo_id: &RepoId,
                _id: CommitId,
            ) -> Result<Option<CommitRecord>, VfsError> {
                Err(leaky_metadata_store_error())
            }

            async fn contains(&self, _repo_id: &RepoId, _id: CommitId) -> Result<bool, VfsError> {
                Err(leaky_metadata_store_error())
            }

            async fn list(&self, _repo_id: &RepoId) -> Result<Vec<CommitRecord>, VfsError> {
                Err(leaky_metadata_store_error())
            }
        }

        struct LeakyRefStore;

        #[async_trait]
        impl RefStore for LeakyRefStore {
            async fn list(
                &self,
                _repo_id: &RepoId,
            ) -> Result<Vec<crate::backend::RefRecord>, VfsError> {
                Err(leaky_metadata_store_error())
            }

            async fn get(
                &self,
                _repo_id: &RepoId,
                _name: &RefName,
            ) -> Result<Option<crate::backend::RefRecord>, VfsError> {
                Err(leaky_metadata_store_error())
            }

            async fn update(
                &self,
                _update: RefUpdate,
            ) -> Result<crate::backend::RefRecord, VfsError> {
                Err(leaky_metadata_store_error())
            }

            async fn update_source_checked(
                &self,
                _update: crate::backend::SourceCheckedRefUpdate,
            ) -> Result<crate::backend::RefRecord, VfsError> {
                Err(leaky_metadata_store_error())
            }
        }

        struct LeakyUpdateRefStore {
            current: crate::backend::RefRecord,
        }

        #[async_trait]
        impl RefStore for LeakyUpdateRefStore {
            async fn list(
                &self,
                _repo_id: &RepoId,
            ) -> Result<Vec<crate::backend::RefRecord>, VfsError> {
                Ok(vec![self.current.clone()])
            }

            async fn get(
                &self,
                _repo_id: &RepoId,
                _name: &RefName,
            ) -> Result<Option<crate::backend::RefRecord>, VfsError> {
                Ok(Some(self.current.clone()))
            }

            async fn update(
                &self,
                _update: RefUpdate,
            ) -> Result<crate::backend::RefRecord, VfsError> {
                Err(leaky_metadata_store_error())
            }

            async fn update_source_checked(
                &self,
                _update: crate::backend::SourceCheckedRefUpdate,
            ) -> Result<crate::backend::RefRecord, VfsError> {
                Err(leaky_metadata_store_error())
            }
        }

        struct RefMutatingCommitStore {
            inner: Arc<dyn CommitStore>,
            refs: Arc<dyn RefStore>,
            repo_id: RepoId,
            name: RefName,
            expected_target: CommitId,
            expected_version: crate::backend::RefVersion,
            racing_target: CommitId,
            missing_target: CommitId,
            fired: AtomicBool,
        }

        #[async_trait]
        impl CommitStore for RefMutatingCommitStore {
            async fn insert(&self, record: CommitRecord) -> Result<CommitRecord, VfsError> {
                self.inner.insert(record).await
            }

            async fn get(
                &self,
                repo_id: &RepoId,
                id: CommitId,
            ) -> Result<Option<CommitRecord>, VfsError> {
                self.inner.get(repo_id, id).await
            }

            async fn contains(&self, repo_id: &RepoId, id: CommitId) -> Result<bool, VfsError> {
                if repo_id == &self.repo_id
                    && id == self.missing_target
                    && !self.fired.swap(true, Ordering::SeqCst)
                {
                    self.refs
                        .update(RefUpdate {
                            repo_id: self.repo_id.clone(),
                            name: self.name.clone(),
                            target: self.racing_target,
                            expectation: RefExpectation::Matches {
                                target: self.expected_target,
                                version: self.expected_version,
                            },
                        })
                        .await?;
                }
                self.inner.contains(repo_id, id).await
            }

            async fn list(&self, repo_id: &RepoId) -> Result<Vec<CommitRecord>, VfsError> {
                self.inner.list(repo_id).await
            }
        }

        struct CreateRefMutatingCommitStore {
            inner: Arc<dyn CommitStore>,
            refs: Arc<dyn RefStore>,
            repo_id: RepoId,
            name: RefName,
            racing_target: CommitId,
            missing_target: CommitId,
            fired: AtomicBool,
        }

        #[async_trait]
        impl CommitStore for CreateRefMutatingCommitStore {
            async fn insert(&self, record: CommitRecord) -> Result<CommitRecord, VfsError> {
                self.inner.insert(record).await
            }

            async fn get(
                &self,
                repo_id: &RepoId,
                id: CommitId,
            ) -> Result<Option<CommitRecord>, VfsError> {
                self.inner.get(repo_id, id).await
            }

            async fn contains(&self, repo_id: &RepoId, id: CommitId) -> Result<bool, VfsError> {
                if repo_id == &self.repo_id
                    && id == self.missing_target
                    && !self.fired.swap(true, Ordering::SeqCst)
                {
                    self.refs
                        .update(RefUpdate {
                            repo_id: self.repo_id.clone(),
                            name: self.name.clone(),
                            target: self.racing_target,
                            expectation: RefExpectation::MustNotExist,
                        })
                        .await?;
                }
                self.inner.contains(repo_id, id).await
            }

            async fn list(&self, repo_id: &RepoId) -> Result<Vec<CommitRecord>, VfsError> {
                self.inner.list(repo_id).await
            }
        }

        #[test]
        fn reports_contract_without_local_state() {
            let repo_id = RepoId::local();
            let runtime = DurableCoreRuntime::new(repo_id.clone(), StratumStores::local_memory());
            let skeleton = runtime.commit_transaction_skeleton();

            assert_eq!(runtime.repo_id(), &repo_id);
            assert_eq!(
                runtime.transaction_write_path(),
                DurableCoreStepSemantics::ordered_write_path()
            );
            assert!(!runtime.route_execution_enabled());
            assert_eq!(
                skeleton.ordered_write_path(),
                DurableCoreStepSemantics::ordered_write_path()
            );
            assert!(!skeleton.live_execution_enabled());
            assert!(skeleton.unresolved_prerequisites().contains(
                &crate::backend::core_transaction::DurableCoreCommitPrerequisite::RepairWorker
            ));
        }

        #[tokio::test]
        async fn route_methods_fail_closed() {
            let runtime = DurableCoreRuntime::new(RepoId::local(), StratumStores::local_memory());
            let session = Session::root();
            let request_path = "/tenant/alice/private-token";
            let username = "alice-private-token";
            let raw_token = "workspace-secret-token";
            let request_body = b"file body secret".to_vec();
            let commit_message = "commit message private-token";

            for err in [
                runtime
                    .login(username)
                    .await
                    .expect_err("login should fail closed"),
                runtime
                    .authenticate_token(raw_token)
                    .await
                    .expect_err("authenticate_token should fail closed"),
                runtime
                    .write_file_as(request_path, request_body, &session)
                    .await
                    .expect_err("write_file should fail closed"),
                runtime
                    .commit_as(commit_message, &session)
                    .await
                    .expect_err("commit should fail closed"),
            ] {
                let VfsError::NotSupported { message } = err else {
                    panic!("durable core route should return NotSupported");
                };
                assert!(message.contains("durable core runtime"));
                assert!(message.contains("route execution"));
                for forbidden in [
                    request_path,
                    username,
                    raw_token,
                    "file body secret",
                    commit_message,
                    "alice",
                    "private-token",
                    "durable-cloud",
                    "STRATUM_CORE_RUNTIME",
                ] {
                    assert!(
                        !message.contains(forbidden),
                        "durable core error leaked sensitive input {forbidden:?}: {message}"
                    );
                }
            }
        }

        #[tokio::test]
        async fn durable_core_runtime_reads_committed_tree_without_local_state() {
            let repo_id = RepoId::local();
            let stores = StratumStores::local_memory();
            let note_bytes = b"durable route\nTODO source of truth\n".to_vec();
            let note_id = put_object(&stores, &repo_id, ObjectKind::Blob, note_bytes.clone()).await;
            let nested_id = put_object(
                &stores,
                &repo_id,
                ObjectKind::Blob,
                b"nested durable route".to_vec(),
            )
            .await;
            let nested_tree_id = put_object(
                &stores,
                &repo_id,
                ObjectKind::Tree,
                TreeObject {
                    entries: vec![tree_entry(
                        "nested.txt",
                        TreeEntryKind::Blob,
                        nested_id,
                        0o644,
                    )],
                }
                .serialize(),
            )
            .await;
            let root_tree_id = put_object(
                &stores,
                &repo_id,
                ObjectKind::Tree,
                TreeObject {
                    entries: vec![
                        tree_entry("docs", TreeEntryKind::Tree, nested_tree_id, 0o755),
                        tree_entry("notes.txt", TreeEntryKind::Blob, note_id, 0o644),
                    ],
                }
                .serialize(),
            )
            .await;
            let commit_id =
                seed_committed_tree(&stores, &repo_id, root_tree_id, "runtime committed").await;
            let runtime = DurableCoreRuntime::new(repo_id.clone(), stores.clone());
            let session = Session::root();

            let (content, stat) = runtime
                .cat_with_stat_as("/notes.txt", &session)
                .await
                .unwrap();
            assert_eq!(content, note_bytes);
            assert_eq!(
                stat.content_hash,
                Some(format!("sha256:{}", note_id.to_hex()))
            );

            let entries = runtime.ls_as(Some("/"), &session).await.unwrap();
            assert_eq!(
                entries
                    .iter()
                    .map(|entry| entry.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["docs", "notes.txt"]
            );
            assert_eq!(
                runtime.stat_as("/docs", &session).await.unwrap().kind,
                "directory"
            );
            assert_eq!(
                runtime.tree_as(Some("/"), &session).await.unwrap(),
                ".\n\u{251c}\u{2500}\u{2500} docs/\n\u{2502}   \u{2514}\u{2500}\u{2500} nested.txt\n\u{2514}\u{2500}\u{2500} notes.txt\n"
            );
            assert_eq!(
                runtime
                    .find_as(Some("/"), Some("*.txt"), &session)
                    .await
                    .unwrap(),
                vec!["/docs/nested.txt", "/notes.txt"]
            );
            let grep = runtime
                .grep_as("TODO", Some("/"), true, &session)
                .await
                .unwrap();
            assert_eq!(grep.len(), 1);
            assert_eq!(grep[0].file, "/notes.txt");

            let refs = runtime.list_refs().await.unwrap();
            assert_eq!(refs.len(), 1);
            assert_eq!(refs[0].target, commit_id.to_hex());
            let log = runtime.vcs_log_as(&session).await.unwrap();
            assert_eq!(log.len(), 1);
            assert_eq!(log[0].id, commit_id.object_id());

            let guarded = LocalCoreRuntime::shared_with_guarded_durable_commit_route(
                StratumDb::open_memory(),
                repo_id,
                stores,
            );
            let (guarded_content, _) = guarded
                .cat_with_stat_as("/notes.txt", &session)
                .await
                .unwrap();
            assert_eq!(guarded_content, content);
        }

        #[tokio::test]
        async fn durable_mutable_workspace_routes_fail_closed_without_request_leaks() {
            let runtime = DurableCoreRuntime::new(RepoId::local(), StratumStores::local_memory());
            let session = Session::root();
            let request_path = "/tenant/alice/private-token";

            for err in [
                runtime
                    .revert_as_with_path_check("abc123private", &session, Arc::new(|_path| false))
                    .await
                    .expect_err("revert should fail closed"),
                runtime
                    .vcs_status_as(&session)
                    .await
                    .expect_err("status should fail closed"),
                runtime
                    .vcs_diff_as(Some(request_path), &session)
                    .await
                    .expect_err("diff should fail closed"),
            ] {
                let rendered = err.to_string();
                let VfsError::NotSupported { message } = err else {
                    panic!("durable mutable workspace routes should return NotSupported");
                };
                assert_eq!(message, DURABLE_MUTABLE_WORKSPACE_NOT_SUPPORTED);
                for forbidden in [
                    request_path,
                    "abc123private",
                    "alice",
                    "private-token",
                    "STRATUM_CORE_RUNTIME",
                ] {
                    assert!(
                        !rendered.contains(forbidden),
                        "durable mutable workspace error leaked {forbidden:?}: {rendered}"
                    );
                }
            }
        }

        #[tokio::test]
        async fn durable_vcs_log_maps_multi_parent_metadata_using_first_parent() {
            let repo_id = RepoId::local();
            let stores = StratumStores::local_memory();
            let first_parent = commit_id("first-parent");
            let second_parent = commit_id("second-parent");
            let merge_id = commit_id("merge-commit");
            let mut merge_record = commit_record(&repo_id, merge_id, "merge");
            merge_record.parents = vec![first_parent, second_parent];
            stores.commits.insert(merge_record).await.unwrap();
            let runtime = DurableCoreRuntime::new(repo_id, stores);

            let commits = runtime.durable_vcs_log_as(&Session::root()).await.unwrap();

            assert_eq!(commits.len(), 1);
            assert_eq!(commits[0].id, merge_id.object_id());
            assert_eq!(commits[0].parent, Some(first_parent.object_id()));
        }

        #[tokio::test]
        async fn durable_vcs_log_redacts_commit_store_list_errors() {
            let repo_id = RepoId::local();
            let mut stores = StratumStores::local_memory();
            stores.commits = Arc::new(LeakyCommitStore);
            let runtime = DurableCoreRuntime::new(repo_id, stores);

            let error = runtime
                .durable_vcs_log_as(&Session::root())
                .await
                .expect_err("leaky commit list error should fail");

            assert_metadata_store_error_redacted(error);
        }

        #[tokio::test]
        async fn durable_list_refs_redacts_ref_store_list_errors() {
            let repo_id = RepoId::local();
            let mut stores = StratumStores::local_memory();
            stores.refs = Arc::new(LeakyRefStore);
            let runtime = DurableCoreRuntime::new(repo_id, stores);

            let error = runtime
                .durable_list_refs()
                .await
                .expect_err("leaky ref list error should fail");

            assert_metadata_store_error_redacted(error);
        }

        #[tokio::test]
        async fn durable_create_ref_redacts_commit_store_contains_errors() {
            let repo_id = RepoId::local();
            let mut stores = StratumStores::local_memory();
            stores.commits = Arc::new(LeakyCommitStore);
            let runtime = DurableCoreRuntime::new(repo_id, stores);

            let error = runtime
                .durable_create_ref("main", &commit_id("target").to_hex())
                .await
                .expect_err("leaky commit contains error should fail");

            assert_metadata_store_error_redacted(error);
        }

        #[tokio::test]
        async fn durable_update_ref_redacts_ref_store_update_errors() {
            let repo_id = RepoId::local();
            let mut stores = StratumStores::local_memory();
            let current_target = commit_id("current");
            let next_target = commit_id("next");
            let main = RefName::new(MAIN_REF).unwrap();
            CommitStore::insert(
                &*stores.commits,
                commit_record(&repo_id, next_target, "next"),
            )
            .await
            .unwrap();
            stores.refs = Arc::new(LeakyUpdateRefStore {
                current: crate::backend::RefRecord {
                    repo_id: repo_id.clone(),
                    name: main,
                    target: current_target,
                    version: crate::backend::RefVersion::new(1).unwrap(),
                },
            });
            let runtime = DurableCoreRuntime::new(repo_id, stores);

            let error = runtime
                .durable_update_ref(MAIN_REF, &current_target.to_hex(), 1, &next_target.to_hex())
                .await
                .expect_err("leaky ref update error should fail");

            assert_metadata_store_error_redacted(error);
        }

        #[tokio::test]
        async fn commit_as_remains_fail_closed_redacted_and_non_mutating() {
            let repo_id = RepoId::local();
            let stores = StratumStores::local_memory();
            let runtime = DurableCoreRuntime::new(repo_id.clone(), stores.clone());
            let session = Session::new(
                1000,
                1000,
                vec![1000],
                "alice-session-private-token".to_string(),
            );
            let commit_message = "commit message with workspace-secret private-token";

            let error = runtime
                .commit_as(commit_message, &session)
                .await
                .expect_err("durable commit execution should fail closed");
            let rendered = error.to_string();
            let VfsError::NotSupported { message } = error else {
                panic!("durable commit execution should return NotSupported");
            };

            assert_eq!(message, DURABLE_CORE_ROUTE_NOT_SUPPORTED);
            for forbidden in [
                commit_message,
                session.username.as_str(),
                "alice",
                "private-token",
                "workspace-secret",
                "STRATUM_CORE_RUNTIME",
                "durable-cloud",
            ] {
                assert!(
                    !message.contains(forbidden) && !rendered.contains(forbidden),
                    "durable commit error leaked sensitive input {forbidden:?}: {rendered}"
                );
            }
            assert!(
                CommitStore::list(&*stores.commits, &repo_id)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                RefStore::list(&*stores.refs, &repo_id)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }

        #[tokio::test]
        async fn metadata_preflight_returns_unborn_main_without_mutation() {
            let repo_id = RepoId::local();
            let stores = StratumStores::local_memory();
            let runtime = DurableCoreRuntime::new(repo_id.clone(), stores.clone());

            let preflight = runtime
                .commit_metadata_preflight()
                .await
                .expect("unborn main should preflight");

            assert_eq!(preflight.target_ref(), MAIN_REF);
            assert_eq!(
                preflight.parent_state(),
                DurableCoreCommitParentState::Unborn
            );
            assert_eq!(
                preflight.ordered_write_path(),
                DurableCoreStepSemantics::ordered_write_path()
            );
            assert!(!preflight.live_execution_enabled());
            assert!(
                CommitStore::list(&*stores.commits, &repo_id)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                RefStore::list(&*stores.refs, &repo_id)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }

        #[tokio::test]
        async fn metadata_preflight_returns_existing_parent_when_commit_metadata_exists() {
            let repo_id = RepoId::local();
            let stores = StratumStores::local_memory();
            let runtime = DurableCoreRuntime::new(repo_id.clone(), stores.clone());
            let main = RefName::new(MAIN_REF).unwrap();
            let target = commit_id("target");

            CommitStore::insert(&*stores.commits, commit_record(&repo_id, target, "target"))
                .await
                .unwrap();
            let current = RefStore::update(
                &*stores.refs,
                RefUpdate {
                    repo_id: repo_id.clone(),
                    name: main.clone(),
                    target,
                    expectation: RefExpectation::MustNotExist,
                },
            )
            .await
            .unwrap();

            let preflight = runtime
                .commit_metadata_preflight()
                .await
                .expect("existing parent should preflight");

            assert_eq!(
                preflight.parent_state(),
                DurableCoreCommitParentState::Existing {
                    target,
                    version: current.version
                }
            );
            assert_eq!(
                RefStore::get(&*stores.refs, &repo_id, &main)
                    .await
                    .unwrap()
                    .unwrap(),
                current
            );
            assert_eq!(
                CommitStore::list(&*stores.commits, &repo_id)
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }

        #[tokio::test]
        async fn metadata_preflight_reports_missing_parent_as_redacted_corrupt_store() {
            let repo_id = RepoId::local();
            let stores = StratumStores::local_memory();
            let runtime = DurableCoreRuntime::new(repo_id.clone(), stores.clone());
            let main = RefName::new(MAIN_REF).unwrap();
            let missing_target = commit_id("missing-parent-private-token");

            let current = RefStore::update(
                &*stores.refs,
                RefUpdate {
                    repo_id: repo_id.clone(),
                    name: main.clone(),
                    target: missing_target,
                    expectation: RefExpectation::MustNotExist,
                },
            )
            .await
            .unwrap();

            let error = runtime
                .commit_metadata_preflight()
                .await
                .expect_err("missing parent metadata should fail");
            let rendered = error.to_string();
            let VfsError::CorruptStore { message } = error else {
                panic!("missing parent metadata should return CorruptStore");
            };
            assert_eq!(message, "durable commit parent metadata is missing");
            assert_message_omits(
                &rendered,
                &[
                    &missing_target.to_hex(),
                    "missing-parent-private-token",
                    "private-token",
                    "workspace-secret",
                    "STRATUM_CORE_RUNTIME",
                    "durable-cloud",
                ],
            );

            assert_eq!(
                RefStore::get(&*stores.refs, &repo_id, &main)
                    .await
                    .unwrap()
                    .unwrap(),
                current
            );
            assert!(
                CommitStore::list(&*stores.commits, &repo_id)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }

        #[tokio::test]
        async fn metadata_preflight_rechecks_ref_before_missing_parent_error() {
            let repo_id = RepoId::local();
            let mut stores = StratumStores::local_memory();
            let inner_commits = stores.commits.clone();
            let refs = stores.refs.clone();
            let main = RefName::new(MAIN_REF).unwrap();
            let missing_target = commit_id("missing-parent-private-token");
            let racing_target = commit_id("racing-target");

            CommitStore::insert(
                &*inner_commits,
                commit_record(&repo_id, racing_target, "racing-target"),
            )
            .await
            .unwrap();
            let current = RefStore::update(
                &*refs,
                RefUpdate {
                    repo_id: repo_id.clone(),
                    name: main.clone(),
                    target: missing_target,
                    expectation: RefExpectation::MustNotExist,
                },
            )
            .await
            .unwrap();

            stores.commits = Arc::new(RefMutatingCommitStore {
                inner: inner_commits,
                refs: refs.clone(),
                repo_id: repo_id.clone(),
                name: main.clone(),
                expected_target: missing_target,
                expected_version: current.version,
                racing_target,
                missing_target,
                fired: AtomicBool::new(false),
            });
            let runtime = DurableCoreRuntime::new(repo_id.clone(), stores.clone());

            let error = runtime
                .commit_metadata_preflight()
                .await
                .expect_err("raced missing parent should surface as stale CAS");
            let rendered = error.to_string();
            let VfsError::InvalidArgs { message } = error else {
                panic!("raced missing parent should return InvalidArgs");
            };
            assert_eq!(message, "ref compare-and-swap mismatch");
            assert_message_omits(
                &rendered,
                &[
                    &missing_target.to_hex(),
                    "missing-parent-private-token",
                    "private-token",
                    "workspace-secret",
                    "STRATUM_CORE_RUNTIME",
                    "durable-cloud",
                ],
            );

            let loaded = RefStore::get(&*stores.refs, &repo_id, &main)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(loaded.target, racing_target);
            assert_eq!(loaded.version.value(), current.version.value() + 1);
            assert_eq!(
                CommitStore::list(&*stores.commits, &repo_id)
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }

        #[tokio::test]
        async fn create_ref_rejects_invalid_target_without_leaking_raw_value() {
            let runtime = DurableCoreRuntime::new(RepoId::local(), StratumStores::local_memory());
            let raw_target = "target-private-token";

            let error = runtime
                .create_ref("agent/alice/session-1", raw_target)
                .await
                .expect_err("invalid target should fail");

            let VfsError::InvalidArgs { message } = error else {
                panic!("invalid target should return InvalidArgs");
            };
            assert_eq!(message, "invalid ref target commit id");
            assert_message_omits(
                &message,
                &[
                    raw_target,
                    "private-token",
                    "STRATUM_CORE_RUNTIME",
                    "durable-cloud",
                ],
            );
        }

        #[tokio::test]
        async fn create_ref_rejects_invalid_ref_name_without_leaking_raw_value() {
            let runtime = DurableCoreRuntime::new(RepoId::local(), StratumStores::local_memory());
            let target = commit_id("target").to_hex();
            let raw_ref_name = "agent/alice/private-token/extra";

            let error = runtime
                .create_ref(raw_ref_name, &target)
                .await
                .expect_err("invalid ref name should fail");

            let VfsError::InvalidArgs { message } = error else {
                panic!("invalid ref name should return InvalidArgs");
            };
            assert_eq!(message, "invalid ref name");
            assert_message_omits(
                &message,
                &[
                    raw_ref_name,
                    "alice",
                    "private-token",
                    "STRATUM_CORE_RUNTIME",
                    "durable-cloud",
                ],
            );
        }

        #[tokio::test]
        async fn create_ref_rejects_duplicate_ref_without_mutation_or_raw_name() {
            let repo_id = RepoId::local();
            let stores = StratumStores::local_memory();
            let runtime = DurableCoreRuntime::new(repo_id.clone(), stores.clone());
            let name = RefName::new("agent/alice/session-1").unwrap();
            let current_target = commit_id("current-target");
            let missing_target = commit_id("missing-target");

            CommitStore::insert(
                &*stores.commits,
                commit_record(&repo_id, current_target, "current-target"),
            )
            .await
            .unwrap();

            let current = RefStore::update(
                &*stores.refs,
                RefUpdate {
                    repo_id: repo_id.clone(),
                    name: name.clone(),
                    target: current_target,
                    expectation: RefExpectation::MustNotExist,
                },
            )
            .await
            .unwrap();

            let error = runtime
                .create_ref(name.as_str(), &missing_target.to_hex())
                .await
                .expect_err("duplicate ref should fail");
            assert_duplicate_ref_error_redacted(
                error,
                &[
                    name.as_str(),
                    "alice",
                    "private-token",
                    "STRATUM_CORE_RUNTIME",
                    "durable-cloud",
                ],
            );

            let loaded = RefStore::get(&*stores.refs, &repo_id, &name)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(loaded.target, current_target);
            assert_eq!(loaded.version, current.version);
        }

        #[tokio::test]
        async fn create_ref_rejects_missing_target_without_mutation() {
            let repo_id = RepoId::local();
            let stores = StratumStores::local_memory();
            let runtime = DurableCoreRuntime::new(repo_id.clone(), stores.clone());
            let name = RefName::new("agent/alice/session-1").unwrap();
            let missing_target = commit_id("missing-target");

            let error = runtime
                .create_ref(name.as_str(), &missing_target.to_hex())
                .await
                .expect_err("missing target should fail");
            let VfsError::ObjectNotFound { id } = error else {
                panic!("missing target should return ObjectNotFound");
            };
            assert_eq!(id, missing_target.to_hex());

            let loaded = RefStore::get(&*stores.refs, &repo_id, &name).await.unwrap();
            assert!(loaded.is_none(), "missing target must not create ref");
        }

        #[tokio::test]
        async fn create_ref_rechecks_duplicate_before_missing_target_error() {
            let repo_id = RepoId::local();
            let mut stores = StratumStores::local_memory();
            let inner_commits = stores.commits.clone();
            let refs = stores.refs.clone();
            let name = RefName::new("agent/alice/session-1").unwrap();
            let racing_target = commit_id("racing-target");
            let missing_target = commit_id("missing-target");

            CommitStore::insert(
                &*inner_commits,
                commit_record(&repo_id, racing_target, "racing-target"),
            )
            .await
            .unwrap();

            stores.commits = Arc::new(CreateRefMutatingCommitStore {
                inner: inner_commits,
                refs: refs.clone(),
                repo_id: repo_id.clone(),
                name: name.clone(),
                racing_target,
                missing_target,
                fired: AtomicBool::new(false),
            });
            let runtime = DurableCoreRuntime::new(repo_id.clone(), stores.clone());

            let error = runtime
                .create_ref(name.as_str(), &missing_target.to_hex())
                .await
                .expect_err("raced duplicate should fail as duplicate");
            assert_duplicate_ref_error_redacted(
                error,
                &[
                    name.as_str(),
                    "alice",
                    "private-token",
                    "STRATUM_CORE_RUNTIME",
                    "durable-cloud",
                ],
            );

            let loaded = RefStore::get(&*stores.refs, &repo_id, &name)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(loaded.target, racing_target);
            assert_eq!(loaded.version.value(), 1);
        }

        #[tokio::test]
        async fn create_ref_creates_ref_for_existing_commit() {
            let repo_id = RepoId::local();
            let stores = StratumStores::local_memory();
            let runtime = DurableCoreRuntime::new(repo_id.clone(), stores.clone());
            let name = RefName::new("agent/alice/session-1").unwrap();
            let target = commit_id("target");

            CommitStore::insert(&*stores.commits, commit_record(&repo_id, target, "target"))
                .await
                .unwrap();

            let created = runtime
                .create_ref(name.as_str(), &target.to_hex())
                .await
                .expect("create_ref should succeed");
            assert_eq!(created.name, name.as_str());
            assert_eq!(created.target, target.to_hex());
            assert_eq!(created.version, 1);

            let loaded = RefStore::get(&*stores.refs, &repo_id, &name)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(loaded.target, target);
            assert_eq!(loaded.version.value(), 1);
        }

        #[tokio::test]
        async fn update_ref_rejects_invalid_target_without_leaking_raw_value() {
            let runtime = DurableCoreRuntime::new(RepoId::local(), StratumStores::local_memory());
            let expected_target = commit_id("expected-target").to_hex();
            let raw_target = "not-a-hex-target-private-token";

            let error = runtime
                .update_ref("agent/alice/session-1", &expected_target, 1, raw_target)
                .await
                .expect_err("invalid target should fail");

            let VfsError::InvalidArgs { message } = error else {
                panic!("invalid target should return InvalidArgs");
            };
            assert_eq!(message, "invalid ref target commit id");
            assert_message_omits(
                &message,
                &[
                    raw_target,
                    "target-private-token",
                    "private-token",
                    "STRATUM_CORE_RUNTIME",
                    "durable-cloud",
                ],
            );
        }

        #[tokio::test]
        async fn update_ref_rejects_invalid_expected_target_without_leaking_raw_value() {
            let runtime = DurableCoreRuntime::new(RepoId::local(), StratumStores::local_memory());
            let target = commit_id("target").to_hex();
            let raw_expected_target = "not-a-hex-expected-private-token";

            let error = runtime
                .update_ref("agent/alice/session-1", raw_expected_target, 1, &target)
                .await
                .expect_err("invalid expected target should fail");

            let VfsError::InvalidArgs { message } = error else {
                panic!("invalid expected target should return InvalidArgs");
            };
            assert_eq!(message, "invalid expected ref target commit id");
            assert_message_omits(
                &message,
                &[
                    raw_expected_target,
                    "expected-private-token",
                    "private-token",
                    "STRATUM_CORE_RUNTIME",
                    "durable-cloud",
                ],
            );
        }

        #[tokio::test]
        async fn update_ref_rejects_invalid_ref_name_without_leaking_raw_value() {
            let runtime = DurableCoreRuntime::new(RepoId::local(), StratumStores::local_memory());
            let expected_target = commit_id("expected-target").to_hex();
            let target = commit_id("target").to_hex();
            let raw_ref_name = "agent/alice/private-token/extra";

            let error = runtime
                .update_ref(raw_ref_name, &expected_target, 1, &target)
                .await
                .expect_err("invalid ref name should fail");

            let VfsError::InvalidArgs { message } = error else {
                panic!("invalid ref name should return InvalidArgs");
            };
            assert_eq!(message, "invalid ref name");
            assert_message_omits(
                &message,
                &[
                    raw_ref_name,
                    "alice",
                    "private-token",
                    "STRATUM_CORE_RUNTIME",
                    "durable-cloud",
                ],
            );
        }

        #[tokio::test]
        async fn update_ref_rejects_stale_expectation_without_mutation() {
            let repo_id = RepoId::local();
            let stores = StratumStores::local_memory();
            let runtime = DurableCoreRuntime::new(repo_id.clone(), stores.clone());
            let name = RefName::new("agent/alice/session-1").unwrap();
            let expected_target = commit_id("expected");
            let current_target = commit_id("current");
            let next_target = commit_id("next");

            CommitStore::insert(
                &*stores.commits,
                commit_record(&repo_id, expected_target, "expected"),
            )
            .await
            .unwrap();
            CommitStore::insert(
                &*stores.commits,
                commit_record(&repo_id, current_target, "current"),
            )
            .await
            .unwrap();
            CommitStore::insert(
                &*stores.commits,
                commit_record(&repo_id, next_target, "next"),
            )
            .await
            .unwrap();

            let seeded = RefStore::update(
                &*stores.refs,
                RefUpdate {
                    repo_id: repo_id.clone(),
                    name: name.clone(),
                    target: expected_target,
                    expectation: RefExpectation::MustNotExist,
                },
            )
            .await
            .unwrap();
            let current = RefStore::update(
                &*stores.refs,
                RefUpdate {
                    repo_id: repo_id.clone(),
                    name: name.clone(),
                    target: current_target,
                    expectation: RefExpectation::Matches {
                        target: expected_target,
                        version: seeded.version,
                    },
                },
            )
            .await
            .unwrap();

            let error = runtime
                .update_ref(
                    name.as_str(),
                    &expected_target.to_hex(),
                    seeded.version.value(),
                    &next_target.to_hex(),
                )
                .await
                .expect_err("stale expectation should fail");
            let VfsError::InvalidArgs { message } = error else {
                panic!("stale expectation should return InvalidArgs");
            };
            assert_eq!(message, "ref compare-and-swap mismatch");

            let loaded = RefStore::get(&*stores.refs, &repo_id, &name)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(loaded.target, current_target);
            assert_eq!(loaded.version, current.version);
        }

        #[tokio::test]
        async fn update_ref_rejects_missing_target_after_expectation_without_mutation() {
            let repo_id = RepoId::local();
            let stores = StratumStores::local_memory();
            let runtime = DurableCoreRuntime::new(repo_id.clone(), stores.clone());
            let name = RefName::new("agent/alice/session-1").unwrap();
            let expected_target = commit_id("expected");
            let missing_target = commit_id("missing-target");

            CommitStore::insert(
                &*stores.commits,
                commit_record(&repo_id, expected_target, "expected"),
            )
            .await
            .unwrap();

            let current = RefStore::update(
                &*stores.refs,
                RefUpdate {
                    repo_id: repo_id.clone(),
                    name: name.clone(),
                    target: expected_target,
                    expectation: RefExpectation::MustNotExist,
                },
            )
            .await
            .unwrap();

            let error = runtime
                .update_ref(
                    name.as_str(),
                    &expected_target.to_hex(),
                    current.version.value(),
                    &missing_target.to_hex(),
                )
                .await
                .expect_err("missing target commit should fail");
            let VfsError::ObjectNotFound { id } = error else {
                panic!("missing target should return ObjectNotFound");
            };
            assert_eq!(id, missing_target.to_hex());

            let loaded = RefStore::get(&*stores.refs, &repo_id, &name)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(loaded.target, expected_target);
            assert_eq!(loaded.version, current.version);
        }

        #[tokio::test]
        async fn update_ref_rechecks_expectation_before_missing_target_error() {
            let repo_id = RepoId::local();
            let mut stores = StratumStores::local_memory();
            let inner_commits = stores.commits.clone();
            let refs = stores.refs.clone();
            let name = RefName::new("agent/alice/session-1").unwrap();
            let expected_target = commit_id("expected");
            let racing_target = commit_id("racing");
            let missing_target = commit_id("missing-target");

            CommitStore::insert(
                &*inner_commits,
                commit_record(&repo_id, expected_target, "expected"),
            )
            .await
            .unwrap();
            CommitStore::insert(
                &*inner_commits,
                commit_record(&repo_id, racing_target, "racing"),
            )
            .await
            .unwrap();

            let current = RefStore::update(
                &*refs,
                RefUpdate {
                    repo_id: repo_id.clone(),
                    name: name.clone(),
                    target: expected_target,
                    expectation: RefExpectation::MustNotExist,
                },
            )
            .await
            .unwrap();

            stores.commits = Arc::new(RefMutatingCommitStore {
                inner: inner_commits,
                refs: refs.clone(),
                repo_id: repo_id.clone(),
                name: name.clone(),
                expected_target,
                expected_version: current.version,
                racing_target,
                missing_target,
                fired: AtomicBool::new(false),
            });
            let runtime = DurableCoreRuntime::new(repo_id.clone(), stores.clone());

            let error = runtime
                .update_ref(
                    name.as_str(),
                    &expected_target.to_hex(),
                    current.version.value(),
                    &missing_target.to_hex(),
                )
                .await
                .expect_err("raced missing target should surface as stale CAS");
            let VfsError::InvalidArgs { message } = error else {
                panic!("raced missing target should return InvalidArgs");
            };
            assert_eq!(message, "ref compare-and-swap mismatch");

            let loaded = RefStore::get(&*stores.refs, &repo_id, &name)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(loaded.target, racing_target);
            assert_eq!(loaded.version.value(), current.version.value() + 1);
        }

        #[tokio::test]
        async fn update_ref_updates_existing_ref_for_existing_commit() {
            let repo_id = RepoId::local();
            let stores = StratumStores::local_memory();
            let runtime = DurableCoreRuntime::new(repo_id.clone(), stores.clone());
            let name = RefName::new("agent/alice/session-1").unwrap();
            let expected_target = commit_id("expected");
            let target = commit_id("target");

            CommitStore::insert(
                &*stores.commits,
                commit_record(&repo_id, expected_target, "expected"),
            )
            .await
            .unwrap();
            CommitStore::insert(&*stores.commits, commit_record(&repo_id, target, "target"))
                .await
                .unwrap();

            let current = RefStore::update(
                &*stores.refs,
                RefUpdate {
                    repo_id: repo_id.clone(),
                    name: name.clone(),
                    target: expected_target,
                    expectation: RefExpectation::MustNotExist,
                },
            )
            .await
            .unwrap();

            let updated = runtime
                .update_ref(
                    name.as_str(),
                    &expected_target.to_hex(),
                    current.version.value(),
                    &target.to_hex(),
                )
                .await
                .expect("update_ref should succeed");
            assert_eq!(updated.name, name.as_str());
            assert_eq!(updated.target, target.to_hex());
            assert_eq!(updated.version, current.version.value() + 1);

            let loaded = RefStore::get(&*stores.refs, &repo_id, &name)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(loaded.target, target);
            assert_eq!(loaded.version.value(), updated.version);
        }
    }
}
