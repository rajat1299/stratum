use async_trait::async_trait;
use std::sync::Arc;

use crate::auth::Uid;
use crate::auth::session::Session;
use crate::backend::core_transaction::{DurableCoreStepSemantics, DurableCoreTransactionStep};
use crate::backend::{RepoId, StratumStores};
use crate::db::{DbVcsRef, StratumDb};
use crate::error::VfsError;
use crate::fs::{GrepResult, LsEntry, MetadataUpdate, MetadataUpdateResult, StatInfo};
use crate::store::commit::CommitObject;

pub(crate) type SharedCoreRuntime = Arc<dyn CoreDb>;

pub(crate) type ProtectedPathPredicate = Arc<dyn Fn(&str) -> bool + Send + Sync>;

const DURABLE_CORE_ROUTE_NOT_SUPPORTED: &str =
    "durable core runtime route execution is not supported yet";

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
pub(crate) struct LocalCoreRuntime {
    db: Arc<StratumDb>,
}

impl LocalCoreRuntime {
    #[cfg(test)]
    pub(crate) fn new(db: StratumDb) -> Self {
        Self { db: Arc::new(db) }
    }

    pub(crate) fn from_shared(db: Arc<StratumDb>) -> Self {
        Self { db }
    }

    #[cfg(test)]
    pub(crate) fn shared(db: StratumDb) -> SharedCoreRuntime {
        Arc::new(Self::new(db))
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
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "durable core runtime is intentionally constructed only in tests until routed"
        )
    )]
    pub(crate) fn new(repo_id: RepoId, stores: StratumStores) -> Self {
        Self { repo_id, stores }
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "durable core runtime is intentionally inspected only in tests until routed"
        )
    )]
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
        self.db.cat_with_stat_as(path, session).await
    }

    async fn ls_as(&self, path: Option<&str>, session: &Session) -> Result<Vec<LsEntry>, VfsError> {
        self.db.ls_as(path, session).await
    }

    async fn stat_as(&self, path: &str, session: &Session) -> Result<StatInfo, VfsError> {
        self.db.stat_as(path, session).await
    }

    async fn tree_as(&self, path: Option<&str>, session: &Session) -> Result<String, VfsError> {
        self.db.tree_as(path, session).await
    }

    async fn find_as(
        &self,
        path: Option<&str>,
        pattern: Option<&str>,
        session: &Session,
    ) -> Result<Vec<String>, VfsError> {
        self.db.find_as(path, pattern, session).await
    }

    async fn grep_as(
        &self,
        pattern: &str,
        path: Option<&str>,
        recursive: bool,
        session: &Session,
    ) -> Result<Vec<GrepResult>, VfsError> {
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

    async fn commit_as(&self, message: &str, session: &Session) -> Result<String, VfsError> {
        self.db.commit_as(message, session).await
    }

    async fn vcs_log_as(&self, session: &Session) -> Result<Vec<CommitObject>, VfsError> {
        self.db.vcs_log_as(session).await
    }

    async fn revert_as_with_path_check(
        &self,
        hash_prefix: &str,
        session: &Session,
        is_protected_path: ProtectedPathPredicate,
    ) -> Result<String, VfsError> {
        self.db
            .revert_as_with_path_check(hash_prefix, session, move |path| is_protected_path(path))
            .await
    }

    async fn vcs_status_as(&self, session: &Session) -> Result<String, VfsError> {
        self.db.vcs_status_as(session).await
    }

    async fn vcs_diff_as(&self, path: Option<&str>, session: &Session) -> Result<String, VfsError> {
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
        _path: &str,
        _session: &Session,
    ) -> Result<(Vec<u8>, StatInfo), VfsError> {
        Err(self.route_not_supported())
    }

    async fn ls_as(
        &self,
        _path: Option<&str>,
        _session: &Session,
    ) -> Result<Vec<LsEntry>, VfsError> {
        Err(self.route_not_supported())
    }

    async fn stat_as(&self, _path: &str, _session: &Session) -> Result<StatInfo, VfsError> {
        Err(self.route_not_supported())
    }

    async fn tree_as(&self, _path: Option<&str>, _session: &Session) -> Result<String, VfsError> {
        Err(self.route_not_supported())
    }

    async fn find_as(
        &self,
        _path: Option<&str>,
        _pattern: Option<&str>,
        _session: &Session,
    ) -> Result<Vec<String>, VfsError> {
        Err(self.route_not_supported())
    }

    async fn grep_as(
        &self,
        _pattern: &str,
        _path: Option<&str>,
        _recursive: bool,
        _session: &Session,
    ) -> Result<Vec<GrepResult>, VfsError> {
        Err(self.route_not_supported())
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
        Err(self.route_not_supported())
    }

    async fn create_ref(&self, _name: &str, _target: &str) -> Result<DbVcsRef, VfsError> {
        Err(self.route_not_supported())
    }

    async fn update_ref(
        &self,
        _name: &str,
        _expected_target: &str,
        _expected_version: u64,
        _target: &str,
    ) -> Result<DbVcsRef, VfsError> {
        Err(self.route_not_supported())
    }

    async fn commit_as(&self, _message: &str, _session: &Session) -> Result<String, VfsError> {
        Err(self.route_not_supported())
    }

    async fn vcs_log_as(&self, _session: &Session) -> Result<Vec<CommitObject>, VfsError> {
        Err(self.route_not_supported())
    }

    async fn revert_as_with_path_check(
        &self,
        _hash_prefix: &str,
        _session: &Session,
        _is_protected_path: ProtectedPathPredicate,
    ) -> Result<String, VfsError> {
        Err(self.route_not_supported())
    }

    async fn vcs_status_as(&self, _session: &Session) -> Result<String, VfsError> {
        Err(self.route_not_supported())
    }

    async fn vcs_diff_as(
        &self,
        _path: Option<&str>,
        _session: &Session,
    ) -> Result<String, VfsError> {
        Err(self.route_not_supported())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::session::Session;
    use crate::backend::core_transaction::DurableCoreStepSemantics;
    use crate::backend::{RepoId, StratumStores};

    mod durable_core_runtime {
        use super::*;

        #[test]
        fn reports_contract_without_local_state() {
            let repo_id = RepoId::local();
            let runtime = DurableCoreRuntime::new(repo_id.clone(), StratumStores::local_memory());

            assert_eq!(runtime.repo_id(), &repo_id);
            assert_eq!(
                runtime.transaction_write_path(),
                DurableCoreStepSemantics::ordered_write_path()
            );
            assert!(!runtime.route_execution_enabled());
        }

        #[tokio::test]
        async fn route_methods_fail_closed() {
            let runtime = DurableCoreRuntime::new(RepoId::local(), StratumStores::local_memory());
            let session = Session::root();
            let request_path = "/tenant/alice/private-token";
            let username = "alice-private-token";
            let raw_token = "workspace-secret-token";
            let request_body = b"file body secret".to_vec();
            let ref_name = "agent/alice/private-token";
            let expected_target = "expected-private-token";
            let target = "target-private-token";
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
                    .list_refs()
                    .await
                    .expect_err("list_refs should fail closed"),
                runtime
                    .stat_as(request_path, &session)
                    .await
                    .expect_err("stat should fail closed"),
                runtime
                    .write_file_as(request_path, request_body, &session)
                    .await
                    .expect_err("write_file should fail closed"),
                runtime
                    .create_ref(ref_name, target)
                    .await
                    .expect_err("create_ref should fail closed"),
                runtime
                    .update_ref(ref_name, expected_target, 7, target)
                    .await
                    .expect_err("update_ref should fail closed"),
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
                    ref_name,
                    expected_target,
                    target,
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
    }
}
