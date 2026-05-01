use super::*;
use stratum::persist::PersistManager;
use stratum::vcs::{ChangeKind, CommitId, MAIN_REF, RefName, Vcs};

#[test]
fn test_persist_save_and_load() {
    let tmp = std::env::temp_dir().join(format!("stratum_test_{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();

    let persist = PersistManager::new(&tmp);

    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("mkdir docs", &mut fs);
    exec("touch readme.md", &mut fs);
    exec("write readme.md # Hello World", &mut fs);
    exec("cd docs", &mut fs);
    exec("touch notes.md", &mut fs);
    exec("write notes.md Some notes here", &mut fs);
    exec("cd /", &mut fs);

    vcs.commit(&fs, "initial", "root").unwrap();

    exec("touch changelog.md", &mut fs);
    exec("write changelog.md ## v0.1.0", &mut fs);
    vcs.commit(&fs, "add changelog", "root").unwrap();

    persist.save(&fs, &vcs).unwrap();
    assert!(persist.state_exists());

    let (fs2, vcs2) = persist.load().unwrap();

    assert_eq!(
        String::from_utf8_lossy(fs2.cat("readme.md").unwrap()),
        "# Hello World"
    );
    assert_eq!(
        String::from_utf8_lossy(fs2.cat("docs/notes.md").unwrap()),
        "Some notes here"
    );
    assert_eq!(
        String::from_utf8_lossy(fs2.cat("changelog.md").unwrap()),
        "## v0.1.0"
    );

    let commits = vcs2.log();
    assert_eq!(commits.len(), 2);
    assert_eq!(commits[1].message, "initial");
    assert_eq!(commits[0].message, "add changelog");
    assert!(
        commits[0]
            .changed_paths
            .iter()
            .any(|change| change.path == "/changelog.md" && change.kind == ChangeKind::Added)
    );
    assert!(vcs2.head().is_some());

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_refs_survive_save_and_load() {
    let tmp = std::env::temp_dir().join(format!("stratum_refs_{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();

    let persist = PersistManager::new(&tmp);

    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch data.md", &mut fs);
    let id1 = vcs.commit(&fs, "v1", "root").unwrap();
    exec("touch notes.md", &mut fs);
    let id2 = vcs.commit(&fs, "v2", "root").unwrap();

    let session_ref = RefName::session("alice", "s1").unwrap();
    vcs.create_ref(session_ref.clone(), CommitId::from(id1))
        .unwrap();

    persist.save(&fs, &vcs).unwrap();
    let (_, vcs2) = persist.load().unwrap();

    let main = vcs2
        .get_ref(RefName::new(MAIN_REF).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(main.target, CommitId::from(id2));

    let loaded_session = vcs2.get_ref(session_ref).unwrap().unwrap();
    assert_eq!(loaded_session.target, CommitId::from(id1));
    assert_eq!(loaded_session.version, 1);

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_persist_revert_after_reload() {
    let tmp = std::env::temp_dir().join(format!("stratum_revert_{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();

    let persist = PersistManager::new(&tmp);

    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch data.md", &mut fs);
    exec("write data.md version1", &mut fs);
    let id1 = vcs.commit(&fs, "v1", "root").unwrap();

    exec("write data.md version2", &mut fs);
    vcs.commit(&fs, "v2", "root").unwrap();

    persist.save(&fs, &vcs).unwrap();
    let (mut fs2, mut vcs2) = persist.load().unwrap();

    vcs2.revert(&mut fs2, &id1.short_hex()).unwrap();
    assert_eq!(
        String::from_utf8_lossy(fs2.cat("data.md").unwrap()),
        "version1"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_persist_empty_state() {
    let tmp = std::env::temp_dir().join(format!("stratum_empty_{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();

    let persist = PersistManager::new(&tmp);

    let fs = VirtualFs::new();
    let vcs = Vcs::new();
    persist.save(&fs, &vcs).unwrap();

    let (fs2, vcs2) = persist.load().unwrap();
    assert_eq!(fs2.pwd(), "/");
    assert_eq!(vcs2.commit_count(), 0);
    assert!(vcs2.head().is_none());

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_persist_preserves_permissions() {
    let tmp = std::env::temp_dir().join(format!("stratum_perms_{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();

    let persist = PersistManager::new(&tmp);

    let mut fs = VirtualFs::new();
    let vcs = Vcs::new();

    exec("touch secret.md", &mut fs);
    exec("chmod 600 secret.md", &mut fs);
    exec("mkdir restricted", &mut fs);
    exec("chmod 700 restricted", &mut fs);

    persist.save(&fs, &vcs).unwrap();
    let (fs2, _) = persist.load().unwrap();

    let stat = fs2.stat("secret.md").unwrap();
    assert_eq!(stat.mode, 0o600);
    let stat = fs2.stat("restricted").unwrap();
    assert_eq!(stat.mode, 0o700);

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_persist_preserves_user_registry() {
    let tmp = std::env::temp_dir().join(format!("stratum_reg_{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();

    let persist = PersistManager::new(&tmp);

    let mut fs = VirtualFs::new();
    let mut root_session = Session::root();
    let vcs = Vcs::new();

    exec_s("adduser alice", &mut fs, &mut root_session);
    exec_s("addgroup devs", &mut fs, &mut root_session);
    exec_s("usermod -aG devs alice", &mut fs, &mut root_session);

    persist.save(&fs, &vcs).unwrap();
    let (fs2, _) = persist.load().unwrap();

    assert!(fs2.registry.lookup_uid("alice").is_some());
    assert!(fs2.registry.lookup_gid("devs").is_some());

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn test_persist_multiple_save_load_cycles() {
    let tmp = std::env::temp_dir().join(format!("stratum_cycle_{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();
    let persist = PersistManager::new(&tmp);

    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    for i in 0..5 {
        exec(&format!("touch file_{i}.md"), &mut fs);
        exec(&format!("write file_{i}.md content {i}"), &mut fs);
        vcs.commit(&fs, &format!("commit {i}"), "root").unwrap();
        persist.save(&fs, &vcs).unwrap();

        let (loaded_fs, loaded_vcs) = persist.load().unwrap();
        assert_eq!(loaded_vcs.commit_count(), i + 1);
        assert_eq!(
            String::from_utf8_lossy(loaded_fs.cat(&format!("file_{i}.md")).unwrap()),
            format!("content {i}")
        );
    }

    let _ = std::fs::remove_dir_all(&tmp);
}
