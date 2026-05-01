use super::*;
use std::collections::BTreeMap;
use stratum::fs::MetadataUpdate;
use stratum::store::ObjectId;
use stratum::vcs::{
    ChangeKind, ChangedPath, CommitId, MAIN_REF, RefName, RefUpdateExpectation, Vcs,
};

fn assert_change(changes: &[ChangedPath], path: &str, kind: ChangeKind) {
    assert!(
        changes
            .iter()
            .any(|change| change.path == path && change.kind == kind),
        "expected {kind:?} for {path}, got {changes:?}"
    );
}

#[test]
fn test_commit_and_log() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch readme.md", &mut fs);
    exec("write readme.md # Hello", &mut fs);

    let id = vcs.commit(&fs, "initial commit", "root").unwrap();
    assert!(!id.to_hex().is_empty());

    let log = vcs.log();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].message, "initial commit");
    assert_eq!(log[0].author, "root");
}

#[test]
fn test_commit_write_set_records_changed_paths() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch a.md", &mut fs);
    exec("write a.md before", &mut fs);
    exec("mkdir -p dir", &mut fs);
    exec("touch dir/old.md", &mut fs);
    exec("write dir/old.md old", &mut fs);
    exec("touch meta.md", &mut fs);

    vcs.commit(&fs, "initial", "root").unwrap();
    let initial = &vcs.log()[0].changed_paths;
    assert_change(initial, "/a.md", ChangeKind::Added);
    assert_change(initial, "/dir", ChangeKind::Added);
    assert_change(initial, "/dir/old.md", ChangeKind::Added);
    assert_change(initial, "/meta.md", ChangeKind::Added);

    exec("write a.md after", &mut fs);
    exec("touch new.md", &mut fs);
    exec("rm dir/old.md", &mut fs);
    exec("chmod 600 meta.md", &mut fs);

    vcs.commit(&fs, "second", "root").unwrap();
    let changed_paths = &vcs.log()[0].changed_paths;
    assert_eq!(changed_paths.len(), 4);
    assert_change(changed_paths, "/a.md", ChangeKind::Modified);
    assert_change(changed_paths, "/dir/old.md", ChangeKind::Deleted);
    assert_change(changed_paths, "/meta.md", ChangeKind::MetadataChanged);
    assert_change(changed_paths, "/new.md", ChangeKind::Added);
}

#[test]
fn test_vcs_tracks_and_restores_file_metadata() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch meta.txt", &mut fs);
    exec("write meta.txt initial", &mut fs);
    let mut attrs = BTreeMap::new();
    attrs.insert("owner".to_string(), "docs".to_string());
    fs.set_metadata(
        "meta.txt",
        MetadataUpdate {
            mime_type: Some(Some("text/plain".to_string())),
            custom_attrs: attrs,
            remove_custom_attrs: Vec::new(),
        },
    )
    .unwrap();

    let first = vcs.commit(&fs, "initial", "root").unwrap();

    let mut update = MetadataUpdate {
        mime_type: Some(Some("text/custom".to_string())),
        ..MetadataUpdate::default()
    };
    update
        .custom_attrs
        .insert("owner".to_string(), "engineering".to_string());
    fs.set_metadata("meta.txt", update).unwrap();

    let summary = vcs.status_summary(&fs).unwrap();
    assert_change(&summary.changes, "/meta.txt", ChangeKind::MetadataChanged);

    vcs.revert(&mut fs, &first.short_hex()).unwrap();
    let stat = fs.stat("meta.txt").unwrap();
    assert_eq!(stat.mime_type.as_deref(), Some("text/plain"));
    assert_eq!(
        stat.custom_attrs.get("owner").map(String::as_str),
        Some("docs")
    );
}

#[test]
fn test_status_summary_and_text_status_report_dirty_paths_without_writing_objects() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch tracked.md", &mut fs);
    exec("write tracked.md committed", &mut fs);
    exec("mkdir -p dir", &mut fs);
    exec("touch dir/file.md", &mut fs);
    exec("write dir/file.md committed", &mut fs);
    vcs.commit(&fs, "initial", "root").unwrap();

    let object_count = vcs.object_count();
    exec("write tracked.md changed", &mut fs);
    exec("touch added.md", &mut fs);
    exec("rm -r dir", &mut fs);

    let summary = vcs.status_summary(&fs).unwrap();
    assert_eq!(summary.head, vcs.head());
    assert_eq!(summary.object_count, object_count);
    assert_eq!(summary.changes.len(), 4);
    assert_change(&summary.changes, "/added.md", ChangeKind::Added);
    assert_change(&summary.changes, "/dir", ChangeKind::Deleted);
    assert_change(&summary.changes, "/dir/file.md", ChangeKind::Deleted);
    assert_change(&summary.changes, "/tracked.md", ChangeKind::Modified);

    let status = vcs.status(&fs).unwrap();
    assert!(status.contains("On commit "));
    assert!(status.contains("Objects in store: "));
    assert!(status.contains("Files: 2, Total size: 7 bytes"));
    assert!(status.contains("Changes:"));
    assert!(status.contains("A /added.md"));
    assert!(status.contains("D /dir"));
    assert!(status.contains("D /dir/file.md"));
    assert!(status.contains("M /tracked.md"));
    assert_eq!(vcs.object_count(), object_count);
}

#[test]
fn test_status_reports_clean_working_tree() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch clean.md", &mut fs);
    exec("write clean.md clean", &mut fs);
    vcs.commit(&fs, "initial", "root").unwrap();

    let summary = vcs.status_summary(&fs).unwrap();
    assert!(summary.changes.is_empty());

    let status = vcs.status(&fs).unwrap();
    assert!(status.contains("Working tree clean"));
}

#[test]
fn test_status_summary_without_commits_compares_against_empty_base() {
    let mut fs = VirtualFs::new();
    let vcs = Vcs::new();

    exec("touch draft.md", &mut fs);
    exec("mkdir -p dir", &mut fs);
    exec("touch dir/file.md", &mut fs);

    let summary = vcs.status_summary(&fs).unwrap();
    assert_eq!(summary.head, None);
    assert_eq!(summary.changes.len(), 3);
    assert_change(&summary.changes, "/dir", ChangeKind::Added);
    assert_change(&summary.changes, "/dir/file.md", ChangeKind::Added);
    assert_change(&summary.changes, "/draft.md", ChangeKind::Added);

    assert_eq!(vcs.status(&fs).unwrap(), "No commits yet.\n");
}

#[test]
fn test_diff_reports_text_file_line_changes() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    fs.touch("/note.md", 0, 0).unwrap();
    fs.write_file("/note.md", b"alpha\nbeta\ngamma\n".to_vec())
        .unwrap();
    vcs.commit(&fs, "initial", "root").unwrap();

    fs.write_file("/note.md", b"alpha\nbravo\ngamma\n".to_vec())
        .unwrap();

    let diff = vcs.diff(&fs, Some("/note.md")).unwrap();
    assert!(diff.contains("diff -- /note.md"));
    assert!(diff.contains("--- a/note.md"));
    assert!(diff.contains("+++ b/note.md"));
    assert!(diff.contains(" alpha"));
    assert!(diff.contains("-beta"));
    assert!(diff.contains("+bravo"));
    assert!(diff.contains(" gamma"));
}

#[test]
fn test_diff_reports_metadata_only_file_changes() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    fs.touch("/meta.txt", 0, 0).unwrap();
    fs.write_file("/meta.txt", b"same\n".to_vec()).unwrap();
    let mut attrs = BTreeMap::new();
    attrs.insert("owner".to_string(), "docs".to_string());
    fs.set_metadata(
        "/meta.txt",
        MetadataUpdate {
            mime_type: Some(Some("text/plain".to_string())),
            custom_attrs: attrs,
            remove_custom_attrs: Vec::new(),
        },
    )
    .unwrap();
    vcs.commit(&fs, "initial", "root").unwrap();

    let mut update = MetadataUpdate {
        mime_type: Some(Some("text/custom".to_string())),
        ..MetadataUpdate::default()
    };
    update
        .custom_attrs
        .insert("owner".to_string(), "engineering".to_string());
    fs.set_metadata("/meta.txt", update).unwrap();

    let diff = vcs.diff(&fs, Some("/meta.txt")).unwrap();
    assert!(diff.contains("diff -- /meta.txt"));
    assert!(diff.contains("metadata:"));
    assert!(diff.contains("- mime_type: text/plain"));
    assert!(diff.contains("+ mime_type: text/custom"));
    assert!(diff.contains("- custom_attrs.owner: docs"));
    assert!(diff.contains("+ custom_attrs.owner: engineering"));
    assert!(!diff.contains("No changes."));
}

#[test]
fn test_diff_path_filter_includes_directory_children() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    fs.mkdir_p("/dir", 0, 0).unwrap();
    fs.touch("/dir/note.md", 0, 0).unwrap();
    fs.write_file("/dir/note.md", b"before\n".to_vec()).unwrap();
    fs.touch("/outside.md", 0, 0).unwrap();
    fs.write_file("/outside.md", b"before\n".to_vec()).unwrap();
    vcs.commit(&fs, "initial", "root").unwrap();

    fs.write_file("/dir/note.md", b"after\n".to_vec()).unwrap();
    fs.write_file("/outside.md", b"after\n".to_vec()).unwrap();

    let diff = vcs.diff(&fs, Some("/dir")).unwrap();
    assert!(diff.contains("diff -- /dir/note.md"));
    assert!(!diff.contains("diff -- /outside.md"));
}

#[test]
fn test_diff_type_change_between_directory_and_file_is_reported_not_failed() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    fs.mkdir_p("/node", 0, 0).unwrap();
    vcs.commit(&fs, "directory", "root").unwrap();

    fs.rmdir("/node").unwrap();
    fs.touch("/node", 0, 0).unwrap();
    fs.write_file("/node", b"now a file\n".to_vec()).unwrap();

    let diff = vcs.diff(&fs, Some("/node")).unwrap();
    assert!(diff.contains("diff -- /node"));
    assert!(diff.contains("Non-file changes are not supported"));
}

#[test]
fn test_diff_reports_non_utf8_content_as_unsupported() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    fs.touch("/binary.md", 0, 0).unwrap();
    fs.write_file("/binary.md", b"text\n".to_vec()).unwrap();
    vcs.commit(&fs, "initial", "root").unwrap();

    fs.write_file("/binary.md", vec![0xff, 0xfe, 0xfd]).unwrap();

    let diff = vcs.diff(&fs, Some("/binary.md")).unwrap();
    assert!(diff.contains("diff -- /binary.md"));
    assert!(diff.contains("Binary or non-UTF-8 content is not supported"));
}

#[test]
fn test_diff_reports_control_byte_content_as_unsupported() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    fs.touch("/control.md", 0, 0).unwrap();
    fs.write_file("/control.md", b"text\n".to_vec()).unwrap();
    vcs.commit(&fs, "initial", "root").unwrap();

    fs.write_file("/control.md", b"valid utf8\0but binary".to_vec())
        .unwrap();

    let diff = vcs.diff(&fs, Some("/control.md")).unwrap();
    assert!(diff.contains("diff -- /control.md"));
    assert!(diff.contains("Binary or non-UTF-8 content is not supported"));
}

#[test]
fn test_commit_creates_and_updates_main_ref() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch a.md", &mut fs);
    let id1 = vcs.commit(&fs, "first", "root").unwrap();

    let main = vcs
        .get_ref(RefName::new(MAIN_REF).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(main.target, CommitId::from(id1));
    assert_eq!(main.version, 1);

    exec("touch b.md", &mut fs);
    let id2 = vcs.commit(&fs, "second", "root").unwrap();

    let main = vcs
        .get_ref(RefName::new(MAIN_REF).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(main.target, CommitId::from(id2));
    assert_eq!(main.version, 2);
    assert_eq!(vcs.head(), Some(id2));
}

#[test]
fn test_create_list_get_ref_to_existing_commit() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch a.md", &mut fs);
    let id = vcs.commit(&fs, "initial", "root").unwrap();
    let name = RefName::session("alice", "s1").unwrap();

    let created = vcs.create_ref(name.clone(), CommitId::from(id)).unwrap();
    assert_eq!(created.name, name);
    assert_eq!(created.target, CommitId::from(id));
    assert_eq!(created.version, 1);

    let fetched = vcs.get_ref(name.clone()).unwrap().unwrap();
    assert_eq!(fetched, created);
    assert!(vcs.list_refs().into_iter().any(|r| r.name == name));
}

#[test]
fn test_ref_name_validation_matches_v2_namespaces() {
    assert_eq!(
        RefName::session("legal-bot", "session-123")
            .unwrap()
            .as_str(),
        "agent/legal-bot/session-123"
    );
    assert!(RefName::new("main").is_ok());
    assert!(RefName::new("review/cr-123").is_ok());
    assert!(RefName::new("archive/2026-04-29").is_ok());
    assert!(RefName::new("legal-review").is_err());
    assert!(RefName::new("foo/bar").is_err());
    assert!(RefName::new("agent/alice").is_err());
    assert!(RefName::new("agent/alice/../s1").is_err());
    assert!(RefName::new("main.lock").is_err());
    assert!(RefName::new("refs/heads/main").is_err());
}

#[test]
fn test_duplicate_create_ref_fails() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch a.md", &mut fs);
    let id = vcs.commit(&fs, "initial", "root").unwrap();
    let name = RefName::session("alice", "s1").unwrap();

    vcs.create_ref(name.clone(), CommitId::from(id)).unwrap();
    assert!(vcs.create_ref(name, CommitId::from(id)).is_err());
}

#[test]
fn test_update_ref_to_unknown_commit_fails() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch a.md", &mut fs);
    let id = vcs.commit(&fs, "initial", "root").unwrap();
    let name = RefName::session("alice", "s1").unwrap();
    vcs.create_ref(name.clone(), CommitId::from(id)).unwrap();

    let unknown = CommitId::from(ObjectId::from_bytes(b"not a commit"));
    assert!(
        vcs.update_ref(
            name.clone(),
            RefUpdateExpectation::new(CommitId::from(id), 1),
            unknown,
        )
        .is_err()
    );

    let fetched = vcs.get_ref(name).unwrap().unwrap();
    assert_eq!(fetched.target, CommitId::from(id));
}

#[test]
fn test_compare_and_swap_ref_succeeds_and_fails_without_changing_ref() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch a.md", &mut fs);
    let id1 = vcs.commit(&fs, "first", "root").unwrap();
    exec("touch b.md", &mut fs);
    let id2 = vcs.commit(&fs, "second", "root").unwrap();
    exec("touch c.md", &mut fs);
    let id3 = vcs.commit(&fs, "third", "root").unwrap();

    let name = RefName::session("alice", "s1").unwrap();
    vcs.create_ref(name.clone(), CommitId::from(id1)).unwrap();

    let swapped = vcs
        .compare_and_swap_ref(
            name.clone(),
            Some(RefUpdateExpectation::new(CommitId::from(id1), 1)),
            CommitId::from(id2),
        )
        .unwrap();
    assert_eq!(swapped.target, CommitId::from(id2));
    assert_eq!(swapped.version, 2);

    assert!(
        vcs.compare_and_swap_ref(
            name.clone(),
            Some(RefUpdateExpectation::new(CommitId::from(id2), 1)),
            CommitId::from(id3),
        )
        .is_err()
    );

    assert!(
        vcs.compare_and_swap_ref(
            name.clone(),
            Some(RefUpdateExpectation::new(CommitId::from(id1), 1)),
            CommitId::from(id3),
        )
        .is_err()
    );
    let fetched = vcs.get_ref(name).unwrap().unwrap();
    assert_eq!(fetched.target, CommitId::from(id2));
    assert_eq!(fetched.version, 2);
}

#[test]
fn test_compare_and_swap_expected_none_creates_only_when_absent() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch a.md", &mut fs);
    let id1 = vcs.commit(&fs, "first", "root").unwrap();
    exec("touch b.md", &mut fs);
    let id2 = vcs.commit(&fs, "second", "root").unwrap();

    let name = RefName::session("alice", "s1").unwrap();
    let created = vcs
        .compare_and_swap_ref(name.clone(), None, CommitId::from(id1))
        .unwrap();
    assert_eq!(created.target, CommitId::from(id1));
    assert_eq!(created.version, 1);

    assert!(
        vcs.compare_and_swap_ref(name.clone(), None, CommitId::from(id2))
            .is_err()
    );
    let fetched = vcs.get_ref(name).unwrap().unwrap();
    assert_eq!(fetched.target, CommitId::from(id1));
    assert_eq!(fetched.version, 1);
}

#[test]
fn test_main_ref_update_keeps_legacy_head_in_sync() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch a.md", &mut fs);
    let id1 = vcs.commit(&fs, "first", "root").unwrap();
    exec("touch b.md", &mut fs);
    let id2 = vcs.commit(&fs, "second", "root").unwrap();

    let main = vcs
        .get_ref(RefName::new(MAIN_REF).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(main.target, CommitId::from(id2));

    let updated = vcs
        .update_ref(
            RefName::new(MAIN_REF).unwrap(),
            RefUpdateExpectation::new(CommitId::from(id2), main.version),
            CommitId::from(id1),
        )
        .unwrap();

    assert_eq!(updated.target, CommitId::from(id1));
    assert_eq!(vcs.head(), Some(id1));
}

#[test]
fn test_commit_and_revert() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch a.md", &mut fs);
    exec("write a.md version1", &mut fs);
    let id1 = vcs.commit(&fs, "v1", "root").unwrap();

    exec("write a.md version2", &mut fs);
    exec("touch b.md", &mut fs);
    exec("write b.md extra", &mut fs);
    let _id2 = vcs.commit(&fs, "v2", "root").unwrap();

    assert_eq!(exec("cat a.md", &mut fs), "version2");
    assert!(exec("ls", &mut fs).contains("b.md"));

    vcs.revert(&mut fs, &id1.short_hex()).unwrap();

    assert_eq!(exec("cat a.md", &mut fs), "version1");
    assert!(!exec("ls", &mut fs).contains("b.md"));
}

#[test]
fn test_multiple_commits_and_log() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch a.md", &mut fs);
    vcs.commit(&fs, "first", "root").unwrap();

    exec("touch b.md", &mut fs);
    vcs.commit(&fs, "second", "root").unwrap();

    exec("touch c.md", &mut fs);
    vcs.commit(&fs, "third", "root").unwrap();

    let log = vcs.log();
    assert_eq!(log.len(), 3);
    assert_eq!(log[0].message, "third");
    assert_eq!(log[1].message, "second");
    assert_eq!(log[2].message, "first");
}

#[test]
fn test_deduplication() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch a.md", &mut fs);
    exec("write a.md same content", &mut fs);
    exec("touch b.md", &mut fs);
    exec("write b.md same content", &mut fs);

    vcs.commit(&fs, "dedup test", "root").unwrap();

    let count = vcs.object_count();
    assert!(count > 0);
}

#[test]
fn test_revert_to_first_of_many_commits() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch file.md", &mut fs);
    exec("write file.md v1", &mut fs);
    let id1 = vcs.commit(&fs, "c1", "root").unwrap();

    for i in 2..=10 {
        exec(&format!("write file.md v{i}"), &mut fs);
        vcs.commit(&fs, &format!("c{i}"), "root").unwrap();
    }

    assert_eq!(exec("cat file.md", &mut fs), "v10");
    vcs.revert(&mut fs, &id1.short_hex()).unwrap();
    assert_eq!(exec("cat file.md", &mut fs), "v1");
}

#[test]
fn test_commit_with_directories_and_revert() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("mkdir -p src/lib", &mut fs);
    exec("cd src/lib", &mut fs);
    exec("touch module.md", &mut fs);
    exec("write module.md # Module", &mut fs);
    exec("cd /", &mut fs);
    exec("touch readme.md", &mut fs);
    exec("write readme.md # Project", &mut fs);

    let id1 = vcs.commit(&fs, "project structure", "root").unwrap();

    exec("rm -r src", &mut fs);
    exec("write readme.md # Changed", &mut fs);
    vcs.commit(&fs, "destructive change", "root").unwrap();

    vcs.revert(&mut fs, &id1.short_hex()).unwrap();

    assert_eq!(exec("cat readme.md", &mut fs), "# Project");
    assert_eq!(exec("cat src/lib/module.md", &mut fs), "# Module");
}

#[test]
fn test_commit_preserves_author() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch file.md", &mut fs);
    vcs.commit(&fs, "by alice", "alice").unwrap();
    vcs.commit(&fs, "by bob", "bob").unwrap();

    let log = vcs.log();
    assert_eq!(log[0].author, "bob");
    assert_eq!(log[1].author, "alice");
}

#[test]
fn test_dedup_identical_files() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    let content = "# Same template\n\nIdentical everywhere.\n";
    for i in 0..100 {
        let path = format!("dup_{i:03}.md");
        fs.touch(&path, 0, 0).unwrap();
        fs.write_file(&path, content.as_bytes().to_vec()).unwrap();
    }

    vcs.commit(&fs, "dedup test", "root").unwrap();
    let count = vcs.object_count();
    // 1 blob + 1 tree + 1 commit = 3 objects (not 100+)
    assert!(
        count < 10,
        "expected dedup, got {count} objects for 100 identical files"
    );
}

#[test]
fn test_dedup_after_modification() {
    let mut fs = VirtualFs::new();
    let mut vcs = Vcs::new();

    exec("touch a.md", &mut fs);
    exec("write a.md shared", &mut fs);
    exec("touch b.md", &mut fs);
    exec("write b.md shared", &mut fs);
    vcs.commit(&fs, "c1", "root").unwrap();

    exec("write a.md different", &mut fs);
    vcs.commit(&fs, "c2", "root").unwrap();

    // a.md has a new blob, b.md still references the old one
    let count = vcs.object_count();
    assert!(count > 3, "should have new blob for 'different'");
}
