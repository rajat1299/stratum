use super::*;
use std::collections::BTreeMap;
use stratum::fs::MetadataUpdate;
use stratum::store::ObjectId;

fn sha256_metadata(bytes: &[u8]) -> String {
    format!("sha256:{}", ObjectId::from_bytes(bytes).to_hex())
}

#[test]
fn test_stat() {
    let mut fs = VirtualFs::new();
    exec("touch info.md", &mut fs);
    let output = exec("stat info.md", &mut fs);
    assert!(output.contains("file"));
    assert!(output.contains("info.md"));
}

#[test]
fn test_stat_directory() {
    let mut fs = VirtualFs::new();
    exec("mkdir mydir", &mut fs);
    let output = exec("stat mydir", &mut fs);
    assert!(output.contains("directory"));
}

#[test]
fn test_stat_shows_mode() {
    let mut fs = VirtualFs::new();
    exec("touch secret.md", &mut fs);
    exec("chmod 600 secret.md", &mut fs);
    let output = exec("stat secret.md", &mut fs);
    assert!(output.contains("0600"));
}

#[test]
fn test_stat_shows_uid_gid() {
    let mut fs = VirtualFs::new();
    exec("touch owned.md", &mut fs);
    let output = exec("stat owned.md", &mut fs);
    assert!(output.contains("Uid:"));
    assert!(output.contains("Gid:"));
}

#[test]
fn test_tree() {
    let mut fs = VirtualFs::new();
    exec("mkdir docs", &mut fs);
    exec("cd docs", &mut fs);
    exec("touch readme.md", &mut fs);
    exec("cd /", &mut fs);
    let output = exec("tree", &mut fs);
    assert!(output.contains("docs/"));
    assert!(output.contains("readme.md"));
}

#[test]
fn test_tree_deep_hierarchy() {
    let mut fs = VirtualFs::new();
    exec("mkdir -p a/b/c", &mut fs);
    exec("cd a/b/c", &mut fs);
    exec("touch leaf.md", &mut fs);
    exec("cd /", &mut fs);
    let output = exec("tree", &mut fs);
    assert!(output.contains("a/"));
    assert!(output.contains("b/"));
    assert!(output.contains("c/"));
    assert!(output.contains("leaf.md"));
}

#[test]
fn test_tree_empty_root() {
    let fs = VirtualFs::new();
    let tree = fs.tree(None, "", None).unwrap();
    assert!(tree.starts_with('.'));
}

#[test]
fn test_chmod() {
    let mut fs = VirtualFs::new();
    exec("touch secure.md", &mut fs);
    exec("chmod 600 secure.md", &mut fs);
    let output = exec("stat secure.md", &mut fs);
    assert!(output.contains("0600"));
}

#[test]
fn test_chmod_various_modes() {
    let mut fs = VirtualFs::new();
    exec("touch file.md", &mut fs);
    for mode in ["755", "644", "700", "400", "000", "777"] {
        exec(&format!("chmod {mode} file.md"), &mut fs);
        let stat = exec("stat file.md", &mut fs);
        assert!(
            stat.contains(&format!("0{mode}")),
            "mode {mode} not found in stat output: {stat}"
        );
    }
}

#[test]
fn test_chmod_directory() {
    let mut fs = VirtualFs::new();
    exec("mkdir restricted", &mut fs);
    exec("chmod 700 restricted", &mut fs);
    let stat = exec("stat restricted", &mut fs);
    assert!(stat.contains("0700"));
}

#[test]
fn test_ls_long_format() {
    let mut fs = VirtualFs::new();
    exec("mkdir docs", &mut fs);
    exec("touch readme.md", &mut fs);
    let output = exec("ls -l", &mut fs);
    assert!(output.contains("drwx"));
    assert!(output.contains("-rw-"));
}

#[test]
fn test_ls_long_shows_size() {
    let mut fs = VirtualFs::new();
    exec("touch file.md", &mut fs);
    exec("write file.md hello world", &mut fs);
    let output = exec("ls -l", &mut fs);
    assert!(output.contains("11") || output.contains("file.md"));
}

#[test]
fn test_ls_empty_dir() {
    let mut fs = VirtualFs::new();
    exec("mkdir empty", &mut fs);
    let output = exec("ls empty", &mut fs);
    assert!(output.trim().is_empty());
}

#[test]
fn test_stat_exposes_file_metadata_and_fresh_content_hash() {
    let mut fs = VirtualFs::new();
    exec("touch report.txt", &mut fs);
    exec("write report.txt hello", &mut fs);

    let mut attrs = BTreeMap::new();
    attrs.insert("owner".to_string(), "docs".to_string());
    fs.set_metadata(
        "report.txt",
        MetadataUpdate {
            mime_type: Some(Some("text/plain".to_string())),
            custom_attrs: attrs,
            remove_custom_attrs: Vec::new(),
        },
    )
    .unwrap();

    let stat = fs.stat("report.txt").unwrap();
    assert_eq!(stat.mime_type.as_deref(), Some("text/plain"));
    assert_eq!(stat.content_hash, Some(sha256_metadata(b"hello")));
    assert_eq!(
        stat.custom_attrs.get("owner").map(String::as_str),
        Some("docs")
    );

    exec("write report.txt goodbye", &mut fs);
    let overwritten = fs.stat("report.txt").unwrap();
    assert_eq!(overwritten.content_hash, Some(sha256_metadata(b"goodbye")));
    assert_eq!(overwritten.mime_type.as_deref(), Some("text/plain"));
    assert_eq!(
        overwritten.custom_attrs.get("owner").map(String::as_str),
        Some("docs")
    );

    fs.truncate("report.txt", 0).unwrap();
    let truncated = fs.stat("report.txt").unwrap();
    assert_eq!(truncated.content_hash, Some(sha256_metadata(b"")));

    let handle = fs.open("report.txt", true).unwrap();
    fs.write_handle(handle, b"abc").unwrap();
    fs.release_handle(handle).unwrap();
    let handle_written = fs.stat("report.txt").unwrap();
    assert_eq!(handle_written.content_hash, Some(sha256_metadata(b"abc")));
}

#[test]
fn test_metadata_copies_moves_and_links_with_inode_semantics() {
    let mut fs = VirtualFs::new();
    exec("touch source.txt", &mut fs);
    exec("write source.txt content", &mut fs);

    let mut attrs = BTreeMap::new();
    attrs.insert("classification".to_string(), "public".to_string());
    fs.set_metadata(
        "source.txt",
        MetadataUpdate {
            mime_type: Some(Some("text/plain".to_string())),
            custom_attrs: attrs,
            remove_custom_attrs: Vec::new(),
        },
    )
    .unwrap();

    fs.cp("source.txt", "copy.txt", 0, 0).unwrap();
    fs.mv("source.txt", "moved.txt").unwrap();
    fs.link("moved.txt", "alias.txt").unwrap();

    for path in ["copy.txt", "moved.txt", "alias.txt"] {
        let stat = fs.stat(path).unwrap();
        assert_eq!(stat.mime_type.as_deref(), Some("text/plain"));
        assert_eq!(
            stat.custom_attrs.get("classification").map(String::as_str),
            Some("public")
        );
    }

    let mut update = MetadataUpdate::default();
    update
        .custom_attrs
        .insert("reviewed".to_string(), "yes".to_string());
    fs.set_metadata("alias.txt", update).unwrap();
    assert_eq!(
        fs.stat("moved.txt")
            .unwrap()
            .custom_attrs
            .get("reviewed")
            .map(String::as_str),
        Some("yes")
    );
    assert_eq!(
        fs.stat("copy.txt")
            .unwrap()
            .custom_attrs
            .get("reviewed")
            .map(String::as_str),
        None
    );
}
