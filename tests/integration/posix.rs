use stratum::auth::session::Session;
use stratum::error::VfsError;
use stratum::posix::{
    PosixFs, PosixSetAttr, PosixXattrSetMode, STRATUM_CUSTOM_XATTR_PREFIX, STRATUM_MIME_XATTR,
};

use super::*;

#[test]
fn test_posix_mode_allows_non_markdown_regular_files() {
    let mut fs = VirtualFs::new_posix();
    let root = Session::root();
    let mut posix = PosixFs::new(&mut fs, &root);

    let _fh = posix.create("/notes.txt", 0o644).unwrap();
    posix.write("/notes.txt", 0, b"plain text").unwrap();

    let stat = posix.getattr("/notes.txt").unwrap();
    assert_eq!(stat.kind, "file");
    assert_eq!(stat.size, 10);
}

#[test]
fn test_posix_offset_io_and_truncate() {
    let mut fs = VirtualFs::new_posix();
    let root = Session::root();
    let mut posix = PosixFs::new(&mut fs, &root);

    let _fh = posix.create("/blob.bin", 0o644).unwrap();
    posix.write("/blob.bin", 0, b"hello world").unwrap();
    posix.write("/blob.bin", 6, b"POSIX").unwrap();

    assert_eq!(posix.read("/blob.bin", 0, 11).unwrap(), b"hello POSIX");

    posix.truncate("/blob.bin", 5).unwrap();
    assert_eq!(posix.read("/blob.bin", 0, 16).unwrap(), b"hello");

    let stat = posix.getattr("/blob.bin").unwrap();
    assert_eq!(stat.size, 5);
}

#[test]
fn test_posix_hard_links_update_nlink() {
    let mut fs = VirtualFs::new_posix();
    let root = Session::root();
    let mut posix = PosixFs::new(&mut fs, &root);

    let _fh = posix.create("/alpha.txt", 0o644).unwrap();
    posix.write("/alpha.txt", 0, b"same inode").unwrap();
    posix.link("/alpha.txt", "/beta.txt").unwrap();

    let alpha = posix.getattr("/alpha.txt").unwrap();
    let beta = posix.getattr("/beta.txt").unwrap();
    assert_eq!(alpha.inode_id, beta.inode_id);
    assert_eq!(alpha.nlink, 2);

    posix.unlink("/alpha.txt").unwrap();
    assert_eq!(posix.read("/beta.txt", 0, 32).unwrap(), b"same inode");
    assert_eq!(posix.getattr("/beta.txt").unwrap().nlink, 1);
}

#[test]
fn test_posix_open_handle_survives_unlink_until_release() {
    let mut fs = VirtualFs::new_posix();
    let root = Session::root();

    {
        let mut posix = PosixFs::new(&mut fs, &root);
        let fh = posix.create("/temp.log", 0o644).unwrap();
        posix.write("/temp.log", 0, b"orphaned").unwrap();
        posix.unlink("/temp.log").unwrap();
        assert_eq!(posix.read_handle(fh, 32).unwrap(), b"orphaned");
        posix.release(fh).unwrap();
    }

    assert!(fs.resolve_path("/temp.log").is_err());
}

#[test]
fn test_posix_setattr_updates_mode_and_size() {
    let mut fs = VirtualFs::new_posix();
    let root = Session::root();
    let mut posix = PosixFs::new(&mut fs, &root);

    let _fh = posix.create("/state.json", 0o644).unwrap();
    posix.write("/state.json", 0, br#"{"ok":true}"#).unwrap();

    let stat = posix
        .setattr(
            "/state.json",
            PosixSetAttr {
                mode: Some(0o600),
                size: Some(5),
                ..Default::default()
            },
        )
        .unwrap();

    assert_eq!(stat.mode, 0o600);
    assert_eq!(stat.size, 5);
    assert_eq!(posix.read("/state.json", 0, 16).unwrap(), br#"{"ok""#);
}

#[test]
fn test_posix_xattr_mime_round_trip() {
    let mut fs = VirtualFs::new_posix();
    let root = Session::root();
    let mut posix = PosixFs::new(&mut fs, &root);

    let _fh = posix.create("/image.png", 0o644).unwrap();
    let initial_stat = posix.getattr("/image.png").unwrap();
    let initial_changed = (initial_stat.changed, initial_stat.changed_nanos);
    std::thread::sleep(std::time::Duration::from_millis(1));

    posix
        .setxattr(
            "/image.png",
            STRATUM_MIME_XATTR,
            b"image/png",
            PosixXattrSetMode::Upsert,
        )
        .unwrap();

    assert_eq!(
        posix.getxattr("/image.png", STRATUM_MIME_XATTR).unwrap(),
        b"image/png"
    );
    assert_eq!(posix.listxattr("/image.png").unwrap(), [STRATUM_MIME_XATTR]);

    let updated_stat = posix.getattr("/image.png").unwrap();
    assert_eq!(updated_stat.mime_type.as_deref(), Some("image/png"));
    assert!((updated_stat.changed, updated_stat.changed_nanos) > initial_changed);
}

#[test]
fn test_posix_xattr_custom_round_trip_list_and_remove() {
    let mut fs = VirtualFs::new_posix();
    let root = Session::root();
    let mut posix = PosixFs::new(&mut fs, &root);
    let owner_attr = format!("{STRATUM_CUSTOM_XATTR_PREFIX}owner");
    let reviewer_attr = format!("{STRATUM_CUSTOM_XATTR_PREFIX}reviewer");

    let _fh = posix.create("/report.txt", 0o644).unwrap();
    posix
        .setxattr(
            "/report.txt",
            &owner_attr,
            b"docs",
            PosixXattrSetMode::Upsert,
        )
        .unwrap();
    posix
        .setxattr(
            "/report.txt",
            &reviewer_attr,
            b"legal",
            PosixXattrSetMode::Upsert,
        )
        .unwrap();

    assert_eq!(posix.getxattr("/report.txt", &owner_attr).unwrap(), b"docs");
    let stat = posix.getattr("/report.txt").unwrap();
    assert_eq!(
        stat.custom_attrs.get("owner").map(String::as_str),
        Some("docs")
    );
    assert_eq!(
        stat.custom_attrs.get("reviewer").map(String::as_str),
        Some("legal")
    );
    assert_eq!(
        posix.listxattr("/report.txt").unwrap(),
        [owner_attr.clone(), reviewer_attr.clone()]
    );

    posix.removexattr("/report.txt", &owner_attr).unwrap();
    assert!(matches!(
        posix.getxattr("/report.txt", &owner_attr),
        Err(VfsError::NotFound { .. })
    ));
    assert!(
        !posix
            .getattr("/report.txt")
            .unwrap()
            .custom_attrs
            .contains_key("owner")
    );
    assert_eq!(posix.listxattr("/report.txt").unwrap(), [reviewer_attr]);

    posix
        .setxattr(
            "/report.txt",
            STRATUM_MIME_XATTR,
            b"text/plain",
            PosixXattrSetMode::Upsert,
        )
        .unwrap();
    posix
        .removexattr("/report.txt", STRATUM_MIME_XATTR)
        .unwrap();
    assert!(matches!(
        posix.getxattr("/report.txt", STRATUM_MIME_XATTR),
        Err(VfsError::NotFound { .. })
    ));
}

#[test]
fn test_posix_xattr_create_and_replace_modes() {
    let mut fs = VirtualFs::new_posix();
    let root = Session::root();
    let mut posix = PosixFs::new(&mut fs, &root);
    let owner_attr = format!("{STRATUM_CUSTOM_XATTR_PREFIX}owner");

    let _fh = posix.create("/draft.txt", 0o644).unwrap();
    assert!(matches!(
        posix.setxattr(
            "/draft.txt",
            &owner_attr,
            b"docs",
            PosixXattrSetMode::ReplaceOnly,
        ),
        Err(VfsError::NotFound { .. })
    ));

    posix
        .setxattr(
            "/draft.txt",
            &owner_attr,
            b"docs",
            PosixXattrSetMode::CreateOnly,
        )
        .unwrap();
    assert!(matches!(
        posix.setxattr(
            "/draft.txt",
            &owner_attr,
            b"legal",
            PosixXattrSetMode::CreateOnly,
        ),
        Err(VfsError::AlreadyExists { .. })
    ));

    posix
        .setxattr(
            "/draft.txt",
            &owner_attr,
            b"legal",
            PosixXattrSetMode::ReplaceOnly,
        )
        .unwrap();
    assert_eq!(posix.getxattr("/draft.txt", &owner_attr).unwrap(), b"legal");
}

#[test]
fn test_posix_xattr_validation_rejects_invalid_values_and_names() {
    let mut fs = VirtualFs::new_posix();
    let root = Session::root();
    let mut posix = PosixFs::new(&mut fs, &root);
    let invalid_utf8 = [0xff, 0xfe];
    let owner_attr = format!("{STRATUM_CUSTOM_XATTR_PREFIX}owner");
    let empty_custom_attr = STRATUM_CUSTOM_XATTR_PREFIX.to_string();

    let _fh = posix.create("/bad.txt", 0o644).unwrap();

    assert!(matches!(
        posix.setxattr(
            "/bad.txt",
            &owner_attr,
            &invalid_utf8,
            PosixXattrSetMode::Upsert,
        ),
        Err(VfsError::InvalidArgs { .. })
    ));
    for mode in [
        PosixXattrSetMode::Upsert,
        PosixXattrSetMode::CreateOnly,
        PosixXattrSetMode::ReplaceOnly,
    ] {
        assert!(matches!(
            posix.setxattr("/bad.txt", &empty_custom_attr, b"value", mode),
            Err(VfsError::InvalidArgs { .. })
        ));
    }
    assert!(matches!(
        posix.getxattr("/bad.txt", &empty_custom_attr),
        Err(VfsError::InvalidArgs { .. })
    ));
    assert!(matches!(
        posix.removexattr("/bad.txt", &empty_custom_attr),
        Err(VfsError::InvalidArgs { .. })
    ));
    assert!(matches!(
        posix.setxattr(
            "/bad.txt",
            STRATUM_MIME_XATTR,
            b"not-a-mime",
            PosixXattrSetMode::Upsert,
        ),
        Err(VfsError::InvalidArgs { .. })
    ));
    assert!(matches!(
        posix.setxattr(
            "/bad.txt",
            "user.other.attr",
            b"value",
            PosixXattrSetMode::Upsert,
        ),
        Err(VfsError::NotSupported { .. })
    ));
    assert!(matches!(
        posix.getxattr("/bad.txt", "user.other.attr"),
        Err(VfsError::NotSupported { .. })
    ));
    assert!(matches!(
        posix.removexattr("/bad.txt", "user.other.attr"),
        Err(VfsError::NotSupported { .. })
    ));
}

#[test]
fn test_posix_xattr_symlink_uses_link_inode_metadata() {
    let mut fs = VirtualFs::new_posix();
    let root = Session::root();
    let mut posix = PosixFs::new(&mut fs, &root);
    let owner_attr = format!("{STRATUM_CUSTOM_XATTR_PREFIX}owner");

    let _fh = posix.create("/target.txt", 0o644).unwrap();
    posix
        .setxattr(
            "/target.txt",
            &owner_attr,
            b"target",
            PosixXattrSetMode::Upsert,
        )
        .unwrap();
    posix.symlink("/target.txt", "/link").unwrap();

    posix
        .setxattr("/link", &owner_attr, b"link", PosixXattrSetMode::Upsert)
        .unwrap();
    assert_eq!(posix.getxattr("/link", &owner_attr).unwrap(), b"link");
    assert_eq!(
        posix
            .getattr("/link")
            .unwrap()
            .custom_attrs
            .get("owner")
            .map(String::as_str),
        Some("link")
    );
    assert_eq!(
        posix
            .getattr("/target.txt")
            .unwrap()
            .custom_attrs
            .get("owner")
            .map(String::as_str),
        Some("target")
    );

    posix.removexattr("/link", &owner_attr).unwrap();
    assert!(matches!(
        posix.getxattr("/link", &owner_attr),
        Err(VfsError::NotFound { .. })
    ));
    assert_eq!(
        posix
            .getattr("/target.txt")
            .unwrap()
            .custom_attrs
            .get("owner")
            .map(String::as_str),
        Some("target")
    );
}

#[test]
fn test_posix_xattr_permissions_require_read_or_write() {
    let mut fs = VirtualFs::new_posix();
    let root = Session::root();
    let alice = Session::new(1000, 1000, vec![1000], "alice".to_string());
    let owner_attr = format!("{STRATUM_CUSTOM_XATTR_PREFIX}owner");

    {
        let mut posix = PosixFs::new(&mut fs, &root);
        let _fh = posix.create("/secret.txt", 0o600).unwrap();
        posix
            .setxattr(
                "/secret.txt",
                &owner_attr,
                b"root",
                PosixXattrSetMode::Upsert,
            )
            .unwrap();
    }

    {
        let posix = PosixFs::new(&mut fs, &alice);
        assert!(matches!(
            posix.getxattr("/secret.txt", &owner_attr),
            Err(VfsError::PermissionDenied { .. })
        ));
        assert!(matches!(
            posix.listxattr("/secret.txt"),
            Err(VfsError::PermissionDenied { .. })
        ));
    }

    {
        let mut posix = PosixFs::new(&mut fs, &root);
        posix
            .setattr(
                "/secret.txt",
                PosixSetAttr {
                    mode: Some(0o644),
                    ..Default::default()
                },
            )
            .unwrap();
    }

    {
        let mut posix = PosixFs::new(&mut fs, &alice);
        assert!(matches!(
            posix.setxattr(
                "/secret.txt",
                &owner_attr,
                b"alice",
                PosixXattrSetMode::Upsert,
            ),
            Err(VfsError::PermissionDenied { .. })
        ));
        assert!(matches!(
            posix.removexattr("/secret.txt", &owner_attr),
            Err(VfsError::PermissionDenied { .. })
        ));
    }
}
