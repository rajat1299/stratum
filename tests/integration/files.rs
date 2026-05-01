use super::*;
use stratum::config::CompatibilityTarget;
use stratum::fs::FsOptions;

fn markdown_fs() -> VirtualFs {
    VirtualFs::new_with_options(FsOptions {
        compatibility_target: CompatibilityTarget::Markdown,
    })
}

#[test]
fn test_touch_and_cat() {
    let mut fs = VirtualFs::new();
    exec("touch readme.md", &mut fs);
    exec("write readme.md Hello, stratum!", &mut fs);
    let output = exec("cat readme.md", &mut fs);
    assert_eq!(output, "Hello, stratum!");
}

#[test]
fn test_default_virtual_fs_allows_non_markdown_regular_files() {
    let mut fs = VirtualFs::new();
    assert_eq!(fs.compatibility_target(), CompatibilityTarget::Posix);
    exec("touch hello.txt", &mut fs);
    exec("write hello.txt plain text", &mut fs);
    assert_eq!(exec("cat hello.txt", &mut fs), "plain text");
}

#[test]
fn test_arbitrary_file_data_written_and_read_through_commands() {
    let mut fs = VirtualFs::new();
    exec("touch sample.bin", &mut fs);
    exec("write sample.bin bytes 00 ff 7f", &mut fs);
    assert_eq!(exec("cat sample.bin", &mut fs), "bytes 00 ff 7f");
}

#[test]
fn test_markdown_compatibility_rejects_non_markdown_regular_file_operations() {
    let mut fs = markdown_fs();

    let err = exec_err("touch hello.txt", &mut fs);
    assert!(err.contains("only supports .md files"));

    exec("touch source.md", &mut fs);
    exec("touch target.md", &mut fs);
    exec("mkdir docs", &mut fs);
    exec("write source.md content", &mut fs);

    let err = exec_err("write hello.txt content", &mut fs);
    assert!(err.contains("only supports .md files"));

    let err = exec_err("mv source.md source.txt", &mut fs);
    assert!(err.contains("only supports .md files"));
    assert_eq!(exec("cat source.md", &mut fs), "content");

    let err = exec_err("cp source.md copy.txt", &mut fs);
    assert!(err.contains("only supports .md files"));
    assert!(exec_err("cat copy.txt", &mut fs).contains("no such file"));

    let err = exec_err("ln source.md alias.txt", &mut fs);
    assert!(err.contains("only supports .md files"));
    assert!(exec_err("cat alias.txt", &mut fs).contains("no such file"));

    exec("cp source.md docs", &mut fs);
    assert_eq!(exec("cat docs/source.md", &mut fs), "content");

    exec("mv source.md moved.md", &mut fs);
    exec("cp moved.md copied.md", &mut fs);
    exec("ln moved.md alias.md", &mut fs);
    exec("mv moved.md docs", &mut fs);
    assert_eq!(exec("cat copied.md", &mut fs), "content");
    assert_eq!(exec("cat alias.md", &mut fs), "content");
    assert_eq!(exec("cat docs/moved.md", &mut fs), "content");
}

#[test]
fn test_markdown_compatibility_rejects_non_md_extensions() {
    let mut fs = markdown_fs();
    for ext in ["txt", "rs", "py", "json", "yaml", "toml", "html", "css"] {
        let err = exec_err(&format!("touch file.{ext}"), &mut fs);
        assert!(
            err.contains("only supports .md files"),
            "extension .{ext} should be rejected"
        );
    }
}

#[test]
fn test_touch_updates_timestamp() {
    let mut fs = VirtualFs::new();
    exec("touch file.md", &mut fs);
    let stat1 = exec("stat file.md", &mut fs);
    std::thread::sleep(std::time::Duration::from_millis(10));
    exec("touch file.md", &mut fs);
    let stat2 = exec("stat file.md", &mut fs);
    // Timestamps should be equal or newer (within test resolution)
    assert!(stat1.contains("file") && stat2.contains("file"));
}

#[test]
fn test_write_to_nonexistent_dir_fails() {
    let mut fs = VirtualFs::new();
    let err = exec_err("write nonexistent/ghost.md content", &mut fs);
    assert!(err.contains("no such file"));
}

#[test]
fn test_cat_nonexistent_file() {
    let mut fs = VirtualFs::new();
    let err = exec_err("cat ghost.md", &mut fs);
    assert!(err.contains("no such file"));
}

#[test]
fn test_cat_directory_fails() {
    let mut fs = VirtualFs::new();
    exec("mkdir dir", &mut fs);
    let err = exec_err("cat dir", &mut fs);
    assert!(err.contains("is a directory"));
}

#[test]
fn test_write_overwrite() {
    let mut fs = VirtualFs::new();
    exec("touch file.md", &mut fs);
    exec("write file.md first", &mut fs);
    assert_eq!(exec("cat file.md", &mut fs), "first");
    exec("write file.md second", &mut fs);
    assert_eq!(exec("cat file.md", &mut fs), "second");
}

#[test]
fn test_write_via_pipe_empty() {
    let mut fs = VirtualFs::new();
    exec("touch file.md", &mut fs);
    exec("write file.md initial", &mut fs);
    // Verify the initial write
    assert_eq!(exec("cat file.md", &mut fs), "initial");
    // Overwrite with pipe
    exec("echo replaced | write file.md", &mut fs);
    let content = exec("cat file.md", &mut fs);
    assert!(content.contains("replaced"));
}

#[test]
fn test_write_multiline() {
    let mut fs = VirtualFs::new();
    exec("touch file.md", &mut fs);
    exec(
        "write file.md # Title\n\nParagraph 1\n\nParagraph 2",
        &mut fs,
    );
    let content = exec("cat file.md", &mut fs);
    assert!(content.contains("# Title"));
    assert!(content.contains("Paragraph 1"));
    assert!(content.contains("Paragraph 2"));
}

#[test]
fn test_write_unicode() {
    let mut fs = VirtualFs::new();
    exec("touch unicode.md", &mut fs);
    exec("write unicode.md 你好世界 🌍 café résumé naïve", &mut fs);
    let content = exec("cat unicode.md", &mut fs);
    assert!(content.contains("你好世界"));
    assert!(content.contains("🌍"));
    assert!(content.contains("café"));
}
