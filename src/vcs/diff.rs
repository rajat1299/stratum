use crate::error::VfsError;
use crate::fs::inode::InodeKind;
use crate::fs::VirtualFs;
use crate::store::blob::BlobStore;
use crate::store::ObjectKind;
use crate::vcs::change::{ChangedPath, PathKind, PathMap, PathRecord};

const MAX_TEXT_DIFF_BYTES: usize = 512 * 1024;
const MAX_TEXT_DIFF_CELLS: usize = 4_000_000;

pub(crate) fn render_worktree_diff(
    store: &BlobStore,
    fs: &VirtualFs,
    before: &PathMap,
    after: &PathMap,
    changes: &[ChangedPath],
    path: Option<&str>,
) -> Result<String, VfsError> {
    let filter = path.map(normalize_path);
    let mut output = String::new();

    for change in changes {
        if !matches_path(&change.path, filter.as_deref()) {
            continue;
        }

        let before_record = before.get(&change.path);
        let after_record = after.get(&change.path);
        let before_kind = before_record.map(|record| record.kind);
        let after_kind = after_record.map(|record| record.kind);

        let is_text_file_change = matches!(
            (before_kind, after_kind),
            (None, Some(PathKind::File))
                | (Some(PathKind::File), None)
                | (Some(PathKind::File), Some(PathKind::File))
        );
        if !is_text_file_change {
            output.push_str(&format!("diff -- {}\n", change.path));
            output.push_str("Non-file changes are not supported by text diff.\n");
            continue;
        }

        let before_content = committed_content(store, before_record)?;
        let after_content = worktree_content(fs, after_record)?;

        if before_content.len().saturating_add(after_content.len()) > MAX_TEXT_DIFF_BYTES {
            output.push_str(&too_large_message(&change.path));
            continue;
        }

        if !is_probably_text(&before_content) || !is_probably_text(&after_content) {
            output.push_str(&binary_message(&change.path));
            continue;
        }

        let before_text = match String::from_utf8(before_content) {
            Ok(text) => text,
            Err(_) => {
                output.push_str(&binary_message(&change.path));
                continue;
            }
        };
        let after_text = match String::from_utf8(after_content) {
            Ok(text) => text,
            Err(_) => {
                output.push_str(&binary_message(&change.path));
                continue;
            }
        };

        output.push_str(&render_text_diff(&change.path, &before_text, &after_text));
    }

    if output.is_empty() {
        output.push_str("No changes.\n");
    }

    Ok(output)
}

pub(crate) fn render_text_diff(path: &str, before: &str, after: &str) -> String {
    if before == after {
        return String::new();
    }

    let before_lines = before.lines().collect::<Vec<_>>();
    let after_lines = after.lines().collect::<Vec<_>>();
    if before_lines.len().saturating_mul(after_lines.len()) > MAX_TEXT_DIFF_CELLS {
        return too_large_message(path);
    }

    let ops = line_diff(&before_lines, &after_lines);

    let mut output = String::new();
    output.push_str(&format!("diff -- {path}\n"));
    output.push_str(&format!("--- a{path}\n"));
    output.push_str(&format!("+++ b{path}\n"));
    output.push_str("@@\n");
    for op in ops {
        match op {
            DiffOp::Equal(line) => output.push_str(&format!(" {line}\n")),
            DiffOp::Remove(line) => output.push_str(&format!("-{line}\n")),
            DiffOp::Add(line) => output.push_str(&format!("+{line}\n")),
        }
    }
    output
}

fn committed_content(
    store: &BlobStore,
    record: Option<&PathRecord>,
) -> Result<Vec<u8>, VfsError> {
    match record {
        Some(record) => {
            let content_id = record.content_id.ok_or_else(|| VfsError::CorruptStore {
                message: format!("missing content id for {}", record.path),
            })?;
            Ok(store.get_typed(&content_id, ObjectKind::Blob)?.to_vec())
        }
        None => Ok(Vec::new()),
    }
}

fn worktree_content(fs: &VirtualFs, record: Option<&PathRecord>) -> Result<Vec<u8>, VfsError> {
    match record {
        Some(record) => {
            let inode_id = fs.resolve_path(&record.path)?;
            let inode = fs.get_inode(inode_id)?;
            match &inode.kind {
                InodeKind::File { content } => Ok(content.clone()),
                _ => Err(VfsError::CorruptStore {
                    message: format!("status record {} is not a file", record.path),
                }),
            }
        }
        None => Ok(Vec::new()),
    }
}

fn normalize_path(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

fn matches_path(path: &str, filter: Option<&str>) -> bool {
    match filter {
        Some("/") => true,
        Some(filter) => path == filter || path.starts_with(&format!("{filter}/")),
        None => true,
    }
}

fn binary_message(path: &str) -> String {
    format!("diff -- {path}\nBinary or non-UTF-8 content is not supported by text diff.\n")
}

fn too_large_message(path: &str) -> String {
    format!("diff -- {path}\nText diff is too large to render.\n")
}

fn is_probably_text(bytes: &[u8]) -> bool {
    bytes.iter().all(|byte| {
        matches!(
            *byte,
            b'\n' | b'\r' | b'\t' | 0x20..=0x7e | 0x80..=0xff
        )
    })
}

#[derive(Debug, PartialEq, Eq)]
enum DiffOp<'a> {
    Equal(&'a str),
    Remove(&'a str),
    Add(&'a str),
}

fn line_diff<'a>(before: &[&'a str], after: &[&'a str]) -> Vec<DiffOp<'a>> {
    let mut lcs = vec![vec![0usize; after.len() + 1]; before.len() + 1];
    for i in (0..before.len()).rev() {
        for j in (0..after.len()).rev() {
            lcs[i][j] = if before[i] == after[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }

    let mut ops = Vec::new();
    let mut i = 0;
    let mut j = 0;
    while i < before.len() && j < after.len() {
        if before[i] == after[j] {
            ops.push(DiffOp::Equal(before[i]));
            i += 1;
            j += 1;
        } else if lcs[i + 1][j] >= lcs[i][j + 1] {
            ops.push(DiffOp::Remove(before[i]));
            i += 1;
        } else {
            ops.push(DiffOp::Add(after[j]));
            j += 1;
        }
    }
    while i < before.len() {
        ops.push(DiffOp::Remove(before[i]));
        i += 1;
    }
    while j < after.len() {
        ops.push(DiffOp::Add(after[j]));
        j += 1;
    }

    ops
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lcs_diff_reports_replaced_line() {
        let diff = render_text_diff("/a.md", "one\ntwo\nthree\n", "one\n2\nthree\n");

        assert!(diff.contains(" one\n"));
        assert!(diff.contains("-two\n"));
        assert!(diff.contains("+2\n"));
        assert!(diff.contains(" three\n"));
    }

    #[test]
    fn lcs_diff_rejects_too_many_cells() {
        let before = (0..=2000).map(|i| format!("old {i}\n")).collect::<String>();
        let after = (0..=2000).map(|i| format!("new {i}\n")).collect::<String>();

        let diff = render_text_diff("/large.md", &before, &after);

        assert!(diff.contains("diff -- /large.md"));
        assert!(diff.contains("Text diff is too large to render"));
    }

    #[test]
    fn text_heuristic_rejects_control_bytes() {
        assert!(!is_probably_text(b"hello\0world"));
        assert!(!is_probably_text(b"hello\x01world"));
        assert!(is_probably_text(b"hello\tworld\n"));
    }
}
