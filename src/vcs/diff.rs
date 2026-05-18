use crate::backend::{ObjectStore, RepoId};
use crate::error::VfsError;
use crate::fs::VirtualFs;
use crate::fs::inode::InodeKind;
use crate::store::ObjectKind;
use crate::store::blob::BlobStore;
use crate::vcs::change::{ChangeKind, ChangedPath, PathKind, PathMap, PathRecord};

const MAX_TEXT_DIFF_BYTES: usize = 512 * 1024;
const MAX_TEXT_DIFF_CELLS: usize = 4_000_000;
const DIFF_CONTEXT_LINES: usize = 3;
const DURABLE_DIFF_READ_FAILED: &str = "durable diff read failed";

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
        if change.kind == ChangeKind::MetadataChanged {
            output.push_str(&render_metadata_diff(
                &change.path,
                before_record,
                after_record,
            ));
            continue;
        }

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

pub(crate) fn render_committed_diff(
    store: &BlobStore,
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
        if change.kind == ChangeKind::MetadataChanged {
            output.push_str(&render_metadata_diff(
                &change.path,
                before_record,
                after_record,
            ));
            continue;
        }

        let before_kind = before_record.map(|record| record.kind);
        let after_kind = after_record.map(|record| record.kind);
        if !is_text_file_change(before_kind, after_kind) {
            output.push_str(&format!("diff -- {}\n", change.path));
            output.push_str("Non-file changes are not supported by text diff.\n");
            continue;
        }

        let before_content = committed_content(store, before_record)?;
        let after_content = committed_content(store, after_record)?;

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

pub(crate) async fn render_durable_diff(
    repo_id: &RepoId,
    objects: &dyn ObjectStore,
    changes: &[ChangedPath],
    path: Option<&str>,
) -> Result<String, VfsError> {
    let filter = path.map(normalize_path);
    let mut output = String::new();

    for change in changes {
        if !matches_path(&change.path, filter.as_deref()) {
            continue;
        }

        let before_record = change.before.as_ref();
        let after_record = change.after.as_ref();
        if change.kind == ChangeKind::MetadataChanged {
            output.push_str(&render_metadata_diff(
                &change.path,
                before_record,
                after_record,
            ));
            continue;
        }

        let before_kind = before_record.map(|record| record.kind);
        let after_kind = after_record.map(|record| record.kind);
        if !is_text_file_change(before_kind, after_kind) {
            output.push_str(&render_content_summary(
                &change.path,
                before_record,
                after_record,
                "path kind changed; text diff is not available",
            ));
            continue;
        }

        if selected_content_size(before_record, after_record) > MAX_TEXT_DIFF_BYTES as u64 {
            output.push_str(&render_content_summary(
                &change.path,
                before_record,
                after_record,
                "text diff is too large to render",
            ));
            continue;
        }

        if has_binary_mime(before_record) || has_binary_mime(after_record) {
            output.push_str(&render_content_summary(
                &change.path,
                before_record,
                after_record,
                "binary or non-UTF-8 content is not supported by text diff",
            ));
            continue;
        }

        let before_content = durable_content(repo_id, objects, before_record).await?;
        let after_content = durable_content(repo_id, objects, after_record).await?;

        if !is_probably_text(&before_content) || !is_probably_text(&after_content) {
            output.push_str(&render_content_summary(
                &change.path,
                before_record,
                after_record,
                "binary or non-UTF-8 content is not supported by text diff",
            ));
            continue;
        }

        let before_text = match String::from_utf8(before_content) {
            Ok(text) => text,
            Err(_) => {
                output.push_str(&render_content_summary(
                    &change.path,
                    before_record,
                    after_record,
                    "binary or non-UTF-8 content is not supported by text diff",
                ));
                continue;
            }
        };
        let after_text = match String::from_utf8(after_content) {
            Ok(text) => text,
            Err(_) => {
                output.push_str(&render_content_summary(
                    &change.path,
                    before_record,
                    after_record,
                    "binary or non-UTF-8 content is not supported by text diff",
                ));
                continue;
            }
        };

        output.push_str(&render_grouped_text_diff(
            &change.path,
            &before_text,
            &after_text,
        ));
    }

    if output.is_empty() {
        output.push_str("No changes.\n");
    }

    Ok(output)
}

fn render_metadata_diff(
    path: &str,
    before_record: Option<&PathRecord>,
    after_record: Option<&PathRecord>,
) -> String {
    let mut output = String::new();
    output.push_str(&format!("diff -- {path}\n"));
    output.push_str("metadata:\n");

    let (Some(before), Some(after)) = (before_record, after_record) else {
        output.push_str("Metadata record missing for metadata-only change.\n");
        return output;
    };
    if before.mode != after.mode {
        output.push_str(&format!("- mode: {:04o}\n", before.mode));
        output.push_str(&format!("+ mode: {:04o}\n", after.mode));
    }
    if before.uid != after.uid {
        output.push_str(&format!("- uid: {}\n", before.uid));
        output.push_str(&format!("+ uid: {}\n", after.uid));
    }
    if before.gid != after.gid {
        output.push_str(&format!("- gid: {}\n", before.gid));
        output.push_str(&format!("+ gid: {}\n", after.gid));
    }
    if before.mime_type != after.mime_type {
        output.push_str(&format!(
            "- mime_type: {}\n",
            metadata_value(before.mime_type.as_deref())
        ));
        output.push_str(&format!(
            "+ mime_type: {}\n",
            metadata_value(after.mime_type.as_deref())
        ));
    }

    for key in before
        .custom_attrs
        .keys()
        .chain(after.custom_attrs.keys())
        .collect::<std::collections::BTreeSet<_>>()
    {
        let before_value = before.custom_attrs.get(key).map(String::as_str);
        let after_value = after.custom_attrs.get(key).map(String::as_str);
        if before_value != after_value {
            output.push_str(&format!(
                "- custom_attrs.{key}: {}\n",
                metadata_value(before_value)
            ));
            output.push_str(&format!(
                "+ custom_attrs.{key}: {}\n",
                metadata_value(after_value)
            ));
        }
    }

    output
}

fn metadata_value(value: Option<&str>) -> &str {
    value.unwrap_or("<unset>")
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

fn render_grouped_text_diff(path: &str, before: &str, after: &str) -> String {
    if before == after {
        return String::new();
    }

    let before_lines = before.lines().collect::<Vec<_>>();
    let after_lines = after.lines().collect::<Vec<_>>();
    if before_lines.len().saturating_mul(after_lines.len()) > MAX_TEXT_DIFF_CELLS {
        return too_large_message(path);
    }

    let ops = line_diff(&before_lines, &after_lines);
    let hunks = grouped_hunks(&ops);

    let mut output = String::new();
    output.push_str(&format!("diff -- {path}\n"));
    output.push_str(&format!("--- a{path}\n"));
    output.push_str(&format!("+++ b{path}\n"));
    for hunk in hunks {
        output.push_str(&format!(
            "@@ -{},{} +{},{} @@\n",
            hunk.before_start, hunk.before_count, hunk.after_start, hunk.after_count
        ));
        for op in &ops[hunk.start..hunk.end] {
            match op {
                DiffOp::Equal(line) => output.push_str(&format!(" {line}\n")),
                DiffOp::Remove(line) => output.push_str(&format!("-{line}\n")),
                DiffOp::Add(line) => output.push_str(&format!("+{line}\n")),
            }
        }
    }
    output
}

fn committed_content(store: &BlobStore, record: Option<&PathRecord>) -> Result<Vec<u8>, VfsError> {
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

async fn durable_content(
    repo_id: &RepoId,
    objects: &dyn ObjectStore,
    record: Option<&PathRecord>,
) -> Result<Vec<u8>, VfsError> {
    let Some(record) = record else {
        return Ok(Vec::new());
    };
    let content_id = record.content_id.ok_or_else(durable_diff_read_failed)?;
    let stored = objects
        .get(repo_id, content_id, ObjectKind::Blob)
        .await
        .map_err(|_| durable_diff_read_failed())?
        .ok_or_else(durable_diff_read_failed)?;
    if stored.repo_id != *repo_id || stored.id != content_id || stored.kind != ObjectKind::Blob {
        return Err(durable_diff_read_failed());
    }
    Ok(stored.bytes)
}

fn is_text_file_change(before_kind: Option<PathKind>, after_kind: Option<PathKind>) -> bool {
    matches!(
        (before_kind, after_kind),
        (None, Some(PathKind::File))
            | (Some(PathKind::File), None)
            | (Some(PathKind::File), Some(PathKind::File))
    )
}

fn selected_content_size(
    before_record: Option<&PathRecord>,
    after_record: Option<&PathRecord>,
) -> u64 {
    before_record
        .map(|record| record.size)
        .unwrap_or_default()
        .saturating_add(after_record.map(|record| record.size).unwrap_or_default())
}

fn has_binary_mime(record: Option<&PathRecord>) -> bool {
    record
        .and_then(|record| record.mime_type.as_deref())
        .is_some_and(|mime_type| !is_textual_mime(mime_type))
}

fn is_textual_mime(mime_type: &str) -> bool {
    let mime_type = mime_type
        .split_once(';')
        .map(|(mime_type, _)| mime_type)
        .unwrap_or(mime_type)
        .trim()
        .to_ascii_lowercase();
    mime_type.starts_with("text/")
        || matches!(
            mime_type.as_str(),
            "application/json"
                | "application/javascript"
                | "application/xml"
                | "application/x-yaml"
                | "application/toml"
        )
        || mime_type.ends_with("+json")
        || mime_type.ends_with("+xml")
}

fn render_content_summary(
    path: &str,
    before_record: Option<&PathRecord>,
    after_record: Option<&PathRecord>,
    reason: &str,
) -> String {
    let mut output = String::new();
    output.push_str(&format!("diff -- {path}\n"));
    output.push_str(&format!("reason: {reason}\n"));
    output.push_str(&format!("before: {}\n", record_summary(before_record)));
    output.push_str(&format!("after: {}\n", record_summary(after_record)));
    output
}

fn record_summary(record: Option<&PathRecord>) -> String {
    match record {
        Some(record) => format!(
            "object={} size={} type={} mime={}",
            record
                .content_id
                .map(|id| id.to_hex())
                .unwrap_or_else(|| "<none>".to_string()),
            record.size,
            path_kind_summary(record.kind),
            metadata_value(record.mime_type.as_deref())
        ),
        None => "object=<none> size=0 type=absent mime=<unset>".to_string(),
    }
}

fn path_kind_summary(kind: PathKind) -> &'static str {
    match kind {
        PathKind::File => "file",
        PathKind::Directory => "directory",
        PathKind::Symlink => "symlink",
    }
}

fn durable_diff_read_failed() -> VfsError {
    VfsError::CorruptStore {
        message: DURABLE_DIFF_READ_FAILED.to_string(),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiffOp<'a> {
    Equal(&'a str),
    Remove(&'a str),
    Add(&'a str),
}

struct DiffHunk {
    start: usize,
    end: usize,
    before_start: usize,
    before_count: usize,
    after_start: usize,
    after_count: usize,
}

#[derive(Clone, Copy)]
struct AnnotatedDiffOp {
    before_pos: usize,
    after_pos: usize,
    consumes_before: bool,
    consumes_after: bool,
    is_change: bool,
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

fn grouped_hunks(ops: &[DiffOp<'_>]) -> Vec<DiffHunk> {
    let annotated = annotate_ops(ops);
    let change_indices = annotated
        .iter()
        .enumerate()
        .filter_map(|(index, op)| op.is_change.then_some(index))
        .collect::<Vec<_>>();
    let Some((&first_change, rest)) = change_indices.split_first() else {
        return Vec::new();
    };

    let mut hunks = Vec::new();
    let mut hunk_start = context_start(ops, first_change);
    let mut last_change = first_change;
    for &change_index in rest {
        let equal_between = ops[last_change + 1..change_index]
            .iter()
            .filter(|op| matches!(op, DiffOp::Equal(_)))
            .count();
        if equal_between > DIFF_CONTEXT_LINES * 2 {
            let hunk_end = context_end(ops, last_change);
            hunks.push(build_hunk(&annotated, hunk_start, hunk_end));
            hunk_start = context_start(ops, change_index);
        }
        last_change = change_index;
    }
    let hunk_end = context_end(ops, last_change);
    hunks.push(build_hunk(&annotated, hunk_start, hunk_end));
    hunks
}

fn annotate_ops(ops: &[DiffOp<'_>]) -> Vec<AnnotatedDiffOp> {
    let mut before_pos = 1usize;
    let mut after_pos = 1usize;
    ops.iter()
        .map(|op| {
            let annotated = match op {
                DiffOp::Equal(_) => AnnotatedDiffOp {
                    before_pos,
                    after_pos,
                    consumes_before: true,
                    consumes_after: true,
                    is_change: false,
                },
                DiffOp::Remove(_) => AnnotatedDiffOp {
                    before_pos,
                    after_pos,
                    consumes_before: true,
                    consumes_after: false,
                    is_change: true,
                },
                DiffOp::Add(_) => AnnotatedDiffOp {
                    before_pos,
                    after_pos,
                    consumes_before: false,
                    consumes_after: true,
                    is_change: true,
                },
            };
            if annotated.consumes_before {
                before_pos += 1;
            }
            if annotated.consumes_after {
                after_pos += 1;
            }
            annotated
        })
        .collect()
}

fn context_start(ops: &[DiffOp<'_>], change_index: usize) -> usize {
    let mut start = change_index;
    let mut context = 0usize;
    while start > 0 && context < DIFF_CONTEXT_LINES {
        if !matches!(ops[start - 1], DiffOp::Equal(_)) {
            break;
        }
        start -= 1;
        context += 1;
    }
    start
}

fn context_end(ops: &[DiffOp<'_>], change_index: usize) -> usize {
    let mut end = change_index + 1;
    let mut context = 0usize;
    while end < ops.len() && context < DIFF_CONTEXT_LINES {
        if !matches!(ops[end], DiffOp::Equal(_)) {
            break;
        }
        end += 1;
        context += 1;
    }
    end
}

fn build_hunk(annotated: &[AnnotatedDiffOp], start: usize, end: usize) -> DiffHunk {
    let before_count = annotated[start..end]
        .iter()
        .filter(|op| op.consumes_before)
        .count();
    let after_count = annotated[start..end]
        .iter()
        .filter(|op| op.consumes_after)
        .count();
    DiffHunk {
        start,
        end,
        before_start: hunk_start_line(annotated[start].before_pos, before_count),
        before_count,
        after_start: hunk_start_line(annotated[start].after_pos, after_count),
        after_count,
    }
}

fn hunk_start_line(position: usize, count: usize) -> usize {
    if count == 0 {
        position.saturating_sub(1)
    } else {
        position
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_text_diff_preserves_legacy_local_header_and_full_equal_lines() {
        let diff = render_text_diff("/a.md", "one\ntwo\nthree\n", "one\n2\nthree\n");

        assert!(diff.contains("@@\n"));
        assert!(!diff.contains("@@ -"));
        assert!(diff.contains(" one\n"));
        assert!(diff.contains("-two\n"));
        assert!(diff.contains("+2\n"));
        assert!(diff.contains(" three\n"));
    }

    #[test]
    fn grouped_text_diff_uses_unified_hunk_header_and_trims_distant_equal_lines() {
        let before = (1..=12)
            .map(|line| {
                if line == 8 {
                    "before\n".to_string()
                } else {
                    format!("shared line {line:02}\n")
                }
            })
            .collect::<String>();
        let after = (1..=12)
            .map(|line| {
                if line == 8 {
                    "after\n".to_string()
                } else {
                    format!("shared line {line:02}\n")
                }
            })
            .collect::<String>();

        let diff = render_grouped_text_diff("/a.md", &before, &after);

        assert!(diff.contains("@@ -5,7 +5,7 @@\n"));
        assert!(diff.contains("-before\n"));
        assert!(diff.contains("+after\n"));
        assert!(!diff.contains(" shared line 01\n"));
        assert!(!diff.contains("@@\n---"));
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
