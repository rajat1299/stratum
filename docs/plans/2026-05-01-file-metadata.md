# File Metadata Foundation

Date: 2026-05-01
Branch: v2/foundation

## Goal

Complete the Phase 1 file metadata gap for real user workflows:

- expose MIME type, size, content hash, and custom attrs through stat
- persist MIME/custom attrs on inodes
- keep content hashes fresh without a cached-hash invalidation burden
- make custom attrs writable through the HTTP API with authorization, idempotency, and audit
- preserve metadata through copy/move/link, local persistence, VCS commit/status/revert

## Product Contract

`GET /fs/{path}?stat=true` remains the read surface and adds:

- `mime_type`: string or null
- `content_hash`: `sha256:<hex>` for regular files, null for directories/symlinks
- `custom_attrs`: string map

`PATCH /fs/{path}` updates metadata only:

```json
{
  "mime_type": "text/markdown",
  "custom_attrs": {"owner": "docs"},
  "remove_custom_attrs": ["old-key"]
}
```

The route requires write scope/write permission, does not create files, updates `changed`/ctime but not file content, records audit, and supports `Idempotency-Key`.

`PUT /fs/{path}` may set MIME with `X-Stratum-Mime-Type`. Existing files keep their MIME when the header is absent.

Raw `GET /fs/{path}` returns the stored MIME as `Content-Type`, defaulting to `application/octet-stream`.

## Implementation Notes

- Add `mime_type: Option<String>` and `custom_attrs: BTreeMap<String, String>` to `Inode` with serde defaults.
- Compute `content_hash` on demand in `VirtualFs::stat`; do not persist a cached hash.
- Add bounded metadata validation: MIME length and syntax, attr key/value/count/total limits.
- Add DB methods for metadata update with the same write checks as content writes, without auto-creating paths.
- Add metadata fields to VCS tree/path records and include MIME/custom attrs in metadata-change detection.
- Bump local persisted state version so older binaries do not silently drop metadata.

## Verification Plan

- Unit/integration tests for stat hash freshness across write, truncate, and handle writes.
- Tests for metadata persistence, copy/move/link behavior, and VCS status/revert.
- HTTP tests for stat JSON, raw GET content type, PUT MIME, PATCH metadata, permissions, idempotency, and audit.
- Run fmt, focused tests, clippy, full locked tests, audit, and diff whitespace check before completion.

## Deferred

- FUSE xattr get/set/list/remove support.
- MIME inference/sniffing beyond explicit user-provided MIME.
- Secret-aware idempotency storage for endpoints returning raw secrets.
