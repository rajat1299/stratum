# Semantic Index

## Postgres FTS MVP (shipped)

Durable-cloud exposes `GET /search/semantic` as a derived, fail-closed Postgres full-text search surface over committed durable state.

- Freshness boundary: exact `(repo_id, commit_id, root_tree_id)` for the current durable read head (`main` or mounted `session_ref`).
- Index rows store bounded `content_preview` from ready extracted text only (`plain-text-v1`, `markdown-v1`, `docx-v1`, `pdf-v1`). Raw docx/pdf bytes, unsupported binaries, corrupt inputs, and oversized sources are persisted as separate extraction records but are not indexed.
- Provider-free extractors run in-process with conservative caps (10 MiB source, 100k extracted chars, bounded docx zip/XML, bounded pdf pages/operators). No OCR, external binaries, or network fetches.
- `routes.search.semantic.available` is `true` only when the search index store is ready for the router's current `main` head and extraction metadata is `ready` with version `extracted-text-v1`.
- Missing, stale, failed, or schema-absent index state returns `503` (or `501` when the store is unavailable). Slice 20/21 indexes without extraction metadata remain `extraction_status = missing` and fail closed until reindexed. Durable-cloud does not fall back to `grep`, `find`, tree walks, or local `.vfs` state.
- Every indexed file row carries a `posix-tree-v1` ACL snapshot (root/ancestor execute plus file read requirements) and a content hash bound to repo/commit/root tree/path/object id.
- Search results are filtered against the caller session (read prefixes, mode/uid/gid bits, delegate intersection) before route rendering.
- Slice 20 indexes without ACL snapshots remain `acl_snapshot_status = missing` and fail closed until reindexed.
- Candidate hits are still rechecked with the same committed read permission logic as `GET /fs` before paths/snippets are returned.
- Query must be non-empty and at most 256 characters. `limit` defaults to `50` and must be between `1` and `1000`.
- Automatic/background index production is not part of this MVP. Until a durable head has been explicitly indexed, the route fails closed with `503`.

TypeScript and Python SDK clients call `search.semantic(query, options)` and surface server capability/HTTP status. `@stratum/bash` `sgrep` remains unsupported and points callers at server capabilities.

## pgvector Expansion

Slice 23 adds pgvector-backed ranking as an additive derived index. `GET /search/semantic` keeps the same request and response shape; when vector readiness is usable, scores come from vector distance, and when it is unavailable or failed the route falls back to the FTS behavior above.

- The default embedding provider is disabled. Tests and dev fixtures use an in-process deterministic provider that makes no network calls.
- Vector state is scoped by exact `(repo_id, commit_id, root_tree_id, embedding_provider, embedding_model, embedding_dimensions, chunker_version)`.
- Vector rows store no raw chunk text. They bind each chunk to `path`, `object_id`, `extracted_text_hash`, `acl_snapshot_hash`, `chunk_hash`, model identity, dimensions, and `semantic-chunk-v1`.
- Provider failures and malformed provider output mark only vector state failed with a redacted failure code. FTS/extraction/ACL readiness remains intact.
- Empty ready vector state is not treated as usable when the base FTS head has indexed files.
- Postgres vector search uses exact distance ordering after repo/head/path/extraction/ACL filters. ANN indexes are deliberately not part of this slice.
- The route still rechecks every candidate with committed file reads and object hash equality before rendering.

The vector layer accelerates retrieval, but `stratum` remains the source of truth for contents, permissions, commits, and rollback.
