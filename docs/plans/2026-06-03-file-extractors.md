# File Extractors Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add bounded extracted-text records for text, markdown, docx, and pdf files so search and VCS diff/status can use derived text without indexing or rendering raw binary bytes.

**Architecture:** Introduce a provider-free extraction domain and store, backed by an additive Postgres schema for durable-cloud. Commit indexing should extract each committed blob into explicit extraction metadata, persist ready/unsupported/failed records separately from raw object bytes, feed only ready extracted text into the existing FTS search index, and keep `posix-tree-v1` ACL snapshots plus final committed-read rechecks intact. Durable status/diff should consult extracted-text records for supported binary formats and render extracted-text hunks or explicit extraction metadata summaries instead of attempting raw binary text diffs.

**Tech Stack:** Rust 2024, Axum, Tokio, Postgres, existing durable object/commit/ref stores, existing `SearchIndexStore` and ACL snapshot code, pure Rust extraction dependencies only after review, TypeScript/Python/Bash SDK contract checks, provider-free tests, and optional live Postgres tests behind `STRATUM_POSTGRES_TEST_URL`.

---

## Current Inventory

- `src/backend/search_index.rs` currently walks durable tree/blob objects directly and indexes any blob that is valid UTF-8 into `IndexedFileRow.content_preview`, capped at 100,000 chars.
- `src/backend/search_index.rs` skips invalid UTF-8 blobs and symlinks. It has no file type awareness beyond tree entry metadata.
- `src/backend/search_index/acl.rs` builds and verifies `posix-tree-v1` snapshots over repo, commit, root tree, path, object id, root/ancestor execute requirements, and file read requirements. This must remain the permission boundary for search indexing.
- `src/backend/postgres.rs` implements `SearchIndexStore` with `search_index_state` and `search_index_files`, including Postgres JSONB ACL filtering before result limiting.
- `migrations/postgres/0019_postgres_fts_search_mvp.sql` stores FTS rows, bounded `content_preview`, and a `tsvector`.
- `migrations/postgres/0020_acl_snapshot_filtering.sql` adds ACL metadata and makes old Slice 20 indexes fail closed until reindexed.
- `src/server/routes_fs.rs::search_semantic` resolves durable head identity, calls `state.search_index.search(...)`, then rechecks every candidate with `state.core.cat_with_stat_as(&candidate.path, &session)` and object hash equality before rendering results.
- `src/vcs/diff.rs::render_durable_diff` currently renders grouped text hunks by reading raw blob bytes when MIME is textual and content is UTF-8. It renders summaries for binary, non-UTF-8, oversized, metadata-only, non-file, and type-changed paths.
- `src/server/core.rs::durable_vcs_status_as` and `durable_vcs_diff_as` compare durable path maps from `DurableCommittedFsReader`, then append source identity lines. Status has only path/status-code text today.
- `crates/stratum-core/src/change.rs::PathRecord` carries path, kind, mode, uid, gid, size, content id, MIME, and custom attrs. It should not become a carrier for extracted text.
- SDKs already expose `search.semantic()`, plain-text `status()`, and plain-text `diff()`. No public SDK method shape change is expected unless capability fixtures/docs change.

## Design Constraints

- Extracted text is derived data, never source of truth.
- Store extracted text separately from raw blob bytes. Do not store raw docx/pdf bytes in extraction/search tables.
- Search must use ready extracted text only. It must not fall back to raw binary bytes, local `.vfs`, `grep`, `find`, or tree walks.
- Existing ACL snapshot filtering and final `cat_with_stat_as` candidate recheck stay in place.
- Unsupported file types must produce explicit unsupported extraction metadata and stay unindexed.
- Corrupt/invalid docx/pdf inputs must fail as extraction metadata without leaking parser internals, raw bytes, temp paths, object keys, SQL, tokens, env vars, DB URLs, or backing paths.
- Extraction must be bounded: source byte caps, extracted text caps, zip entry/expanded byte caps for docx, page/operator caps for pdf, traversal caps, and redacted failure codes.
- Do not invoke external binaries such as `pdftotext`, LibreOffice, or Python helpers from the server. Keep extraction provider-free and deterministic.
- Avoid heavyweight dependencies unless the task justifies them and review confirms license/security/build behavior.

## Extraction Model

Use a first version named `extracted-text-v1`.

Record statuses:

- `ready`: supported file type, extraction succeeded, bounded extracted text is available.
- `unsupported`: file type is outside text/markdown/docx/pdf scope or MIME/extension is ambiguous and unsafe.
- `too_large`: source bytes or parser-expanded content exceeded configured caps.
- `failed`: supported file type but parsing failed or returned malformed data.

Extractor kinds:

- `plain-text-v1`: UTF-8 text files. Normalize CRLF/CR to LF; strip UTF-8 BOM.
- `markdown-v1`: UTF-8 markdown files. Preserve source markdown text after line normalization; do not render HTML.
- `docx-v1`: Office Open XML `.docx`. Parse only internal `word/document.xml` plus bounded headers/footers if included by the implementation. Extract visible `w:t` text, `w:tab`, and paragraph/table breaks. Do not follow external relationships.
- `pdf-v1`: PDF text extraction only. Extract text from content streams with bounded page/operator work. No OCR, image text, JavaScript, attachments, remote fetches, or font/network side effects.
- `unsupported-v1`: explicit unsupported metadata, no extracted text.

Suggested caps for the first pass:

- `MAX_EXTRACT_SOURCE_BYTES = 10 * 1024 * 1024`
- `MAX_EXTRACTED_TEXT_CHARS = 100_000`
- `MAX_DOCX_ZIP_ENTRIES = 512`
- `MAX_DOCX_EXPANDED_XML_BYTES = 8 * 1024 * 1024`
- `MAX_PDF_PAGES = 500`
- `MAX_PDF_TEXT_OPERATORS = 250_000`

These are deliberately conservative and can become capability limits later.

## Schema Shape

Add migration `0021_file_extractors.sql`.

Create `extracted_text_records`:

```sql
CREATE TABLE IF NOT EXISTS extracted_text_records (
    repo_id TEXT NOT NULL,
    commit_id TEXT NOT NULL CONSTRAINT extracted_text_records_commit_id_check CHECK (commit_id ~ '^[0-9a-f]{64}$'),
    root_tree_id TEXT NOT NULL CONSTRAINT extracted_text_records_root_tree_id_check CHECK (root_tree_id ~ '^[0-9a-f]{64}$'),
    path TEXT NOT NULL CONSTRAINT extracted_text_records_path_check CHECK (path <> '' AND path ~ '^/'),
    object_id TEXT NOT NULL CONSTRAINT extracted_text_records_object_id_check CHECK (object_id ~ '^[0-9a-f]{64}$'),
    source_byte_len BIGINT NOT NULL CONSTRAINT extracted_text_records_source_byte_len_check CHECK (source_byte_len >= 0),
    source_mime_type TEXT,
    extractor TEXT NOT NULL,
    status TEXT NOT NULL CONSTRAINT extracted_text_records_status_check CHECK (status IN ('ready', 'unsupported', 'too_large', 'failed')),
    text_hash TEXT CONSTRAINT extracted_text_records_text_hash_check CHECK (text_hash IS NULL OR text_hash ~ '^[0-9a-f]{64}$'),
    text_char_count INTEGER NOT NULL DEFAULT 0 CONSTRAINT extracted_text_records_text_char_count_check CHECK (text_char_count >= 0),
    extracted_text TEXT,
    failure_code TEXT CONSTRAINT extracted_text_records_failure_code_check CHECK (failure_code IS NULL OR failure_code <> ''),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_id, commit_id, root_tree_id, path),
    FOREIGN KEY (repo_id, commit_id) REFERENCES commits(repo_id, id) ON DELETE CASCADE,
    CONSTRAINT extracted_text_records_ready_shape_check CHECK (
        (status = 'ready' AND extracted_text IS NOT NULL AND text_hash IS NOT NULL AND failure_code IS NULL)
        OR (status <> 'ready' AND extracted_text IS NULL AND text_hash IS NULL AND failure_code IS NOT NULL)
    )
);
```

Add indexes:

- `extracted_text_records_object_lookup_idx` on `(repo_id, object_id)`.
- `extracted_text_records_head_lookup_idx` on `(repo_id, commit_id, root_tree_id)`.
- `extracted_text_records_path_lookup_idx` on `(repo_id, commit_id, root_tree_id, path)`.

Extend search readiness:

```sql
ALTER TABLE search_index_state
  ADD COLUMN IF NOT EXISTS extraction_version TEXT,
  ADD COLUMN IF NOT EXISTS extraction_status TEXT NOT NULL DEFAULT 'missing'
    CONSTRAINT search_index_state_extraction_status_check
    CHECK (extraction_status IN ('missing', 'ready', 'failed')),
  ADD COLUMN IF NOT EXISTS extraction_failure_code TEXT
    CONSTRAINT search_index_state_extraction_failure_code_check
    CHECK (extraction_failure_code IS NULL OR extraction_failure_code <> '');

ALTER TABLE search_index_files
  ADD COLUMN IF NOT EXISTS extraction_version TEXT,
  ADD COLUMN IF NOT EXISTS extractor TEXT,
  ADD COLUMN IF NOT EXISTS extracted_text_hash TEXT
    CONSTRAINT search_index_files_extracted_text_hash_check
    CHECK (extracted_text_hash IS NULL OR extracted_text_hash ~ '^[0-9a-f]{64}$');
```

Add lifecycle constraints so old Slice 20/21 indexes have `extraction_status = 'missing'` and fail closed until reindexed. A ready search index must have `extraction_status = 'ready'`, `extraction_version = 'extracted-text-v1'`, and every search row must have extractor metadata plus `extracted_text_hash`.

## Task 1: Migration And Store Contract

**Files:**

- Add: `migrations/postgres/0021_file_extractors.sql`
- Modify: `src/backend/postgres_migrations.rs`
- Add: `src/backend/text_extraction.rs`
- Modify: `src/backend/mod.rs`
- Modify: `src/backend/postgres.rs`

**Step 1: Write failing migration registration tests**

Add `file_extractors_migration_is_registered_and_non_destructive`.

Run:

```bash
cargo test --locked --features postgres backend::postgres_migrations::tests::file_extractors_migration_is_registered_and_non_destructive --lib -- --nocapture
```

Expected: FAIL because migration 21 is absent.

**Step 2: Add the additive migration**

Add migration 21, include it in `postgres_migrations.rs`, update `POSTGRES_MIGRATIONS` length to 21, and register it after `acl_snapshot_filtering`.

Do not alter raw blob/object tables.

**Step 3: Add adoption verification**

Extend `verify_known_schema_catalog` to require:

- `extracted_text_records` table, primary key, FK to `commits`, indexes, column types/defaults.
- status, hash, path, byte length, and ready-shape checks.
- `search_index_state` extraction columns and lifecycle checks.
- `search_index_files` extraction columns and hash checks.

Add negative tests for:

- missing extraction table,
- weakened ready-shape constraint,
- weakened `text_hash` or `object_id` checks,
- missing search-state extraction status default,
- search rows allowed to be extraction-ready without hashes,
- public adoption errors leaking table/column names.

**Step 4: Add the store trait skeleton**

In `src/backend/text_extraction.rs`, add:

```rust
pub const EXTRACTED_TEXT_VERSION_V1: &str = "extracted-text-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractedTextStatus {
    Ready,
    Unsupported,
    TooLarge,
    Failed,
}

#[derive(Debug, Clone)]
pub struct ExtractedTextRecord {
    pub path: String,
    pub object_id: ObjectId,
    pub source_byte_len: u64,
    pub source_mime_type: Option<String>,
    pub extractor: String,
    pub status: ExtractedTextStatus,
    pub text: Option<String>,
    pub text_hash: Option<String>,
    pub failure_code: Option<String>,
}

#[async_trait]
pub trait TextExtractionStore: Send + Sync {
    async fn ensure_available(&self) -> Result<(), VfsError>;
    async fn put_records(
        &self,
        head: SearchIndexHead,
        records: Vec<ExtractedTextRecord>,
    ) -> Result<(), VfsError>;
    async fn record_for_path(
        &self,
        head: &SearchIndexHead,
        path: &str,
    ) -> Result<Option<ExtractedTextRecord>, VfsError>;
    fn available(&self) -> bool;
}
```

Add `InMemoryTextExtractionStore` and `UnavailableTextExtractionStore`.

**Step 5: Wire shared stores**

Add `SharedTextExtractionStore = Arc<dyn TextExtractionStore>` to `src/backend/mod.rs`.

Add `text_extraction` to:

- `StratumStores`,
- `ServerStores` / `ServerState` in `src/server/mod.rs`,
- durable Postgres store opening,
- local/in-memory test builders.

Default local runtime should use `UnavailableTextExtractionStore` unless a focused test injects in-memory extraction.

**Step 6: Verify**

```bash
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo check --locked
cargo check --locked --features postgres
```

Expected: PASS, with live Postgres portions skipped unless `STRATUM_POSTGRES_TEST_URL` is set.

Commit:

```bash
git add migrations/postgres/0021_file_extractors.sql src/backend/postgres_migrations.rs src/backend/text_extraction.rs src/backend/mod.rs src/backend/postgres.rs src/server/mod.rs
git commit -m "migration: add extracted text records"
```

## Task 2: Provider-Free Extractors

**Files:**

- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/backend/text_extraction.rs`
- Add test fixtures inline in `src/backend/text_extraction.rs` tests unless fixture size becomes unwieldy

**Dependency rule:**

Prefer pure Rust dependencies with bounded parsing behavior. Expected candidates are a ZIP reader and XML pull parser for docx, and a PDF parser for pdf text streams. Before committing, review dependency count, license, default features, native build requirements, and known unsafe/native surfaces. Disable default features where practical.

**Step 1: Write failing text and markdown tests**

Cover:

- `.txt` / `text/plain` UTF-8 extracts as `plain-text-v1`.
- `.md` / `text/markdown` extracts as `markdown-v1`.
- CRLF and CR normalize to LF.
- UTF-8 BOM is removed.
- invalid UTF-8 text file returns `failed` with a bounded failure code.
- oversized text returns `too_large` with no `extracted_text`.
- debug/display output does not include extracted text containing `secret-token`.

Run:

```bash
cargo test --locked backend::text_extraction text_markdown --lib -- --nocapture
```

Expected: FAIL before extractor implementation.

**Step 2: Implement text and markdown extraction**

Implement `extract_text_for_blob(path, mime_type, object_id, bytes) -> ExtractedTextRecord`.

Detection order:

1. explicit supported MIME,
2. supported extension,
3. unsupported.

Keep ambiguous `application/octet-stream` unsupported unless the extension is supported.

**Step 3: Write failing docx tests**

Use a tiny generated docx fixture in tests:

- valid docx extracts paragraph text and table cell text.
- headers/footers are included only if implementation chooses to support them; make that behavior explicit in tests.
- corrupt zip returns `failed`.
- zip with too many entries or oversized XML returns `too_large`.
- document XML with malformed XML returns `failed`.
- external relationships are ignored and do not trigger network or file access.
- extracted text is capped and hash reflects capped text.

Run:

```bash
cargo test --locked backend::text_extraction docx --lib -- --nocapture
```

Expected: FAIL before docx extractor implementation.

**Step 4: Implement docx extraction**

Parse only internal zip members needed for visible text. Bound entry count, compressed/uncompressed bytes, and XML bytes before parsing. Convert paragraph/table boundaries into newlines. Collapse repeated whitespace only where OpenXML semantics require it; preserve enough line breaks for useful diffs.

Public errors should use fixed codes such as:

- `docx_zip_invalid`
- `docx_zip_too_large`
- `docx_xml_invalid`
- `docx_no_text`

Do not include zip entry names from untrusted input in public errors.

**Step 5: Write failing pdf tests**

Use a tiny generated PDF fixture in tests:

- valid pdf extracts visible text.
- multiple pages preserve page-order text with newlines.
- image-only pdf returns `ready` with empty text or `unsupported` only if documented and tested. Prefer `ready` empty so the metadata says pdf extraction ran.
- malformed pdf returns `failed`.
- encrypted/unsupported pdf returns `failed` or `unsupported` with a stable code.
- oversized/page/operator cap returns `too_large`.
- debug/public errors omit raw content and parser internals.

Run:

```bash
cargo test --locked backend::text_extraction pdf --lib -- --nocapture
```

Expected: FAIL before pdf extractor implementation.

**Step 6: Implement pdf extraction**

Extract text only from PDF content stream text operators. Keep this MVP intentionally modest: no OCR, attachments, JavaScript, remote resources, or external font fetching. If a parser dependency exposes a higher-level text extraction helper, wrap it so caps and redaction still happen at Stratum boundaries.

Use fixed failure codes such as:

- `pdf_invalid`
- `pdf_encrypted`
- `pdf_too_large`
- `pdf_no_text`

**Step 7: Verify extractor module**

```bash
cargo fmt --all -- --check
cargo test --locked backend::text_extraction --lib -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
```

Expected: PASS.

Commit:

```bash
git add Cargo.toml Cargo.lock src/backend/text_extraction.rs
git commit -m "feat: add provider-free file extractors"
```

## Task 3: Extraction Records In Commit Indexing And Search

**Files:**

- Modify: `src/backend/search_index.rs`
- Modify: `src/backend/search_index/acl.rs` only if snapshot helpers need a narrower API
- Modify: `src/backend/postgres.rs`
- Modify: `src/backend/postgres_migrations.rs` only if tests expose missing adoption checks
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_capabilities.rs`
- Modify: `sdk/contracts/capabilities.v1.json` if fixture output changes
- Modify: `sdk/contracts/capabilities.v1.durable-cloud.json` if fixture output changes

**Step 1: Write failing provider-free search tests**

Add tests under `backend::search_index` and/or `backend::text_extraction` covering:

- text and markdown extracted records become search rows.
- docx extracted text becomes searchable while raw docx bytes are not present in `content_preview`.
- pdf extracted text becomes searchable while raw pdf bytes are not present in `content_preview`.
- unsupported binary records are persisted with `unsupported` and are not search rows.
- corrupt docx/pdf records are persisted with `failed` and are not search rows.
- oversized supported files are `too_large` and not search rows.
- missing extraction metadata makes search fail closed with generic `search index not ready`.
- old ready `search_index_state` with extraction status `missing` returns `503`.
- ACL filtering still denies private extracted text before result limiting.
- final candidate recheck still suppresses stale object-id mismatches.

Run:

```bash
cargo test --locked backend::search_index --lib -- --nocapture
```

Expected: FAIL until indexing is extraction-aware.

**Step 2: Extend indexed rows**

Extend `IndexedFileRow` with:

```rust
pub extraction_version: String,
pub extractor: String,
pub extracted_text_hash: String,
```

Keep `content_preview` as the bounded extracted text preview that feeds FTS. Do not rename it in this slice unless the diff is smaller than the compatibility churn.

**Step 3: Update `index_durable_commit`**

Change the signature to accept both stores:

```rust
pub async fn index_durable_commit(
    repo_id: &RepoId,
    head: &SearchIndexHead,
    objects: &dyn ObjectStore,
    extraction_store: &dyn TextExtractionStore,
    search_store: &dyn SearchIndexStore,
) -> Result<(), VfsError>
```

Traversal rules:

- Keep existing tree entry validation and traversal cap.
- For each blob, call `extract_text_for_blob`.
- Persist every extraction record, including unsupported/failed/too_large.
- Create `IndexedFileRow` only for `ready` records with non-empty extracted text.
- Build the existing ACL snapshot from the same durable tree metadata for every search row.
- Continue skipping symlinks for extraction/search in this slice.
- Mark search state extraction-ready only after extraction records and search rows are written consistently.

**Step 4: Implement Postgres persistence**

In `PostgresMetadataStore`:

- Implement `TextExtractionStore`.
- Insert extraction records idempotently for `(repo_id, commit_id, root_tree_id, path)`.
- In `SearchIndexStore::index_commit`, require extraction metadata on every row.
- Set `search_index_state.extraction_version = 'extracted-text-v1'`.
- Set `search_index_state.extraction_status = 'ready'` only after search rows are fully written.
- On extraction/search write failure, mark search state `failed` with a bounded non-secret extraction/search failure code.
- Preserve stale-ready hardening from Slice 21.

**Step 5: Update search health and capabilities**

`search_index_acl_ready` should become a broader readiness helper or be complemented by `search_index_extraction_ready`.

Semantic search capability must require:

- durable-cloud runtime,
- search index store availability,
- current durable head resolution,
- `SearchIndexStatus::Ready`,
- `AclSnapshotStatus::Ready` with `posix-tree-v1`,
- `extraction_status = ready`,
- `extraction_version = extracted-text-v1`.

Use generic capability reasons such as `search index not extraction-ready for current head`.

**Step 6: Verify search integration**

```bash
cargo test --locked backend::search_index --lib -- --nocapture
cargo test --locked server::routes_fs --lib -- --nocapture
cargo test --locked server::routes_capabilities --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo check --locked --features postgres
```

Expected: PASS, with live Postgres portions skipped unless configured.

Commit:

```bash
git add src/backend/search_index.rs src/backend/search_index/acl.rs src/backend/postgres.rs src/server/routes_fs.rs src/server/routes_capabilities.rs sdk/contracts
git commit -m "feat: index extracted text for search"
```

## Task 4: Extracted-Text Diff And Status

**Files:**

- Modify: `src/vcs/diff.rs`
- Modify: `src/backend/committed_read.rs` only if compare summaries need source identity helpers
- Modify: `src/server/core.rs`
- Modify: `src/server/routes_vcs.rs` tests
- Modify: `src/backend/text_extraction.rs`

**Step 1: Write failing diff/status tests**

Add tests proving:

- modified docx renders unified hunks from extracted text.
- modified pdf renders unified hunks from extracted text.
- added/deleted docx/pdf render extracted text additions/deletions.
- status marks supported binary-format content changes as extracted-text-visible, for example `M /demo/report.docx [extracted-text]`.
- unsupported binary status/diff remains a metadata summary and does not render raw bytes.
- corrupt/failed extraction renders a stable summary with failure code, not parser internals.
- oversized extraction renders a stable summary.
- path filtering still works for exact paths and descendants.
- scoped sessions cannot infer private extracted text through status/diff.
- rendered output does not contain raw ZIP/PDF bytes, object-store keys, SQL, env vars, tokens, or private parser errors.

Run:

```bash
cargo test --locked server::routes_vcs --lib -- --nocapture
```

Expected: FAIL before renderer integration.

**Step 2: Pass extraction lookup into durable diff/status**

Add a small lookup interface to `src/backend/text_extraction.rs` that can retrieve records by head/path. Use the existing `TextExtractionStore::record_for_path` if it is enough.

Change durable rendering flow so it has:

- repo id,
- base commit/root tree,
- head commit/root tree,
- changed path records,
- object store for legacy text reads,
- text extraction store for supported extracted types.

Do not add extracted text fields to `PathRecord`.

**Step 3: Update diff renderer**

For each file change:

- If before/after extraction records are `ready`, use their extracted text for grouped unified hunks.
- If only before or after exists and is `ready`, diff against empty text.
- If extraction status is `unsupported`, render existing content summary plus `extraction: unsupported`.
- If extraction status is `too_large` or `failed`, render content summary plus stable extraction status/failure code.
- For ordinary plain text/markdown, prefer extraction records when present. If missing in local/default mode, keep existing raw UTF-8 behavior so local legacy diff does not regress.
- For docx/pdf, never read raw blob bytes to attempt UTF-8 diff.

Expected docx/pdf diff header:

```diff
diff -- /demo/report.docx
extracted-text: docx-v1
--- a/demo/report.docx
+++ b/demo/report.docx
@@ -1,3 +1,3 @@
-old clause
+new clause
```

**Step 4: Update status rendering**

In durable status output, append an extracted-text marker only when:

- path kind is file,
- MIME/extension is supported by extraction,
- before/after extraction metadata shows at least one ready/failed/too_large/unsupported status.

Example:

```text
M /demo/report.docx [extracted-text]
M /demo/archive.bin [extraction-unsupported]
```

Keep existing source identity lines unchanged.

**Step 5: Verify**

```bash
cargo test --locked server::routes_vcs --lib -- --nocapture
cargo test --locked backend::text_extraction --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo check --locked
```

Expected: PASS.

Commit:

```bash
git add src/vcs/diff.rs src/backend/committed_read.rs src/server/core.rs src/server/routes_vcs.rs src/backend/text_extraction.rs src/backend/postgres.rs
git commit -m "feat: render extracted text diffs"
```

## Task 5: Docs, SDK Impact, And Capability Fixtures

**Files:**

- Modify: `docs/http-api-guide.md`
- Modify: `docs/semantic-index.md`
- Modify: `docs/project-status.md`
- Modify: `src/server/routes_capabilities.rs`
- Modify: `sdk/contracts/capabilities.v1.json` if fixture output changes
- Modify: `sdk/contracts/capabilities.v1.durable-cloud.json` if fixture output changes
- Modify: `sdk/typescript/src/types.ts` only if capability schema changes
- Modify: `sdk/python/src/stratum_sdk/types.py` only if capability schema changes
- Modify: `sdk/bash/src/tool-description.ts` only if wording changes

**Step 1: Write failing docs/capability tests if manifest changes**

If adding capability hints such as `diff.supported_fragment_kinds = ["text", "metadata", "binary_summary", "extracted_text"]`, update capability tests first.

Run:

```bash
cargo test --locked server::routes_capabilities --lib -- --nocapture
```

Expected: FAIL only if fixtures or capability types need updates.

**Step 2: Update docs**

Document:

- supported extractor types and explicit non-goals,
- extraction caps and status codes,
- search requires extraction readiness, ACL readiness, and exact head freshness,
- unsupported/corrupt/oversized files are not indexed,
- docx/pdf diff uses extracted text and still stores raw files as bytes,
- status markers for extracted-text changes,
- no SDK response fields expose raw extraction metadata beyond existing status/diff text and semantic snippets,
- automatic/background index production remains out of scope unless this slice explicitly adds it.

Update `docs/project-status.md` only after implementation lands.

**Step 3: SDK impact assessment**

Expected:

- TypeScript `SearchClient.semantic()` stays unchanged.
- Python `SearchClient.semantic()` stays unchanged.
- TypeScript/Python `diff()` and `status()` still return plain text.
- Bash `diff` and `status` receive server-rendered extracted-text output automatically.
- Bash `sgrep` remains unsupported unless a separate SDK search command slice enables it.
- Capability fixtures may change if extraction readiness or diff fragment support is represented.

Run SDK checks even if no SDK files change:

```bash
cd sdk && bun run typecheck
cd sdk && bun run test:run
```

If Python files change:

```bash
cd sdk/python && python -m ruff check src tests
cd sdk/python && python -m mypy src/stratum_sdk
cd sdk/python && python -m pytest
```

**Step 4: Verify docs and fixtures**

```bash
git diff --check
cargo test --locked server::routes_capabilities --lib -- --nocapture
STRATUM_UPDATE_CAPABILITY_FIXTURES=1 cargo test --locked server::routes_capabilities::tests::update_checked_in_sdk_contract_fixture_when_requested --lib -- --nocapture
cd sdk && bun run typecheck
cd sdk && bun run test:run
```

Expected: PASS.

Commit:

```bash
git add docs/http-api-guide.md docs/semantic-index.md docs/project-status.md src/server/routes_capabilities.rs sdk/contracts sdk/typescript/src/types.ts sdk/python/src/stratum_sdk/types.py sdk/bash/src/tool-description.ts
git commit -m "docs: document file extraction surfaces"
```

## Task 6: Full Verification And Review

**Files:**

- No new files unless prior tasks uncover narrowly required fixes.

**Step 1: Required verification**

Run:

```bash
git status --short --branch
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::text_extraction --lib -- --nocapture
cargo test --locked backend::search_index --lib -- --nocapture
cargo test --locked server::routes_fs --lib -- --nocapture
cargo test --locked server::routes_vcs --lib -- --nocapture
cargo test --locked server::routes_capabilities --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo check --locked
cargo check --locked --features postgres
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cd sdk && bun run typecheck
cd sdk && bun run test:run
```

Expected: PASS. Live Postgres tests may skip unless `STRATUM_POSTGRES_TEST_URL` is configured.

**Step 2: Redaction search**

Search for accidental public leak surfaces:

```bash
rg -n "postgres://|postgresql://|STRATUM_|token|secret|object_key|SQLSTATE|zip|pdf|docx|xml|raw bytes|read_prefix|acl_snapshot" src docs sdk migrations
```

Expected: only intentional code/docs references. Public errors, fixtures, tests, and docs examples must not include raw tokens, raw read prefixes, object keys, DB URLs, env vars, raw SQL, raw ACL JSON, parser internals, or extracted secret fixture text outside private test-only assertions.

**Step 3: Dependency review**

If new extraction dependencies were added, inspect:

```bash
cargo tree -i zip
cargo tree -i quick-xml
cargo tree -i lopdf
cargo deny check
```

If `cargo deny` is not configured, run `cargo tree` and `cargo audit --deny warnings` at minimum. Record skipped tooling explicitly.

**Step 4: Review checklist**

Review for:

- search indexing raw docx/pdf bytes,
- docx zip bombs or XML expansion bypassing caps,
- pdf parser paths that allocate or recurse without bounds,
- external relationship/network/file access,
- parser errors leaking document content or paths,
- search result limiting before ACL filtering,
- final `cat_with_stat_as` recheck removal or weakening,
- stale indexes that look ready without extraction metadata,
- unsupported files entering FTS rows,
- diff/status revealing private extracted text to unauthorized sessions,
- SDK contract drift,
- Postgres adoption checks missing constraints that make old rows look extraction-ready.

**Step 5: Final status**

```bash
git status --short --branch
```

Expected: clean after commits.

## Rollback Plan

- Migration 21 is additive and should remain applied.
- Mark semantic search unavailable unless `search_index_state.extraction_status = 'ready'` and version is `extracted-text-v1`.
- Keep extraction records for diagnostics, but do not use `failed`, `too_large`, or `unsupported` records for search rows.
- If docx/pdf extraction has a correctness or security bug, return explicit `unsupported` or `failed` extraction metadata for that type and keep raw binary files unindexed.
- If extracted-text diff is wrong, fall back to metadata summaries for docx/pdf instead of raw binary text diff.
- Never fall back to local `.vfs`, grep/find traversal, raw binary indexing, or result rendering before ACL verification.

## Out Of Scope

- OCR for image PDFs.
- Track changes, docx redline generation, or native Office rendering.
- PDF page thumbnails or image extraction.
- pgvector or embeddings.
- Background indexing workers beyond the explicit indexing helper, unless implementation discovers an existing safe trigger already wired for search.
- Hosted search UI.
- Broad MIME inference across arbitrary file types.
- Changing public SDK method signatures.
- Remote extraction services or provider calls.

## Gemini / Subagent Review Prompts

Use these after implementation, not as a substitute for local review.

**Extractor security review prompt:**

Review the Slice 22 file extractor implementation for parser safety and data leaks. Focus on docx zip bounds, XML parsing bounds, pdf parsing bounds, unsupported/corrupt/oversized behavior, public error redaction, dependency risk, and any path that can read external files, fetch network resources, execute commands, or index raw binary bytes. Provide concrete file/line findings only.

**Search/ACL review prompt:**

Review the extracted-text search integration. Verify semantic search requires exact durable head freshness, extraction readiness, ACL snapshot readiness, and `posix-tree-v1`; filters ACLs before result limiting; preserves final `cat_with_stat_as` and object-hash recheck; excludes unsupported/failed/oversized records; and never falls back to local state or raw binary bytes.

**Diff/status review prompt:**

Review durable VCS status/diff integration for extracted text. Verify docx/pdf changes render extracted-text hunks or stable extraction summaries without raw binary bytes, path filters remain segment-safe, mounted/session scopes cannot reveal private extracted text, metadata-only and unsupported binary behavior remain stable, and source identity lines remain correct.

**Postgres migration review prompt:**

Review migration 0021 and adoption verification. Verify all new schema is additive, old search indexes default to extraction-missing and fail closed, constraints prevent malformed ready records, indexes match lookup patterns, public adoption/startup errors are generic, and live/optional Postgres tests are correctly skipped or required based on environment.
