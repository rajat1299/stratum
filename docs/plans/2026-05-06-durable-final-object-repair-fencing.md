# Durable Final-Object Repair/Fencing Conformance Implementation Plan

> **For Codex/subagents:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development and follow `/Users/rajattiwari/.agents/skills/pragmatic-rust-guidelines/SKILL.md`, `/Users/rajattiwari/.agents/skills/rust-best-practices/SKILL.md`, and `/Users/rajattiwari/.agents/skills/rust-async-patterns/SKILL.md`.

**Goal:** Prove the final-object orphan repair path works with real Postgres metadata and Postgres cleanup-claim leases before any live durable route executor is enabled.

**Architecture:** Keep final-object deletion fail-closed. Compose `BlobObjectStore` over `LocalBlobStore` bytes and `PostgresMetadataStore` metadata/claim leases, then add focused conformance tests for repair, active-claim skipping, and hash-mismatch failure recording. This hardens the recovery path needed after final-byte promotion succeeds but metadata insertion fails.

**Tech Stack:** Rust, Tokio, `async_trait`, local byte-store adapter, Postgres feature-gated backend tests, existing durable object metadata and cleanup-claim contracts.

---

## Scope

In scope:

- Add Postgres-backed conformance coverage for `BlobObjectStore::repair_final_object_metadata_orphans`.
- Use `PostgresMetadataStore` as both `ObjectMetadataStore` and `ObjectCleanupClaimStore`.
- Use `LocalBlobStore` for deterministic byte storage in the test schema harness.
- Keep `ObjectOrphanCleanupMode::FinalObjectsMissingMetadataDelete` returning fail-closed `NotSupported`.
- Update `docs/project-status.md` and `docs/http-api-guide.md`.

Out of scope:

- No HTTP/MCP/CLI/FUSE repair endpoint.
- No background repair worker.
- No live durable filesystem/VCS route execution.
- No R2 object-store routing in live server paths.
- No new cleanup-claim kind.
- No final-object deletion implementation.
- No `FinalObjectMetadataFence` wiring to storage deletion.

## Task 1: Add Postgres Repair Conformance Tests

**Files:**

- Modify: `src/backend/postgres.rs`

**Step 1: Write failing tests**

Add focused tests under the existing `#[cfg(test)]` Postgres module:

- `postgres_blob_object_repair_should_recreate_missing_metadata_for_final_orphan`
- `postgres_blob_object_repair_should_skip_active_claim`
- `postgres_blob_object_repair_should_record_failure_without_deleting_when_hash_mismatches`

The tests should use the existing isolated `TestDb` harness. Create a `LocalBlobStore` rooted in a unique temp dir, wrap it in `Arc`, and compose:

```rust
let blob_store = Arc::new(LocalBlobStore::new(&temp_dir));
let object_store = BlobObjectStore::new(blob_store.clone(), Arc::new(test_db.store.clone()));
```

For repair:

1. Insert final bytes directly under `object_key(&repo_id, ObjectKind::Blob, &object_id)` using `RemoteBlobStore::put_bytes`.
2. Assert `ObjectMetadataStore::get(&test_db.store, &repo_id, object_id)` is `None`.
3. Run `object_store.repair_final_object_metadata_orphans(&repo_id, cutoff, &test_db.store, "postgres-repair-worker", Duration::from_secs(60)).await`.
4. Assert one orphan found, one repaired, no skipped claim, no deletion, no errors.
5. Assert metadata now matches `ObjectMetadataRecord::from_bytes(...)`.
6. Assert bytes remain readable.
7. Assert a second claim for that canonical final object returns `None` after completion.

For active-claim skip:

1. Insert final bytes directly.
2. Pre-acquire a `FinalObjectMetadataRepair` claim through `ObjectCleanupClaimStore::claim`.
3. Run repair with another worker.
4. Assert one orphan found, one claim skipped, zero repairs, no errors.
5. Assert metadata is still missing and final bytes remain.

For hash mismatch:

1. Choose an expected object id from `expected_bytes`.
2. Write `wrong_bytes` under the canonical final key for the expected id.
3. Run repair.
4. Assert one orphan found, zero repairs, zero deletions, one error containing a hash-mismatch signal.
5. Assert metadata is still missing and final bytes remain.
6. Expire the claim with the existing `expire_cleanup_claim` helper and assert the claim can be reacquired, proving failure recording did not complete the target.

**Step 2: Verify RED**

Run with Postgres required so missing tests/helpers or incorrect assertions fail against the real adapter:

```bash
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres::tests::postgres_blob_object_repair --lib -- --nocapture
```

Expected before implementation: tests fail because they do not exist or because the helper assertions are not implemented.

**Step 3: Minimal implementation**

Implement only test support and any tiny helper needed inside `src/backend/postgres.rs`.

Preferred helper shape:

```rust
fn cleanup_claim_request_for_object(
    repo_id: &RepoId,
    kind: ObjectKind,
    object_id: ObjectId,
    lease_duration: Duration,
) -> ObjectCleanupClaimRequest
```

Do not change production repair logic unless a test exposes a real bug. If production changes are needed, keep them limited to preserving bytes, claim safety, and redacted/stable errors.

**Step 4: Verify GREEN**

Run:

```bash
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres::tests::postgres_blob_object_repair --lib -- --nocapture
cargo test --locked backend::blob_object --lib -- --nocapture
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked backend::core_transaction --lib -- --nocapture
```

**Step 5: Commit**

```bash
git add src/backend/postgres.rs
git commit -m "test: cover postgres final object repair"
```

## Task 2: Update Docs

**Files:**

- Modify: `docs/project-status.md`
- Modify: `docs/http-api-guide.md`

**Step 1: Update status**

Record that Postgres-backed final-object metadata repair is now covered as a conformance path with local byte-store bytes and durable Postgres metadata/claims.

Keep the residual-risk language explicit:

- final-object deletion still fails closed;
- there is no background repair worker;
- metadata writers still do not consult deletion fences;
- no live durable filesystem/VCS route executor is enabled.

**Step 2: Update HTTP/backend guide**

Update the durability section to state that the helper has memory and Postgres-backed conformance coverage, but remains backend-only and uninvoked by HTTP routes.

**Step 3: Verify docs diff**

Run:

```bash
git diff --check
```

**Step 4: Commit**

```bash
git add docs/project-status.md docs/http-api-guide.md
git commit -m "docs: record postgres final object repair coverage"
```

## Required Verification

Run the hard release perf gate after every meaningful code/docs diff:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Focused gates:

```bash
cargo fmt --all -- --check
cargo test --locked backend::blob_object --lib -- --nocapture
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked backend::core_transaction --lib -- --nocapture
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh
cargo clippy --locked --features postgres --all-targets -- -D warnings
git diff --check
```

Full branch gates before merge:

```bash
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features postgres
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf_comparison -- --test-threads=1 --nocapture
git diff --check
```

For performance reporting, record warm `real/user/sys`, max RSS, and peak memory footprint from `/usr/bin/time -l`. GPU efficiency is not applicable to this storage-path slice.
