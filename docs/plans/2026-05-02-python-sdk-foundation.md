# Python SDK Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a reusable Python SDK package over the current Stratum HTTP API, matching the TypeScript SDK foundation without changing Rust server behavior.

**Architecture:** Build a standalone `sdk/python` package using a `src/` layout and a synchronous HTTP client over `httpx.Client`. Mirror the public resource-client shape from `sdk/typescript`: filesystem, search, VCS, reviews, runs, and workspaces. Keep semantic search explicit as unsupported until the backend derived index exists.

**Tech Stack:** Python 3.11+, `pyproject.toml` project metadata, Hatchling build backend, HTTPX, pytest, mypy, ruff, current Stratum HTTP API from `docs/http-api-guide.md`.

---

## Official Docs Checked

- PyPA `pyproject.toml` project metadata specification: https://packaging.python.org/specifications/declaring-project-metadata/
- pytest getting-started and configuration docs: https://docs.pytest.org/
- HTTPX docs for `Client`, responses, headers, transports, and `MockTransport`: https://www.python-httpx.org/

## Product Constraints

- Do not edit Rust server behavior in this slice.
- Do not implement semantic search. Add `client.search.semantic(...)` that raises `UnsupportedFeatureError`.
- Do not add async support in this first Python slice. A future `AsyncStratumClient` can reuse the same route and type layer once the sync API is stable.
- Do not publish to PyPI or add release automation.
- Keep route construction safe against URL dot-segment escapes, matching the TypeScript SDK route helpers.
- Keep workspace bearer auth support with both explicit auth objects and compatibility `workspace_id` plus `workspace_token` constructor options.
- Keep idempotency-key support on mutating methods, with caller-supplied keys and automatic SDK-generated keys.
- Keep responses typed enough for SDK users while avoiding a large model layer that will drift from the Rust API. Prefer `TypedDict` response shapes and JSON dictionaries over runtime validation frameworks.

## Package Decisions

- Distribution name: `stratum-sdk`.
- Import package: `stratum_sdk`.
- Main class: `StratumClient`.
- Default idempotency prefix: `stratum-python-sdk`.
- Minimum Python version: `>=3.11`.
- Runtime dependency: `httpx>=0.27,<1`.
- Dev dependencies: `pytest`, `mypy`, `ruff`, `build`.

## Task 1: Scaffold `sdk/python`

**Files:**
- Create: `sdk/python/pyproject.toml`
- Create: `sdk/python/README.md`
- Create: `sdk/python/src/stratum_sdk/__init__.py`
- Create: `sdk/python/src/stratum_sdk/py.typed`
- Create: `sdk/python/tests/test_package.py`
- Modify: `.gitignore`

**Behavior:**
- `pyproject.toml` defines:
  - `[build-system]` with Hatchling.
  - `[project]` name `stratum-sdk`, version `0.0.0`, Python `>=3.11`, MIT license, HTTPX dependency.
  - `[project.optional-dependencies] dev` with pytest, mypy, ruff, and build.
  - `[tool.pytest.ini_options]` with `testpaths = ["tests"]`.
  - `[tool.mypy]` with strict-enough checks for `src/stratum_sdk`.
  - `[tool.ruff]` targeting Python 3.11.
- `README.md` states this is the Python SDK for the current Stratum HTTP API and semantic search is intentionally unsupported for now.
- `__init__.py` exports `__version__ = "0.0.0"`.
- `py.typed` marks the package typed.
- `.gitignore` ignores Python build/test artifacts:
  - `/sdk/python/.venv/`
  - `/sdk/python/dist/`
  - `/sdk/python/build/`
  - `/sdk/python/*.egg-info/`
  - `/sdk/python/.pytest_cache/`
  - `/sdk/python/.mypy_cache/`
  - `/sdk/python/.ruff_cache/`

**Test:**
- `tests/test_package.py` imports `stratum_sdk` and asserts `__version__ == "0.0.0"`.

**Verification:**

```bash
cd sdk/python
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
python -m pytest
```

Expected: pytest passes with the package import test.

**Commit:**

```bash
git add .gitignore sdk/python
git commit -m "feat: scaffold python sdk package"
```

## Task 2: Add HTTP Core, Auth, Errors, And Route Helpers

**Files:**
- Create: `sdk/python/src/stratum_sdk/errors.py`
- Create: `sdk/python/src/stratum_sdk/http.py`
- Create: `sdk/python/src/stratum_sdk/paths.py`
- Modify: `sdk/python/src/stratum_sdk/__init__.py`
- Create: `sdk/python/tests/test_http.py`
- Create: `sdk/python/tests/test_paths.py`

**Behavior:**
- `errors.py` defines:
  - `StratumError(Exception)`
  - `StratumHttpError(StratumError)` with `status_code: int` and `body: str`.
  - `UnsupportedFeatureError(StratumError)`.
- `http.py` defines:
  - `UserAuth(username: str)`
  - `BearerAuth(token: str)`
  - `WorkspaceAuth(workspace_id: str, workspace_token: str)`
  - `StratumHttpClient`
- `StratumHttpClient` accepts:
  - `base_url: str`
  - `auth: UserAuth | BearerAuth | WorkspaceAuth | None`
  - `client: httpx.Client | None`
  - `timeout: float | httpx.Timeout | None`
  - `idempotency_key_prefix: str = "stratum-python-sdk"`
- `StratumHttpClient` exposes:
  - `request_json(route, method, query=None, headers=None, body=None, idempotency_key=None, auto_idempotency=False)`
  - `request_text(...)`
  - `request_bytes(...)`
  - `close()`, `__enter__`, and `__exit__`.
- Auth headers match TypeScript:
  - `Authorization: User <username>`
  - `Authorization: Bearer <token>`
  - workspace auth also sets `X-Stratum-Workspace: <workspace_id>`.
- JSON request bodies set `Content-Type: application/json` only when the caller did not provide one.
- Bytes and string request bodies are sent as raw content.
- Non-2xx responses raise `StratumHttpError(status_code, body)`.
- Empty JSON responses return `None`.
- Automatic idempotency keys are visible ASCII, at most 255 bytes, and prefixed.
- `paths.py` mirrors TypeScript route behavior:
  - `normalize_route_path(path: str) -> str`
  - `fs_route(path: str) -> str`
  - `tree_route(path: str) -> str`
  - `ref_route(name: str) -> str`
  - `encode_route_segment(value: str) -> str`
- Filesystem/tree paths normalize dot segments and stay under the route prefix.
- Ref routes preserve ref names instead of normalizing them; dot-only ref segments must be double-encoded exactly like TypeScript (`"." -> "%252E"`, `".." -> "%252E%252E"`).

**Tests:**
- User, bearer, workspace, and no-auth header behavior.
- Caller-supplied idempotency key is preserved.
- Auto idempotency key is generated for mutating calls.
- JSON and raw byte bodies are sent correctly.
- HTTP errors preserve status and response body.
- `fs_route("../secret.txt") == "fs/secret.txt"`.
- `tree_route("/a/./b/../c") == "tree/a/c"`.
- `ref_route("agent/a/../b") == "vcs/refs/agent/a/%252E%252E/b"`.
- `ref_route("/leading") == "vcs/refs//leading"`.

**Verification:**

```bash
cd sdk/python
. .venv/bin/activate
python -m pytest tests/test_http.py tests/test_paths.py
python -m mypy src/stratum_sdk
python -m ruff check src tests
python -m ruff format --check src tests
```

Expected: all checks pass.

**Commit:**

```bash
git add sdk/python
git commit -m "feat: add python sdk http foundation"
```

## Task 3: Add Public Types And Resource Clients

**Files:**
- Create: `sdk/python/src/stratum_sdk/types.py`
- Create: `sdk/python/src/stratum_sdk/client.py`
- Modify: `sdk/python/src/stratum_sdk/__init__.py`
- Create: `sdk/python/tests/test_client.py`

**Behavior:**
- `types.py` defines `TypedDict` request/response shapes for the current HTTP API surface used by the SDK:
  - filesystem stat/list/write/copy/move/delete/metadata patch;
  - grep/find results;
  - VCS commit/log/revert/ref status;
  - protected ref/path rules;
  - change requests, approval state, approvals, reviewers, comments;
  - run create/read records;
  - workspace create/list/get/token issuance.
- Keep optional fields optional with `NotRequired`.
- `client.py` defines:
  - `StratumClient`
  - `FilesystemClient`
  - `SearchClient`
  - `VcsClient`
  - `ReviewsClient`
  - `RunsClient`
  - `WorkspacesClient`
- `StratumClient(...)` accepts:
  - `base_url: str`
  - `auth: UserAuth | BearerAuth | WorkspaceAuth | None = None`
  - compatibility `workspace_id: str | None = None`
  - compatibility `workspace_token: str | None = None`
  - `http_client: httpx.Client | None = None`
  - `timeout: float | httpx.Timeout | None = None`
  - `idempotency_key_prefix: str = "stratum-python-sdk"`
- If `auth` is omitted and both `workspace_id` plus `workspace_token` are supplied, use `WorkspaceAuth`.
- Expose resource clients:
  - `client.fs`
  - `client.search`
  - `client.vcs`
  - `client.reviews`
  - `client.runs`
  - `client.workspaces`
- Add compatibility methods on `StratumClient`:
  - `read_file`, `read_file_bytes`, `write_file`, `mkdir`, `list_directory`, `stat`, `delete_path`, `copy_path`, `move_path`
  - `grep`, `find`, `tree`
  - `status`, `diff`, `commit`
- Filesystem methods:
  - `read_file(path) -> str`
  - `read_file_bytes(path) -> bytes`
  - `write_file(path, content, *, mime_type=None, idempotency_key=None)`
  - `mkdir(path, *, idempotency_key=None)`
  - `list_directory(path="")`
  - `stat(path)`; root stat should raise `ValueError` with the same boundary as TypeScript.
  - `patch_metadata(path, patch, *, idempotency_key=None)`
  - `delete_path(path, recursive=False, *, idempotency_key=None)`
  - `copy_path(source, destination, *, idempotency_key=None)`
  - `move_path(source, destination, *, idempotency_key=None)`
- Search methods:
  - `grep(pattern, *, path="", recursive=True)`
  - `find(name, *, path="")`
  - `tree(path="")`
  - `semantic(query)` raises `UnsupportedFeatureError`.
- VCS methods:
  - `commit(message, *, idempotency_key=None)`
  - `log()`
  - `revert(hash, *, idempotency_key=None)`
  - `status()`
  - `diff(path=None)`
  - `list_refs()`
  - `create_ref(request, *, idempotency_key=None)`
  - `update_ref(name, request, *, idempotency_key=None)`
- Reviews methods mirror TypeScript names in Python snake_case:
  - `create_protected_ref`, `list_protected_refs`
  - `create_protected_path`, `list_protected_paths`
  - `list_change_requests`, `get_change_request`, `create_change_request`
  - `approve`, `list_approvals`
  - `assign_reviewer`, `list_reviewers`
  - `create_comment`, `list_comments`
  - `dismiss_approval`, `reject`, `merge`
- Runs methods:
  - `create`, `get`, `stdout`, `stderr`
- Workspaces methods:
  - `list`, `get`, `create`, `issue_token`
- `workspaces.issue_token` must not accept or auto-generate idempotency keys because the response contains a raw workspace token.

**Tests:**
- Use `httpx.MockTransport` to capture requests.
- Representative method coverage for each resource:
  - `fs.write_file` builds `PUT /fs/<path>` and includes `X-Stratum-Mime-Type` plus idempotency.
  - `fs.copy_path` builds `POST /fs/<source>?op=copy&dst=<destination>`.
  - `search.grep` builds `/search/grep` with pattern/path/recursive query parameters.
  - `search.semantic` raises `UnsupportedFeatureError`.
  - `vcs.update_ref("agent/a/../b", ...)` uses the double-encoded ref route.
  - `reviews.approve`, `reviews.dismiss_approval`, and `reviews.merge` build the expected review routes and include idempotency where allowed.
  - `runs.create` includes idempotency and JSON body.
  - `workspaces.issue_token` sends JSON body without `Idempotency-Key`.
- Workspace compatibility constructor sets both auth headers.
- `StratumClient` context manager closes the underlying HTTP client.

**Verification:**

```bash
cd sdk/python
. .venv/bin/activate
python -m pytest
python -m mypy src/stratum_sdk
python -m ruff check src tests
python -m ruff format --check src tests
```

Expected: all checks pass.

**Commit:**

```bash
git add sdk/python
git commit -m "feat: add python sdk resource clients"
```

## Task 4: Documentation And Status

**Files:**
- Modify: `sdk/python/README.md`
- Modify: `docs/project-status.md`

**Behavior:**
- README includes:
  - install-from-repo example;
  - user auth example;
  - workspace bearer auth example;
  - filesystem read/write/list/stat example;
  - search grep/find/tree example;
  - VCS status/diff/commit example;
  - review/change-request example;
  - runs example;
  - workspaces/token issuance example;
  - semantic search unsupported boundary.
- `docs/project-status.md` records the Python SDK foundation as active/completed depending on implementation state and preserves the backend-team status content.

**Verification:**

```bash
git diff --check -- sdk/python/README.md docs/project-status.md
```

Expected: no whitespace errors.

**Commit:**

```bash
git add sdk/python/README.md docs/project-status.md
git commit -m "docs: document python sdk foundation"
```

## Task 5: Package And Full Verification

**Files:**
- No new feature files unless verification exposes issues.

**Verification:**

```bash
cd sdk/python
. .venv/bin/activate
python -m pytest
python -m mypy src/stratum_sdk
python -m ruff check src tests
python -m ruff format --check src tests
python -m build
python -m pip install --force-reinstall dist/stratum_sdk-0.0.0-py3-none-any.whl
python - <<'PY'
from stratum_sdk import StratumClient, __version__
assert __version__ == "0.0.0"
assert StratumClient is not None
PY
cd ../..
git diff --check
```

Expected:
- pytest passes;
- mypy passes;
- ruff check and format-check pass;
- `python -m build` creates an sdist and wheel under `sdk/python/dist/`;
- the built wheel can be installed and imported;
- `git diff --check` passes.

**Commit review fixes if needed:**

```bash
git add sdk/python docs/project-status.md .gitignore
git commit -m "fix: address python sdk review findings"
```

## Independent Review Checklist

Use separate reviewers after implementation:

- API reviewer: compare Python public surface against `sdk/typescript/src/client.ts` and `docs/http-api-guide.md`; check naming, route construction, idempotency, and unsupported semantic-search boundary.
- Security/correctness reviewer: check auth headers, no token logging, workspace-token issuance no-idempotency behavior, safe route encoding, HTTP error body handling, and package contents.
- Packaging reviewer: check `pyproject.toml`, `py.typed`, wheel contents, README examples, and import ergonomics.

## Out Of Scope For This Slice

- Async Python client.
- Python virtual bash or filesystem mount adapter.
- Semantic search implementation.
- Local server integration tests that boot Rust binaries.
- PyPI publishing, Trusted Publishing, signing, or release automation.
- Rust server changes.

## Manager Handoff Message

You are implementing the Python SDK foundation for Stratum. Work only from `docs/plans/2026-05-02-python-sdk-foundation.md` and execute it task by task with small commits. The goal is parity with the TypeScript SDK over the current HTTP API, not new backend behavior. Do not touch Rust server code, do not implement semantic search, and do not add async support in this slice. Use `httpx.MockTransport` for unit tests so the SDK can be verified without a live server. Keep auth, route encoding, idempotency, and workspace-token issuance semantics aligned with `sdk/typescript/src/client.ts`, `sdk/typescript/src/http.ts`, `sdk/typescript/src/paths.ts`, and `docs/http-api-guide.md`. Update `docs/project-status.md` carefully without removing backend-team status. Run the full verification block in Task 5 before handing the branch back for review.
