# SDK CLI Change Request Path Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add the private-beta change-request creation path so the TypeScript SDK and `stratumctl` can open a change request from an existing session/source ref to a target ref with idempotency and clear errors.

**Architecture:** Do not change backend review semantics. Reuse the existing `POST /change-requests` route and its server-side idempotency support. Add a small TypeScript SDK convenience helper over the existing reviews client, add the missing Rust client method, expose it through `stratumctl change-request create`, and update only the docs/examples needed for the golden path.

**Tech Stack:** TypeScript SDK, Vitest, Rust 2024, Clap, Reqwest, Axum test helpers, existing Stratum HTTP review routes.

---

## Current Inventory

- Worktree: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/private-beta-closeout`
- Branch: `private-beta-closeout`, tracking `origin/main`.
- Known unrelated untracked state: `.codex-scratch/`; preserve it.
- Source-of-truth docs checked:
  - `/var/folders/hp/gn9_bs093l31rdpkq98yyj2h0000gn/T/stratum-task7-agent-adapter-handoff.md`
  - `/Users/rajattiwari/virtualfilesystem/stratum_current_state_cto_review.md`
  - `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/private-beta-closeout/docs/private-beta-contract.md`
  - `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- The private-beta contract says the local golden path includes: scoped workspace token, agent edit, open a change request from session ref to target ref, inspect diff, approve/reject, merge, audit, revert.
- The CTO review explicitly lists "create a change request from sdk/cli" under Priority 1 golden-path reliability.
- Backend route already exists:
  - `POST /change-requests`
  - body: `{ "title": "...", "description": "...", "source_ref": "...", "target_ref": "..." }`
  - idempotency: optional `Idempotency-Key`, server-enforced and advertised in `docs/http-api-guide.md`.
- TypeScript SDK already has low-level `client.reviews.createChangeRequest({ source_ref, target_ref, ... }, options)`.
- Python SDK already documents low-level `create_change_request`; do not touch Python in this slice unless a later review explicitly asks for parity.
- Rust client `src/client/mod.rs` has no change-request method.
- `stratumctl` has no change-request subcommand.
- Task 7 example currently opens CRs through the low-level TypeScript SDK method; after this slice it should use the new helper.

## Baseline Checks From Planning

Passed:

```bash
bun run --cwd sdk/typescript test:run
```

Result: 8 files passed, 60 tests passed, 1 skipped.

Rust baseline commands were attempted with `CARGO_TARGET_DIR=/tmp/stratum-target-task8-plan` but the local machine hit `No space left on device` while compiling dependencies. Treat this as environment state, not a code failure. During implementation, run Rust checks with a fresh disposable target directory and enough disk space. Do not leave `lattice/target` in the workspace.

## Hard Rules

- Do not change server route behavior unless a test proves the existing route is broken.
- Do not add a new backend change-request route.
- Do not add Python parity in this slice unless explicitly requested after review.
- Do not make `stratumctl` auto-commit, auto-create refs, reset `main`, or infer refs from workspace env files.
- The CR command creates a review from an existing session/source ref to a target ref. If the ref is missing, surface the server error clearly.
- Do not log, print, fixture, or commit raw tokens, idempotency keys that look like secrets, bearer values, workspace tokens, DB URLs, backend provider errors, or request bodies.
- Mark auth and idempotency headers sensitive in Rust client requests.
- Keep CLI errors bounded. Do not echo raw invalid repo ids, workspace tokens, bearer tokens, or idempotency key values.
- Preserve `.codex-scratch/`.
- Use `CARGO_TARGET_DIR=/tmp/stratum-target-task8` or another disposable `/tmp` path for Rust checks.

## Public Shape

### TypeScript SDK

Add a convenience helper on `ReviewsClient`:

```ts
export interface ChangeRequestFromSessionRequest {
  readonly title: string;
  readonly description?: string | null;
  readonly session_ref: string;
  readonly target_ref?: string;
}
```

Method:

```ts
createChangeRequestFromSession(
  request: ChangeRequestFromSessionRequest,
  options?: StratumMutationOptions,
): Promise<ChangeRequestResponse>
```

Behavior:

- `session_ref` maps to wire `source_ref`.
- `target_ref` defaults to `"main"` when omitted.
- `title`, `session_ref`, and resolved `target_ref` must be non-empty after trimming.
- Reject invalid local inputs before making a fetch request with clear messages:
  - `title is required`
  - `session_ref is required`
  - `target_ref is required`
- Use existing `createChangeRequest()` internally so idempotency behavior stays identical.

### CLI

Add:

```bash
stratumctl change-request create \
  --session-ref agent/incident-demo/session \
  --target-ref main \
  --title "Investigate checkout latency" \
  --description "Agent incident update" \
  --idempotency-key incident-cr-1
```

Flags:

- `--session-ref <REF>` required. This is the existing source/session ref.
- `--target-ref <REF>` optional, defaults to `main`.
- `--title <TITLE>` required.
- `--description <TEXT>` optional.
- `--idempotency-key <KEY>` optional.
- `--json` optional. Default output is human-readable metadata.

Default text output:

```text
change request <id> open <source_ref> -> <target_ref>
base <base_commit>
head <head_commit>
```

JSON output: pretty-print the full response returned by the server.

Clear local validation errors:

- `change-request create requires --session-ref`
- `change-request create requires --target-ref`
- `change-request create requires --title`
- `Idempotency-Key must not be empty`
- `Idempotency-Key must contain visible ASCII only`
- `Idempotency-Key must be at most 255 bytes`

## Task 1: TypeScript SDK Helper

**Files:**
- Modify: `sdk/typescript/src/types.ts`
- Modify: `sdk/typescript/src/client.ts`
- Modify: `sdk/typescript/tests/client.test.ts`
- Modify: `sdk/typescript/README.md`

### Step 1: Write failing TypeScript tests

Add tests near the existing review mutation tests in `sdk/typescript/tests/client.test.ts`.

Test 1: helper maps session ref to source ref and supplied idempotency.

```ts
it("creates change requests from session refs with supplied idempotency", async () => {
  const { fetchImpl, requests } = recordFetch();
  const client = new StratumClient({
    baseUrl: "https://stratum.example",
    auth: { type: "user", username: "root" },
    fetch: fetchImpl,
  });

  await client.reviews.createChangeRequestFromSession(
    {
      title: "Investigate checkout latency",
      description: "Agent incident update",
      session_ref: "agent/incident-demo/session",
      target_ref: "main",
    },
    { idempotencyKey: "incident-cr-1" },
  );

  expect(requests[0]?.method).toBe("POST");
  expect(requests[0]?.url).toBe("https://stratum.example/change-requests");
  expect(requests[0]?.headers.get("Idempotency-Key")).toBe("incident-cr-1");
  expect(await requestBody(requests[0]!)).toEqual({
    title: "Investigate checkout latency",
    description: "Agent incident update",
    source_ref: "agent/incident-demo/session",
    target_ref: "main",
  });
});
```

Test 2: `target_ref` defaults to `main` and still auto-generates idempotency.

```ts
it("defaults session change requests to main and auto idempotency", async () => {
  const { fetchImpl, requests } = recordFetch();
  const client = new StratumClient({
    baseUrl: "https://stratum.example",
    auth: { type: "user", username: "root" },
    idempotencyKeyPrefix: "test-sdk",
    fetch: fetchImpl,
  });

  await client.reviews.createChangeRequestFromSession({
    title: "Investigate checkout latency",
    session_ref: "agent/incident-demo/session",
  });

  expect(requests[0]?.headers.get("Idempotency-Key")).toMatch(/^test-sdk-/);
  expect(await requestBody(requests[0]!)).toEqual({
    title: "Investigate checkout latency",
    source_ref: "agent/incident-demo/session",
    target_ref: "main",
  });
});
```

Test 3: invalid local inputs reject before fetch.

```ts
it("rejects invalid session change request inputs before fetch", async () => {
  const { fetchImpl, requests } = recordFetch();
  const client = new StratumClient({
    baseUrl: "https://stratum.example",
    auth: { type: "user", username: "root" },
    fetch: fetchImpl,
  });

  await expect(
    client.reviews.createChangeRequestFromSession({ title: "", session_ref: "agent/session" }),
  ).rejects.toThrow("title is required");
  await expect(
    client.reviews.createChangeRequestFromSession({ title: "Review", session_ref: "" }),
  ).rejects.toThrow("session_ref is required");
  await expect(
    client.reviews.createChangeRequestFromSession({
      title: "Review",
      session_ref: "agent/session",
      target_ref: " ",
    }),
  ).rejects.toThrow("target_ref is required");

  expect(requests).toHaveLength(0);
});
```

### Step 2: Run the focused test red

Run:

```bash
bun run --cwd sdk/typescript test:run -- client.test.ts
```

Expected: FAIL because `createChangeRequestFromSession` does not exist.

### Step 3: Implement the TypeScript helper

In `sdk/typescript/src/types.ts`, add:

```ts
export interface ChangeRequestFromSessionRequest {
  readonly title: string;
  readonly description?: string | null;
  readonly session_ref: string;
  readonly target_ref?: string;
}
```

In `sdk/typescript/src/client.ts`, import the new type and add this helper near `createChangeRequest`:

```ts
  createChangeRequestFromSession(
    request: ChangeRequestFromSessionRequest,
    options: StratumMutationOptions = {},
  ): Promise<ChangeRequestResponse> {
    const title = requiredReviewField(request.title, "title");
    const sourceRef = requiredReviewField(request.session_ref, "session_ref");
    const targetRef = requiredReviewField(request.target_ref ?? "main", "target_ref");
    return this.createChangeRequest(
      {
        title,
        ...(request.description !== undefined ? { description: request.description } : {}),
        source_ref: sourceRef,
        target_ref: targetRef,
      },
      options,
    );
  }
```

Add a local helper in `client.ts`:

```ts
function requiredReviewField(value: string, name: string): string {
  if (value.trim() === "") {
    throw new Error(`${name} is required`);
  }
  return value;
}
```

Do not trim the returned value. Validation should reject all-whitespace strings without silently rewriting ref names or titles.

### Step 4: Update TypeScript SDK README

In `sdk/typescript/README.md`, under the admin/user auth example, add a short CR snippet:

```ts
const cr = await admin.reviews.createChangeRequestFromSession(
  {
    title: "Incident update",
    description: "Agent changes from the mounted incident workspace.",
    session_ref: "agent/incident-demo/session",
    target_ref: "main",
  },
  { idempotencyKey: "incident-demo-cr-1" },
);
```

Keep copy functional. Do not add marketing text.

### Step 5: Run TypeScript checks

Run:

```bash
bun run --cwd sdk/typescript test:run -- client.test.ts
bun run --cwd sdk/typescript typecheck
```

Expected: PASS.

## Task 2: Update Task 7 Example To Use The Helper

**Files:**
- Modify: `sdk/agents/examples/incident-change-request.ts`
- Modify: `sdk/agents/tests/incident-example.test.ts` only if the mocked route assertions need adjustment.

### Step 1: Write/adjust failing expectation if needed

The existing incident example test already expects `POST /change-requests` with `source_ref` and `target_ref`. Keep that assertion. If you want an explicit helper-use guard, add a light assertion only around behavior, not implementation internals.

### Step 2: Replace low-level call

In `sdk/agents/examples/incident-change-request.ts`, replace:

```ts
const changeRequest = await adminClient.reviews.createChangeRequest({
  title: plan.title,
  description: plan.description,
  source_ref: sourceRef,
  target_ref: TARGET_REF,
});
```

with:

```ts
const changeRequest = await adminClient.reviews.createChangeRequestFromSession({
  title: plan.title,
  description: plan.description,
  session_ref: sourceRef,
  target_ref: TARGET_REF,
});
```

Do not change the CR/ref algorithm from Task 7.

### Step 3: Run focused agents checks

Run:

```bash
bun run --cwd sdk/agents test:run -- incident-example.test.ts
bun run --cwd sdk/agents typecheck
```

Expected: PASS.

## Task 3: Rust Client Change Request Method

**Files:**
- Modify: `src/client/mod.rs`

### Step 1: Write failing Rust client tests

Extend the existing test helper in `src/client/mod.rs`.

1. Update `record_headers` to include the idempotency header:

```rust
fn record_headers(records: &HeaderRecords, headers: AxumHeaderMap) -> Value {
    let value = serde_json::json!({
        "authorization": header_value(&headers, "authorization"),
        "x-stratum-workspace": header_value(&headers, "x-stratum-workspace"),
        "x-stratum-repo": header_value(&headers, "x-stratum-repo"),
        "idempotency-key": header_value(&headers, "idempotency-key"),
    });
    records.lock().unwrap().push(value.clone());
    value
}
```

2. Add a `POST /change-requests` handler to `spawn_header_echo_server()`:

```rust
async fn change_request_echo(
    State(records): State<HeaderRecords>,
    headers: AxumHeaderMap,
    Json(body): Json<Value>,
) -> Json<Value> {
    record_headers(&records, headers);
    Json(serde_json::json!({
        "change_request": {
            "id": "cr-1",
            "title": body["title"],
            "description": body.get("description").cloned().unwrap_or(Value::Null),
            "source_ref": body["source_ref"],
            "target_ref": body["target_ref"],
            "base_commit": "a".repeat(40),
            "head_commit": "b".repeat(40),
            "status": "open",
            "created_by": 0,
            "version": 1
        },
        "approval_state": {
            "change_request_id": "cr-1",
            "required_approvals": 0,
            "approval_count": 0,
            "approved_by": [],
            "required_reviewers": [],
            "approved_required_reviewers": [],
            "missing_required_reviewers": [],
            "approved": true,
            "matched_ref_rules": [],
            "matched_path_rules": [],
            "require_all_files_viewed": false
        },
        "require_all_files_viewed": false
    }))
}
```

Register it:

```rust
.route("/change-requests", post(change_request_echo))
```

3. Add this test:

```rust
#[tokio::test]
async fn create_change_request_sends_workspace_repo_auth_and_idempotency() {
    let (base_url, server, records) = spawn_header_echo_server().await;
    let client = workspace_repo_client(base_url);

    let response = client
        .create_change_request_from_session(
            "Investigate checkout latency",
            Some("Agent incident update"),
            "agent/incident-demo/session",
            "main",
            Some("incident-cr-1"),
        )
        .await
        .unwrap();
    server.abort();

    assert_eq!(response.change_request.id, "cr-1");
    assert_eq!(response.change_request.source_ref, "agent/incident-demo/session");
    assert_eq!(response.change_request.target_ref, "main");

    let values = recorded_headers(&records);
    assert_eq!(values.len(), 1);
    assert_workspace_repo_headers(&values[0]);
    assert_eq!(
        values[0].get("idempotency-key"),
        Some(&Value::String("incident-cr-1".to_string()))
    );
}
```

4. Add validation tests:

```rust
#[test]
fn create_change_request_rejects_empty_inputs_without_echoing_secrets() {
    let client = StratumClient::new("http://127.0.0.1:3000", ClientAuth::Root);

    for (title, description, session_ref, target_ref, expected) in [
        ("", None, "agent/session", "main", "change-request create requires --title"),
        ("Review", None, "", "main", "change-request create requires --session-ref"),
        ("Review", None, "agent/session", "", "change-request create requires --target-ref"),
    ] {
        let err = client
            .create_change_request_from_session(title, description, session_ref, target_ref, None)
            .now_or_never()
            .expect("validation should complete immediately")
            .expect_err("empty input should fail");
        let VfsError::InvalidArgs { message } = err else {
            panic!("empty input should return InvalidArgs");
        };
        assert_eq!(message, expected);
    }
}
```

If `.now_or_never()` would require adding a new import only for this test, instead make this a `#[tokio::test]` and `await` each call.

5. Add idempotency-key validation:

```rust
#[tokio::test]
async fn create_change_request_rejects_invalid_idempotency_key_without_echoing_value() {
    let client = StratumClient::new("http://127.0.0.1:3000", ClientAuth::Root);
    for (key, expected) in [
        ("", "Idempotency-Key must not be empty"),
        ("has space secret", "Idempotency-Key must contain visible ASCII only"),
    ] {
        let err = client
            .create_change_request_from_session("Review", None, "agent/session", "main", Some(key))
            .await
            .expect_err("invalid idempotency key should fail");
        let VfsError::InvalidArgs { message } = err else {
            panic!("invalid idempotency should return InvalidArgs");
        };
        assert_eq!(message, expected);
        if !key.is_empty() {
            assert!(!message.contains(key));
        }
    }
}
```

Add a separate case for a 256-byte key and expect `Idempotency-Key must be at most 255 bytes`.

### Step 2: Run Rust client test red

Run:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task8 \
  cargo test --locked client::tests --lib -- --nocapture
```

Expected: FAIL because the client method/types do not exist.

### Step 3: Implement Rust client types and method

In `src/client/mod.rs`, add response/request structs near the existing client response structs:

```rust
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ClientChangeRequest {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    pub source_ref: String,
    pub target_ref: String,
    pub base_commit: String,
    pub head_commit: String,
    pub status: String,
    pub created_by: u32,
    pub version: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ClientChangeRequestResponse {
    pub change_request: ClientChangeRequest,
    pub approval_state: serde_json::Value,
    pub require_all_files_viewed: bool,
}

#[derive(Serialize)]
struct CreateChangeRequestRequest<'a> {
    title: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<&'a str>,
    source_ref: &'a str,
    target_ref: &'a str,
}
```

Add validation helpers:

```rust
fn require_change_request_arg<'a>(value: &'a str, flag: &str) -> Result<&'a str, VfsError> {
    if value.trim().is_empty() {
        return Err(VfsError::InvalidArgs {
            message: format!("change-request create requires {flag}"),
        });
    }
    Ok(value)
}

fn idempotency_header_value(value: &str) -> Result<HeaderValue, VfsError> {
    if value.is_empty() {
        return Err(VfsError::InvalidArgs {
            message: "Idempotency-Key must not be empty".to_string(),
        });
    }
    if value.len() > 255 {
        return Err(VfsError::InvalidArgs {
            message: "Idempotency-Key must be at most 255 bytes".to_string(),
        });
    }
    if !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) {
        return Err(VfsError::InvalidArgs {
            message: "Idempotency-Key must contain visible ASCII only".to_string(),
        });
    }
    let mut header = HeaderValue::from_str(value).map_err(|_| VfsError::InvalidArgs {
        message: "Idempotency-Key must contain visible ASCII only".to_string(),
    })?;
    header.set_sensitive(true);
    Ok(header)
}
```

Add a client method:

```rust
pub async fn create_change_request_from_session(
    &self,
    title: &str,
    description: Option<&str>,
    session_ref: &str,
    target_ref: &str,
    idempotency_key: Option<&str>,
) -> Result<ClientChangeRequestResponse, VfsError> {
    let title = require_change_request_arg(title, "--title")?;
    let session_ref = require_change_request_arg(session_ref, "--session-ref")?;
    let target_ref = require_change_request_arg(target_ref, "--target-ref")?;
    let mut builder = self
        .client
        .post(format!("{}/change-requests", self.base_url))
        .headers(self.headers()?);
    if let Some(key) = idempotency_key {
        builder = builder.header("Idempotency-Key", idempotency_header_value(key)?);
    }
    self.json(builder.json(&CreateChangeRequestRequest {
        title,
        description,
        source_ref: session_ref,
        target_ref,
    }))
    .await
}
```

If this double-applies headers because `self.json()` also calls `.headers(self.headers()?)`, refactor narrowly:

```rust
async fn json_with_headers<T>(&self, builder: reqwest::RequestBuilder, headers: HeaderMap) -> Result<T, VfsError>
where
    T: for<'de> Deserialize<'de>,
{
    let response = builder
        .headers(headers)
        .send()
        .await
        .map_err(|e| VfsError::IoError(std::io::Error::other(e.to_string())))?;
    Self::json_response(response).await
}
```

Then have `json()` call `json_with_headers(builder, self.headers()?)`, and have the CR method add idempotency to the header map before calling `json_with_headers`.

### Step 4: Run Rust client tests

Run:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task8 \
  cargo test --locked client::tests --lib -- --nocapture
```

Expected: PASS.

## Task 4: `stratumctl change-request create`

**Files:**
- Modify: `src/bin/stratumctl.rs`
- Modify: `tests/fixtures/cli/stratumctl-help.stdout`

### Step 1: Write failing CLI parse/render tests

In `src/bin/stratumctl.rs`, add tests near the existing CLI parse tests.

Parse test:

```rust
#[test]
fn change_request_create_command_parses_flags() {
    let _env_guard = STRATUM_REPO_ENV_LOCK.lock().unwrap();
    let cli = Cli::try_parse_from([
        "stratumctl",
        "change-request",
        "create",
        "--session-ref",
        "agent/incident-demo/session",
        "--target-ref",
        "main",
        "--title",
        "Investigate checkout latency",
        "--description",
        "Agent incident update",
        "--idempotency-key",
        "incident-cr-1",
        "--json",
    ])
    .unwrap();

    let Command::ChangeRequest {
        command:
            ChangeRequestCommand::Create {
                session_ref,
                target_ref,
                title,
                description,
                idempotency_key,
                json,
            },
    } = cli.command
    else {
        panic!("expected change-request create command");
    };

    assert_eq!(session_ref, "agent/incident-demo/session");
    assert_eq!(target_ref, "main");
    assert_eq!(title, "Investigate checkout latency");
    assert_eq!(description.as_deref(), Some("Agent incident update"));
    assert_eq!(idempotency_key.as_deref(), Some("incident-cr-1"));
    assert!(json);
}
```

Default target-ref test:

```rust
#[test]
fn change_request_create_defaults_target_ref_to_main() {
    let cli = Cli::try_parse_from([
        "stratumctl",
        "change-request",
        "create",
        "--session-ref",
        "agent/incident-demo/session",
        "--title",
        "Investigate checkout latency",
    ])
    .unwrap();

    let Command::ChangeRequest {
        command: ChangeRequestCommand::Create { target_ref, .. },
    } = cli.command
    else {
        panic!("expected change-request create command");
    };

    assert_eq!(target_ref, "main");
}
```

Render test:

```rust
#[test]
fn change_request_text_output_is_metadata_only() {
    let response = stratum::client::ClientChangeRequestResponse {
        change_request: stratum::client::ClientChangeRequest {
            id: "cr-1".to_string(),
            title: "Investigate checkout latency".to_string(),
            description: Some("Agent incident update".to_string()),
            source_ref: "agent/incident-demo/session".to_string(),
            target_ref: "main".to_string(),
            base_commit: "a".repeat(40),
            head_commit: "b".repeat(40),
            status: "open".to_string(),
            created_by: 0,
            version: 1,
        },
        approval_state: serde_json::json!({"approved": true}),
        require_all_files_viewed: false,
    };

    let output = render_change_request_created(&response, false);

    assert!(output.contains("change request cr-1 open agent/incident-demo/session -> main"));
    assert!(output.contains(&"a".repeat(40)));
    assert!(output.contains(&"b".repeat(40)));
    assert!(!output.contains("Agent incident update"));
    assert!(!output.contains("incident-cr-1"));
}
```

### Step 2: Run CLI tests red

Run:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task8 \
  cargo test --locked --bin stratumctl -- --nocapture
```

Expected: FAIL because command types/render helper do not exist.

### Step 3: Implement CLI command

Add enum variant:

```rust
ChangeRequest {
    #[command(subcommand)]
    command: ChangeRequestCommand,
},
```

Add subcommand enum:

```rust
#[derive(Subcommand)]
enum ChangeRequestCommand {
    Create {
        #[arg(long = "session-ref")]
        session_ref: String,
        #[arg(long = "target-ref", default_value = "main")]
        target_ref: String,
        #[arg(long)]
        title: String,
        #[arg(long)]
        description: Option<String>,
        #[arg(long = "idempotency-key")]
        idempotency_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
}
```

Add match arm in `main()`:

```rust
Command::ChangeRequest { ref command } => match command {
    ChangeRequestCommand::Create {
        session_ref,
        target_ref,
        title,
        description,
        idempotency_key,
        json,
    } => match client
        .create_change_request_from_session(
            title,
            description.as_deref(),
            session_ref,
            target_ref,
            idempotency_key.as_deref(),
        )
        .await
    {
        Ok(response) => {
            print!("{}", render_change_request_created(&response, *json));
            Ok(())
        }
        Err(err) => Err(err),
    },
},
```

Add render helper:

```rust
fn render_change_request_created(
    response: &stratum::client::ClientChangeRequestResponse,
    json: bool,
) -> String {
    if json {
        return format!("{}\n", serde_json::to_string_pretty(response).unwrap());
    }
    let cr = &response.change_request;
    format!(
        "change request {} {} {} -> {}\nbase {}\nhead {}\n",
        cr.id, cr.status, cr.source_ref, cr.target_ref, cr.base_commit, cr.head_commit
    )
}
```

For JSON rendering, `ClientChangeRequestResponse` must derive `Serialize` as well as `Deserialize`.

### Step 4: Update help fixture

Run the actual binary help after compilation and update:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task8 \
  cargo run --locked --bin stratumctl -- --help
```

Update `tests/fixtures/cli/stratumctl-help.stdout` to include:

```text
  change-request
```

Keep spacing exactly as generated by Clap.

### Step 5: Run CLI truth and binary tests

Run:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task8 \
  cargo test --locked --bin stratumctl -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task8 \
  cargo test --locked --test cli_truth -- --nocapture
```

Expected: PASS.

## Task 5: Docs For The Golden Path

**Files:**
- Modify: `docs/getting-started.md`
- Modify: `docs/agent-workspace-demo.md`
- Optional modify: `sdk/agents/examples/README.md` if the Task 7 example text needs a one-line pointer to the new SDK/CLI path.

### Step 1: Update `docs/getting-started.md`

In "First Run - Remote CLI", add a concise example after the seed-demo env commands:

```bash
# Open a change request from an existing session/source ref to main
cargo run --release --bin stratumctl -- \
  --url http://127.0.0.1:3000 \
  --user root \
  change-request create \
  --session-ref agent/incident-demo/session \
  --target-ref main \
  --title "Incident update" \
  --description "Agent changes from the mounted incident workspace." \
  --idempotency-key incident-demo-cr-1
```

Add one sentence:

```md
The command assumes the source/session ref already exists; it does not create refs, commit workspace changes, or infer state from the workspace env file.
```

### Step 2: Update `docs/agent-workspace-demo.md`

Add a short "Open a change request" minute or note after the agent writes/commits section. Use the same command shape and keep it metadata-only. Do not include raw tokens.

Mention:

- use `--user root` for local demo admin CR creation;
- hosted durable preview uses repo-bound workspace bearer plus `--workspace-id`, `--workspace-token`, and `--repo`;
- if the source ref does not exist, the server returns a bounded error and the CLI exits non-zero.

### Step 3: Keep docs functional

Do not rewrite the full demo script. This slice is a command path, not product copy.

## Task 6: Focused Verification

Run all commands from a clean staged state. Do not leave `lattice/target` in the workspace.

### TypeScript

```bash
bun run --cwd sdk/typescript test:run -- client.test.ts
bun run --cwd sdk/typescript test:run
bun run --cwd sdk/typescript typecheck
bun run --cwd sdk/agents test:run -- incident-example.test.ts
bun run --cwd sdk/agents typecheck
```

Expected: all pass.

### Rust

Use a disposable target dir and avoid running multiple cargo jobs in parallel on the same target directory:

```bash
CARGO_TARGET_DIR=/tmp/stratum-target-task8 \
  cargo test --locked client::tests --lib -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task8 \
  cargo test --locked --bin stratumctl -- --nocapture
CARGO_TARGET_DIR=/tmp/stratum-target-task8 \
  cargo test --locked --test cli_truth -- --nocapture
cargo fmt --all -- --check
```

Expected: all pass. If `/tmp` is low on space, stop and report the environment blocker instead of changing the workspace target policy.

### Whitespace and secret audit

```bash
git diff --check
rg -n "workspace-secret|issued-workspace-secret|STRATUM_WORKSPACE_TOKEN=.*[A-Za-z0-9_-]{8,}|Bearer [A-Za-z0-9_-]{8,}|Idempotency-Key: [A-Za-z0-9_-]{8,}|sk-[A-Za-z0-9]" \
  src/client/mod.rs src/bin/stratumctl.rs sdk/typescript/src sdk/typescript/tests sdk/agents/examples docs/getting-started.md docs/agent-workspace-demo.md
```

Expected: no real secrets. Test fixture strings like `workspace-secret` may appear only in tests that assert redaction and must not appear in docs or CLI output examples.

## Done Criteria

- TypeScript SDK exposes `reviews.createChangeRequestFromSession()`.
- The helper maps `session_ref` to wire `source_ref`, defaults `target_ref` to `main`, validates non-empty local inputs, and preserves existing idempotency behavior.
- Task 7 incident example uses the helper.
- Rust client can call `POST /change-requests` with optional sensitive `Idempotency-Key`.
- `stratumctl change-request create` opens a CR from an existing session/source ref to target ref.
- CLI supports human and JSON output.
- CLI/Rust local validation errors are clear and do not echo secrets or raw invalid idempotency values.
- Help fixture is updated.
- Docs show the golden-path command without implying the CLI creates refs or commits automatically.
- No backend route behavior is changed.

## Sidecar Implementer Boundary

The smaller implementation model should implement Tasks 1-6 only and stop. It should not commit, push, broaden to Python, add UI, add backend routes, or redesign the review model. The controller session will do integration, security/code-quality review, verification, commit, and push.
