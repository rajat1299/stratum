# Capability manifest v1 — shape lock + typed-banner alignment

**Owner:** Head of Frontend Engineering (coordinating with Backend tech lead)
**Status:** Open — two asks, both blocking nothing today but needed before either team builds on top of `hints.banner` or extends `v1`.
**Filed in response to:** CTO review of 2026-05-15 backend slice 1 + frontend roadmap PRs.

---

## Context

Backend shipped the capability manifest endpoint (`266cb0b feat: add capability manifest endpoint`) with checked-in JSON fixtures at `sdk/contracts/capabilities.v1.json` and `sdk/contracts/capabilities.v1.durable-cloud.json`, plus `getCapabilities()` helpers in the TS and Python SDKs. The CTO's review confirmed alignment on:

- The nested group structure (`revision / server / auth / routes / diff / protection / idempotency / recovery / limits / hints`) — identical to what the frontend independently proposed.
- Per-verb route shape (`{ available, admin, idempotent?, reason?, tracking_ref?, requires?, blocked_when? }`) — backend's `RouteOperationCapability` matches exactly and added `execution?` + `notes?` (additive, forward-compatible).
- The two-field split for `server.backend_mode` (`"local" | "durable" | "durable-cloud"`) + `server.core_runtime` (`"local-state" | "durable-cloud"`) — cleaner than the single-field shape originally proposed.

**One field is misaligned and needs to be tightened before any UI surface reads it.** And the CTO flagged a product-level concern about v1 shape stability that should be codified.

---

## Ask 1 — type `hints.banner` (the misalignment)

### What backend shipped

`banner: Option<serde_json::Value>` — the JSON value can be literally anything the server feels like serializing. Untyped at the contract boundary.

### What frontend proposed in `2026-05-15-capability-manifest-requirements.md` §3

```ts
hints: {
  banner: { kind: "info" | "warn", text: string } | null;
  // ...
}
```

A small closed union. No action URLs, no markdown, no extra keys, ≤ 280 chars of text. The smallest possible surface for "the server wants to tell every reviewer something right now."

### Why this matters

`Option<serde_json::Value>` means:

- The frontend cannot generate types from `sdk/contracts/capabilities.v1.json` and have the banner field do any work — TypeScript sees `unknown` and forces a cast at every consumer.
- A future operator could ship `{ kind: "danger", text: "...", action_url: "https://attacker.example/" }` and it would silently render… something. We don't know what, because the contract is open.
- A future backend refactor could change the shape without breaking a single test, because there is no asserted shape.

### Frontend's interim mitigation (already landed)

`web/src/lib/banner-parser.ts` is the perimeter. `parseBanner(value: unknown): Banner | null` accepts only the proposed shape and rejects everything else with a once-per-session console warning. Vitest coverage at `web/src/lib/banner-parser.test.ts` asserts:

- Accepts `{kind:"info",text:"…"}` and `{kind:"warn",text:"…"}`.
- Accepts `null` and `undefined` silently (the "no banner" state isn't an error).
- Rejects scalars, arrays, unknown `kind` values, missing or empty `text`, text > 280 chars, and **any extra keys** (closed contract — defends against future drift).
- Warns at most once per (reason, fingerprint) pair so the dev console doesn't spam.

This validator is the contract the rest of `web/` is allowed to depend on. Any component that reads `hints.banner` must go through it. The validator becomes a noop once backend tightens the Rust type — at which point we keep the file around as defense-in-depth.

### Ask of backend

Replace `banner: Option<serde_json::Value>` with the typed enum:

```rust
#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(rename_all = "lowercase")]
pub enum BannerKind {
    Info,
    Warn,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Banner {
    pub kind: BannerKind,
    /// Bounded; reject text longer than 280 chars at construction time.
    pub text: String,
}

pub struct CapabilityHints {
    pub banner: Option<Banner>,
    pub branding: Option<Branding>,
    pub support_url: Option<String>,
}
```

Regenerate `sdk/contracts/capabilities.v1.{json,durable-cloud.json}` fixtures from the new struct. SDK contract tests should fail loudly until the fixtures are regenerated — that's the point.

### Timing

No frontend surface reads `hints.banner` today. The first to do so is Phase F1 (empty states + microcopy pass), ~week 11. We'd like this tightened well before then so we can delete the validator from any hot read path. **Target: in flight within 1 week, landed within 3.**

---

## Ask 2 — lock the v1 shape; never mutate fields under `v1`

### CTO product observation (2026-05-15)

> "The fact that the FE's capability requirements and the BE's endpoint plan arrived at *identical* group structure independently … is a strong signal that the domain model is solid. … Lock the v1 shape now and version subsequent changes; do not let either team mutate fields under v1."

### Codifying the rule

Adopt the following as a coordination policy between backend and frontend:

| Change | Treated as |
|---|---|
| New optional field added under an existing group | Additive — compatible. Bump `revision` (date+counter). |
| New group added at the top level | Additive — compatible. Bump `revision`. |
| Existing field's type widened (e.g., enum gets a new variant) | **Breaking under v1.** Cut `v2` of the endpoint. |
| Existing field renamed, removed, or its semantics changed | **Breaking under v1.** Cut `v2` of the endpoint. |
| `routes.*.available` flips from `true` to `false` to reflect a real route flip | Not breaking — that *is* the contract. |
| Adding a new enum variant to `server.backend_mode` | **Breaking.** Cut `v2` — fail-closed in v1 consumers is preferred to surprise behavior. |

`v1` lives at `GET /v1/capabilities` and remains live for at least 60 days after `v2` ships. `sdk/contracts/capabilities.v1.json` is the source of truth for the wire shape during v1; the Rust struct's `Serialize` impl must round-trip the checked-in fixture exactly. Backend's existing contract-fixture test enforces that.

### Frontend's commitment

The frontend will:

- Consume `sdk/contracts/capabilities.v1.json` as the mock during local dev and CI (replacing the inline mock referenced in `web/src/lib/capabilities.mock.ts` once that file exists in Phase A4).
- Treat any field not present in the fixture as if it doesn't exist (don't read undeclared optional fields).
- Run a TypeScript build step that fails if `@stratum/sdk`'s generated `CapabilityManifest` type drifts from the checked-in fixture.

### Ask of backend

- Adopt the table above as the contract-change policy. Document it in `docs/http-api-guide.md` next to the manifest section.
- When a breaking change is needed, cut `GET /v2/capabilities` rather than mutating `v1`.

---

## Resolution

This ticket closes when:

1. Backend ships a typed `Banner` struct + regenerated fixtures, and `parseBanner()` becomes a defense-in-depth noop.
2. The v1-vs-v2 contract policy is documented in `docs/http-api-guide.md` and CI enforces the fixture round-trip.

Until then, the frontend treats `hints.banner` as untrusted JSON and routes every read through `parseBanner()`.

---

*— Frontend Engineering*
