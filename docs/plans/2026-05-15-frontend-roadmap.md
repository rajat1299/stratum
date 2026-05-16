# Frontend roadmap — 2026-05-15

**Owner:** Head of Frontend Engineering
**Status:** Draft 1 — for review
**Cadence:** Small, reviewable, demoable slices, mirroring the backend team's pattern.

This is the 90-day plan to turn Stratum's backend into a product people see. It assumes the briefing #2 framing: V1 is the reviewer console, V2 is the operator console, V3 is polish and differentiation.

---

## 1. Stack decision (final — do not revisit for 6 months)

| Layer | Choice | One-paragraph rationale |
|---|---|---|
| **App framework** | **Vite 7 + TanStack Router** | The Stratum console is an authenticated SPA, not a content site. SSR buys us nothing and costs us route co-location. Vite's dev loop is fastest; TanStack Router gives us file-based typed routes without the Next.js mental model tax. Easy to drop behind any reverse proxy. |
| **Language** | TypeScript (strict) | Non-negotiable. Source of truth is `@stratum/sdk`'s types; we want a compile-time wall against shape drift. |
| **Components** | **shadcn/ui** on Radix primitives | Owned components in our tree, not a black-box dep. Good baseline for keyboard nav and a11y. We bring our own design tokens; shadcn's structure is the substrate. |
| **Styling** | **Tailwind v4** | Pairs with shadcn. v4's CSS-first config matches how we already think about tokens. |
| **Server state** | **TanStack Query** | The dominant pattern; pairs with the SDK's promise-based methods. Background refetch, stale-while-revalidate, optimistic mutations — all for free. |
| **Client state** | Zustand for the small amount we need (active workspace, pinned filters, UI prefs). Avoid Redux. |
| **Routing** | TanStack Router with file-based routes, type-safe `search` params (we need them for diff `?base=…&head=…`). |
| **Forms** | react-hook-form + Zod resolver | Zod gives us one schema definition we can reuse client-side and as a contract assertion against API responses. |
| **Data layer** | `@stratum/sdk` as the single source of HTTP truth | Briefing requirement. Every fetch goes through the SDK; no hand-rolled `fetch` in components. Behind a `useStratumClient()` hook so the workspace context (RepoId, X-Stratum-Workspace) is plumbed exactly once. |
| **Diff parsing** | Hand-rolled parser over the Rust text output | The Rust diff is text, not JSON. See §3 below. The `diff` npm package is for *producing* diffs, which we don't need — we render server-rendered diffs. |
| **Markdown** | react-markdown + remark-gfm + Shiki (lazy-loaded grammars) | Reviewers will read agent rationale and runbooks. GFM tables matter. |
| **Charts** | Tremor for the recovery/audit dashboard | Recharts under the hood; saves a week of layout work. |
| **Auth** | Local users via `/auth/login` for v1. Wrap in an `AuthProvider` whose contract is OIDC-drop-in compatible from day one. |
| **Tests** | Vitest (unit + parser snapshots), Playwright (one happy-path per V1 slice). |
| **Package manager** | **pnpm 11+** with `minimumReleaseAge: 10080` (7 days) and `@stratum/*` exempt. Configured in `pnpm-workspace.yaml` at repo root. |
| **Bundle/perf** | Apply `vercel-react-best-practices` skill notes before V1 ships. Specific items: route-level code split, defer Shiki grammars, memoize the diff renderer per-hunk. |

**Things we are explicitly not adopting:**

- ❌ Next.js — SSR cost without SSR benefit.
- ❌ Heavy diff component libraries (`react-diff-viewer`, etc.) — they're built for client-computed diffs, not server-rendered unified text. Their styling fights ours and they can't render the 5 non-text summary shapes Stratum emits.
- ❌ MUI / Chakra / Ant — we won't redesign their tokens to match Stratum.
- ❌ Redux / Recoil — TanStack Query already covers ~90% of state.
- ❌ Pyodide for V1. The python-in-browser idea from mirage is interesting; it doesn't earn its slot until V3 at earliest.

---

## 2. Who we're building for, in order

1. **Reviewer** (legal/compliance/eng lead). Lives in `/change-requests` and `/audit`. Judges the product. V1 must feel polished for them.
2. **Operator** (the human who configures agents and scopes). Lives in `/workspaces`, `/access`, `/tokens`. V2 surface, lower-fi OK for v1.
3. **Agent** — talks to the SDK, never to our UI. But every agent action surfaces here as a change request, audit row, or commit. The console exists partly to make agents legible.

Implications for the design language:
- Information-dense and quiet. Linear, GitHub PR, Sentry — not Vercel marketing site.
- Light + dark from day one (`adapt` skill pass in week 4).
- Tabular numbers everywhere (`tnum`, `cv11`, `ss01`).
- Empty states with intent (`clarify` skill pass for microcopy).
- Diff view is the hero. If it doesn't feel good for an enterprise lawyer with a tall screen and a slow eye, we've failed.

---

## 3. The diff problem (read this before slicing)

`src/vcs/diff.rs` emits **plain text** with a stable but heterogeneous shape. Our UI must parse and render five distinct fragment kinds per path:

| Kind | Source marker | What we render |
|---|---|---|
| `text-unified` | `@@ -a,b +c,d @@` hunk headers | Per-hunk syntax-highlighted line view with +/-/context columns, expandable context, click-to-jump-to-file |
| `metadata-only` | `metadata:` followed by `- mode:` / `+ mode:` / `- uid:` / `- mime_type:` / `- custom_attrs.<k>:` | Compact attribute-diff card — show only the fields that changed |
| `binary` | `Binary or non-UTF-8 content is not supported by text diff.` | "Binary change" card with before/after object hash, size, MIME (when API returns durable summary), no fake hex |
| `too-large` | `Text diff is too large to render.` | "Diff too large" card with object hashes + sizes + an "open file at ref" deep link |
| `kind-changed` | `path kind changed; text diff is not available` | "File became directory" (or symlink) card with both sides' kinds |

The durable code path also emits an explicit structured summary block:

```
diff -- /path
reason: <reason>
before: object=<hash> size=<n> type=<file|dir|symlink|absent> mime=<mime>
after:  object=<hash> size=<n> type=<file|dir|symlink|absent> mime=<mime>
```

Our parser must recognize all four reason strings (above) plus the legacy unconditional binary/too-large messages from the in-memory codepath, and surface the hash/size/type/mime to the renderer. **This text format is the contract between us and Rust.** Any change here is a coordinated PR with backend.

The code spike at `web/src/spike/diff-spike.tsx` is the live demonstration. See §6.

---

## 4. The first two weeks (you're reading the end of week 1)

### Week 1 — read & demo
- ✅ Read briefing #2, positioning doc, version-control doc, user-management doc, HTTP API guide (review/change-request, audit, diff, recovery sections), `markdownfs_v2_cto_architecture_plan.md` §9–§12, `sdk/typescript/src/`, `src/vcs/diff.rs`.
- ✅ Inventory mirage references (VFP/Observer/JobTable) and decide vendor vs. type-only vs. skip.
- ✅ Verify pnpm `minimumReleaseAge` semantics against the live docs; commit `pnpm-workspace.yaml` with 7-day cooldown.
- ⏳ Stand up a local server, create a workspace, issue a token, write a file via SDK, commit, open a change request via SDK, approve via SDK end-to-end in the terminal. (Manual step — to be done by EOW1.)

### Week 2 — design spike
- Ship the **diff-view spike** at `/spike/diff` — real Rust diff output piped through a real parser into a real renderer.
- Decide design token set; commit `web/tailwind.config.ts` and the four shadcn primitives we know we need: `Button`, `DropdownMenu`, `Dialog`, `Command`.
- Sketch the **change-request detail screen** in code (not Figma — the briefing told us not to waste a pass on Figma).
- Draft `docs/plans/2026-05-15-capability-manifest-requirements.md` (this exists alongside this file) and route those requirements to backend so the GET /v1/capabilities contract is locked.

### Week 2 deliverables (this PR set)
- This roadmap (you're reading it).
- Capability-manifest requirements doc.
- pnpm cooldown configured.
- `web/` scaffold (Vite + Tailwind v4 + TanStack Router + TanStack Query).
- `web/src/spike/diff-spike.tsx` + `web/src/lib/diff-parser.ts` + vitest tests against the Rust fixture strings.

---

## 5. 90-day slice plan (V1 = reviewer console)

Each slice is one-PR-shaped: ≤ 600 LoC, demoable on a real server in under 5 minutes, ships behind a route, has at least one Vitest test and one Playwright happy-path. Numbering matches the V1 ranked list in briefing #2; tighter slices than the briefing's bullets so each one is reviewable.

### Phase A — Foundation (weeks 1–3)

| # | Slice | Output |
|---|---|---|
| A1 | Stack + scaffold + cooldown | `web/` builds and dev-runs; pnpm cooldown active |
| A2 | Auth provider, session-aware SDK client hook | `useStratumClient()` returns an authenticated client; login screen lands on /reviews |
| A3 | App shell — left nav (Repository · Reviews · Audit · Settings), top breadcrumb, command palette stub | Empty routes render with consistent shell |
| A4 | TanStack Query provider, error boundary, toast layer | All queries get optimistic updates, retries, and visible failure |
| A5 | Theme tokens + light/dark + tabular numerals + density toggle | One pass with `make-interfaces-feel-better` skill notes |

### Phase B — Repository browser (weeks 3–5)

| # | Slice | Output |
|---|---|---|
| B1 | Repo browser: ref switcher (main · agent/… · review/…) + breadcrumb path | Pull from `GET /vcs/refs`; protected refs render with a small shield |
| B2 | File tree (sparse — fetch one level at a time) + selected-file panel | `GET /fs/{path}` + `?stat=true` |
| B3 | File content view with MIME-aware rendering: markdown (react-markdown + Shiki), code (Shiki), image, fallback "binary preview unsupported" | Bytes-stable, content-hash visible |
| B4 | Commit timeline alongside file tree (sticky, condensed) | `GET /vcs/log` + hover for parents/changed-path count |

### Phase C — Diff (weeks 5–6) — **the hero**

| # | Slice | Output |
|---|---|---|
| C1 | Diff-text parser, full-coverage Vitest fixtures from `src/vcs/diff.rs` test bodies | Pure function, no React |
| C2 | Diff renderer: text-unified mode (hunks, +/-/context, expandable context, line anchors) | Uses Shiki grammar derived from file extension |
| C3 | Diff renderer: 4 non-text shapes (binary, too-large, metadata-only, kind-changed) | Each gets its own component, not a fallback `<pre>` |
| C4 | Per-file collapse + "viewed" checkbox state (client-side, persisted to session) | Mirrors GitHub PR muscle memory |
| C5 | "Why approval is required" inline rendering: render `matched_ref_rules` and `matched_path_rules` next to the merge button | Source: `approval_state` policy decision |

### Phase D — Change-request console (weeks 6–9) — **the daily driver**

| # | Slice | Output |
|---|---|---|
| D1 | Change-request list at `/change-requests` — open / merged / rejected tabs, filter by source ref + author | `GET /change-requests` |
| D2 | Change-request detail layout: header (title, source→target ref, base→head commits, status), tabs (Conversation / Files changed / Approvals) | Routes by id |
| D3 | Approve, Reject, Dismiss actions wired through the SDK with idempotency keys + optimistic UI + audit-visible confirmations | `.../approvals`, `.../reject`, `.../approvals/{id}/dismiss` |
| D4 | Review-comment thread (inline-on-diff and general) with `kind: changes_requested` markers | `.../comments` |
| D5 | Reviewer-assignment surface — assign, mark required vs optional, downgrade | `.../reviewers` |
| D6 | Merge button with the fast-forward contract spelled out ("This will advance `main` from `<base>` to `<head>`. Source ref must still point to `<head>`.") and a disabled tooltip when `approval_state.approved === false` showing what's missing | `.../merge` |
| D7 | Revert action with a confirm modal, audit-visible | `POST /vcs/revert` |

### Phase E — Audit + operator surfaces (weeks 9–11)

| # | Slice | Output |
|---|---|---|
| E1 | Audit panel (`/audit`) with actor / resource / time filters, pagination, JSON-detail drawer | `GET /audit` admin-only; clean 403 state for non-admins |
| E2 | Workspaces + tokens screen — create, list, scope picker, revoke; **token shown once** with a copy-to-clipboard + "I've saved it" confirm pattern | `GET/POST /workspaces`, `POST .../tokens` |
| E3 | Protected rules editor — refs and path-prefixes, with the YAML shape from plan §8 hidden behind a form | `GET/POST /protected/refs`, `.../paths` |
| E4 | Recovery / health dashboard — phase counts, scheduler heartbeat, "run bounded recovery" button (operator-only) | `GET /vcs/recovery`, `POST /vcs/recovery/run` |

### Phase F — Polish & launch readiness (weeks 11–13)

| # | Slice | Output |
|---|---|---|
| F1 | Empty states with intent — every list and every screen | `clarify` skill pass |
| F2 | Loading and error states for every query and mutation | Skeleton patterns, retry CTAs |
| F3 | Keyboard map — ⌘K palette, j/k navigation in lists, `a` approve on a focused review | `web-design-guidelines` skill review |
| F4 | Performance pass: route splits, Shiki lazy load, memoized hunk renderer, suspense boundaries around heavy panels | `vercel-react-best-practices` skill notes |
| F5 | A11y pass: tab order, focus rings, ARIA roles on the tree/diff, contrast in dark mode | shadcn primitives get us most of this, but verify on the diff view |
| F6 | First polished demo to a design partner | Real workspace, real agent, real review, end-to-end |

**Out of scope for the 90 days:**
- Notifications (V3) — design the data shape but don't ship UI.
- Semantic search UI (waiting on backend; expose grep + find only).
- Run-records browser (V3; the foundation endpoint is there but execution isn't).
- Pyodide preview (V3+).
- Mobile / sub-768 layouts (V3; the audience uses 14"+ screens).

---

## 6. The code spike (week 2 deliverable)

Lives at `web/src/spike/diff-spike.tsx`. Pure-frontend (no live backend needed) — feeds the diff parser real fixture strings copied verbatim from `src/vcs/diff.rs` tests, then renders all five fragment kinds end-to-end.

To run (once `pnpm install` lands):

```bash
cd web && pnpm install && pnpm dev
# open http://localhost:5173/spike/diff
```

To test:

```bash
cd web && pnpm test diff-parser
```

If the spike doesn't feel good at week 2, it won't feel good at week 12. We expect to throw the visual layer away once, keep the parser, and rebuild against the design tokens. That's the design discipline.

---

## 7. Open questions to backend (the unblockers)

These get filed alongside this roadmap into the backend's plan queue. See `docs/plans/2026-05-15-capability-manifest-requirements.md` for the full list. The short version:

1. **Capability manifest endpoint.** `GET /v1/capabilities` shape + a sketch of mock manifests. Without this we can't gracefully render "semantic search unsupported" or "guarded-durable-only" routes. This is the single biggest unblock.
2. **Structured diff JSON, or stability guarantee on the current text format.** We've parsed against the text format. If you ever ship `?format=json` we'll switch.
3. **Server-Sent Events or a long-poll endpoint for the audit stream.** Pulling `/audit?limit=100` every 30s gets us most of the way; SSE would make the activity pane feel live.
4. **An `actor` resolver — UID → username, for change-request rendering.** Right now `approval_state` returns UIDs; we'd resolve via `/auth/login` echo per UID, which is fine for v1 but odd.

---

## 8. Risks and how we'll bend them

| Risk | Mitigation |
|---|---|
| Backend ships durable-cloud while we're mid-build | SDK abstracts; we already plumb workspace context through a hook, not URLs. The only place we'd notice is `/runs/{id}/stdout` streaming, which is V3. |
| Diff text format drifts | Parser has snapshot tests against Rust fixture strings; if backend changes the format, a CI test breaks loudly. |
| Reviewer mental model needs more than 13px Inter rows | Density toggle + tabular numerals + per-hunk collapse. We'll know in week 5 if Linear's density is right or if reviewers want something closer to Phabricator. |
| Capability manifest never lands | Mock manifest in `web/src/lib/capabilities.mock.ts`. We build against it; the day backend ships, we delete the mock. |

---

*— Frontend Engineering*
