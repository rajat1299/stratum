/**
 * diff-as-reviewed — Phase C4 sketch.
 *
 * The reviewer-side experience around a multi-file change. Each file gets a
 * "Viewed" checkbox; viewed files auto-collapse and dim; a progress strip
 * at the top counts how many remain. Static fixtures only — wiring against
 * /vcs/diff lands once backend slice 3 (mutable workspace routing) ships
 * and the change-request console exists in Phase D.
 *
 * What this spike proves out:
 *   - The "Viewed" pattern from GitHub PRs translates to Stratum's
 *     six-fragment-kind diff (text-unified is just one of them — binary
 *     and metadata-only files also get review state).
 *   - Per-file collapse + progress counter give reviewers a sense of
 *     where they are without scrolling back up.
 *   - Sticky review actions (Approve & merge / Request changes) at the
 *     top so the reviewer can act without scrolling back.
 *
 * What this spike does NOT do (queued for the real Phase C4/D slice):
 *   - Persist reviewed state to sessionStorage (in-memory only here).
 *   - Keyboard nav (j/k between files, v to toggle viewed).
 *   - Inline review comments per hunk / per file.
 *   - Hook into a real change request — we use a static fixture and
 *     the actions are no-ops.
 */

import { useMemo, useState } from "react";
import { DiffFragmentBody } from "../components/DiffViewer.tsx";
import { parseDiff, type DiffFragment, fragmentTotals, summariseFragmentKind } from "../lib/diff-parser.ts";

// ─────────────────────────────────────────────────────────────────────────────
// Fixture — a richer multi-file body than the parser spike uses
// ─────────────────────────────────────────────────────────────────────────────

const FIXTURE_BODY: string = [
  // text-unified (the main contract change)
  "diff -- /contracts/loi-acme-q2.docx.md",
  "--- a/contracts/loi-acme-q2.docx.md",
  "+++ b/contracts/loi-acme-q2.docx.md",
  "@@ -47,11 +47,17 @@",
  " The Indemnifying Party shall indemnify and hold harmless the",
  " Indemnified Party from and against any and all losses, damages,",
  "-claims, suits, and liabilities of any kind whatsoever, including",
  "-without limitation reasonable attorneys' fees, arising out of or",
  "-in connection with this Agreement or the performance hereof.",
  "+claims, suits, and liabilities arising out of or in connection",
  "+with this Agreement, but solely to the extent caused by the",
  "+Indemnifying Party's gross negligence or willful misconduct.",
  "+Reasonable attorneys' fees shall be recoverable only upon a",
  "+final, non-appealable judgment by a court of competent",
  "+jurisdiction.",
  " ",
  " The foregoing indemnity shall not apply to losses caused by",
  " the Indemnified Party's own negligence.",
  // text-unified (smaller — runbook touched)
  "diff -- /runbooks/redline-policy.md",
  "--- a/runbooks/redline-policy.md",
  "+++ b/runbooks/redline-policy.md",
  "@@ -42,7 +42,9 @@",
  " ### §4.b — Indemnification carve-out",
  " ",
  " Counterparty drafts default to mutual indemnity. Narrow to:",
  "-1. Gross negligence",
  "-2. Willful misconduct",
  "+1. Gross negligence",
  "+2. Willful misconduct",
  "+3. Final, non-appealable judgment by a court of competent jurisdiction",
  " ",
  " See `/case-files/precedent-2025/` for two precedent matches.",
  // metadata-only (custom_attrs change)
  "diff -- /memory/agents/redline.md",
  "metadata:",
  "- custom_attrs.last_loaded: 2026-05-15T18:00:00Z",
  "+ custom_attrs.last_loaded: 2026-05-16T14:21:08Z",
  // binary (the docx itself)
  "diff -- /contracts/loi-acme-q2.docx",
  "reason: binary or non-UTF-8 content is not supported by text diff",
  "before: object=a4f9c1b2 size=8421 type=file mime=application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  "after: object=e8b2d017 size=8442 type=file mime=application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  // too-large (a long transcript)
  "diff -- /case-files/transcript.txt",
  "reason: text diff is too large to render",
  "before: object=11111111 size=2097152 type=file mime=text/plain",
  "after: object=22222222 size=2098000 type=file mime=text/plain",
  "",
].join("\n");

// ─────────────────────────────────────────────────────────────────────────────
// Spike app
// ─────────────────────────────────────────────────────────────────────────────

export function DiffAsReviewedSpike() {
  const parsed = useMemo(() => parseDiff(FIXTURE_BODY), []);
  const fragments = parsed.fragments;

  // Per-file review state — keyed by path.
  const [viewed, setViewed] = useState<Record<string, boolean>>({});
  const reviewedCount = fragments.filter((f) => viewed[f.path]).length;
  const allReviewed = reviewedCount === fragments.length && fragments.length > 0;
  const progressPct = fragments.length === 0 ? 0 : (reviewedCount / fragments.length) * 100;

  // Roll-up stats across all fragments.
  const totals = fragments.reduce(
    (acc, f) => {
      const t = fragmentTotals(f);
      acc.added += t.added;
      acc.removed += t.removed;
      return acc;
    },
    { added: 0, removed: 0 },
  );

  function toggle(path: string) {
    setViewed((v) => ({ ...v, [path]: !v[path] }));
  }
  function markAll(value: boolean) {
    setViewed(value ? Object.fromEntries(fragments.map((f) => [f.path, true])) : {});
  }

  return (
    <div className="min-h-screen bg-stone-50">
      {/* Header — sticky so reviewer can act + see progress without scrolling back */}
      <header className="sticky top-0 z-10 border-b border-stone-200 bg-white/85 backdrop-blur">
        <div className="mx-auto max-w-5xl px-6 py-4">
          <div className="flex items-start gap-4">
            <div className="min-w-0 flex-1">
              <div className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
                Phase C4 sketch · static fixture
              </div>
              <h1 className="mt-0.5 text-[20px] font-medium tracking-tight text-stone-900">
                redline §3.2 indemnification — narrow carve-out per policy
              </h1>
              <div className="mt-1 flex flex-wrap items-center gap-3 font-mono text-[11.5px] text-stone-500">
                <span>
                  <AgentMark /> agent-redline
                </span>
                <span aria-hidden>·</span>
                <span>loi-redline-04 → main</span>
                <span aria-hidden>·</span>
                <span>
                  <span className="text-emerald-700">+{totals.added}</span>{" "}
                  <span className="text-rose-700">−{totals.removed}</span>
                </span>
                <span aria-hidden>·</span>
                <span>1m ago</span>
              </div>
            </div>
            <ApproveActions disabled={!allReviewed} />
          </div>

          {/* Progress strip */}
          <div className="mt-4 flex items-center gap-3">
            <div className="flex-1">
              <div className="h-1 overflow-hidden rounded-full bg-stone-200">
                <div
                  role="progressbar"
                  aria-valuemin={0}
                  aria-valuemax={fragments.length}
                  aria-valuenow={reviewedCount}
                  aria-label="Files reviewed"
                  style={{ width: `${progressPct}%` }}
                  className="h-full bg-emerald-500 transition-[width] duration-300"
                />
              </div>
            </div>
            <span className="font-mono text-[11.5px] tabular-nums text-stone-600">
              {reviewedCount} of {fragments.length} files reviewed
            </span>
            <button
              type="button"
              onClick={() => markAll(!allReviewed)}
              className="rounded-md border border-stone-300 px-2 py-0.5 font-mono text-[10.5px] text-stone-600 transition hover:border-stone-500 hover:text-stone-900"
            >
              {allReviewed ? "Unmark all" : "Mark all viewed"}
            </button>
          </div>
        </div>
      </header>

      {/* File list */}
      <main className="mx-auto max-w-5xl px-6 py-6">
        <div className="flex flex-col gap-3">
          {fragments.map((fragment) => (
            <ReviewedFileCard
              key={fragment.path}
              fragment={fragment}
              viewed={Boolean(viewed[fragment.path])}
              onToggle={() => toggle(fragment.path)}
            />
          ))}
        </div>

        {/* Foot note about scope */}
        <p className="mt-8 text-center font-serif text-[12.5px] italic text-stone-500">
          Static fixture only — wiring against /vcs/diff lands once backend slice 3 ships and the
          change-request console exists in Phase D.
        </p>
      </main>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// File card with review state
// ─────────────────────────────────────────────────────────────────────────────

function ReviewedFileCard({
  fragment,
  viewed,
  onToggle,
}: {
  readonly fragment: DiffFragment;
  readonly viewed: boolean;
  readonly onToggle: () => void;
}) {
  // Manual collapse for unviewed files. Viewed files auto-collapse but the
  // reviewer can re-open to take another look without un-marking.
  const [expanded, setExpanded] = useState(true);
  const [forceOpenWhenViewed, setForceOpenWhenViewed] = useState(false);
  const effectiveOpen = viewed ? forceOpenWhenViewed : expanded;

  const { added, removed } = fragmentTotals(fragment);
  const kindLabel = summariseFragmentKind(fragment.kind);

  return (
    <section
      aria-labelledby={`r-file-${fragment.path}`}
      className={`overflow-hidden rounded-md border bg-white shadow-sm transition ${
        viewed ? "border-stone-100 opacity-60" : "border-stone-200"
      }`}
    >
      <header className="flex items-center gap-3 border-b border-stone-100 bg-stone-50 px-3 py-2">
        <button
          type="button"
          onClick={() => (viewed ? setForceOpenWhenViewed((v) => !v) : setExpanded((v) => !v))}
          aria-expanded={effectiveOpen}
          aria-controls={`r-body-${fragment.path}`}
          className="rounded-sm px-1 py-0.5 text-stone-500 transition hover:bg-stone-200"
        >
          <svg
            width="10"
            height="10"
            viewBox="0 0 16 16"
            aria-hidden
            style={{
              transform: effectiveOpen ? "rotate(90deg)" : "none",
              transition: "transform 160ms ease",
            }}
          >
            <path d="M6 3l5 5-5 5" stroke="currentColor" strokeWidth={1.5} fill="none" strokeLinecap="round" />
          </svg>
        </button>
        <FileTypeBadge kind={fragment.kind} />
        <h3
          id={`r-file-${fragment.path}`}
          className={`flex-1 truncate font-mono text-[12.5px] ${viewed ? "text-stone-500 line-through decoration-stone-300" : "text-stone-900"}`}
        >
          {fragment.path}
        </h3>
        {fragment.kind === "text-unified" && (
          <span className="font-mono text-[11px] tabular-nums">
            <span className="text-emerald-700">+{added}</span>{" "}
            <span className="text-rose-700">−{removed}</span>
          </span>
        )}
        <span className="hidden font-mono text-[10px] uppercase tracking-wider text-stone-500 sm:inline">
          {kindLabel}
        </span>

        {/* Viewed checkbox — primary review action per file */}
        <label className="flex cursor-pointer items-center gap-1.5 rounded-md border border-stone-200 bg-white px-2 py-0.5 text-[11.5px] text-stone-600 transition hover:border-stone-400 hover:text-stone-900">
          <input
            type="checkbox"
            checked={viewed}
            onChange={onToggle}
            className="h-3 w-3 cursor-pointer accent-emerald-600"
          />
          Viewed
        </label>
      </header>

      {effectiveOpen && (
        <div id={`r-body-${fragment.path}`}>
          <DiffFragmentBody fragment={fragment} />
        </div>
      )}
      {viewed && !effectiveOpen && (
        <button
          type="button"
          onClick={() => setForceOpenWhenViewed(true)}
          className="block w-full px-3 py-2 text-left font-mono text-[11px] text-stone-400 transition hover:bg-stone-50 hover:text-stone-700"
        >
          Re-open this file ↓
        </button>
      )}
    </section>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Atoms
// ─────────────────────────────────────────────────────────────────────────────

function ApproveActions({ disabled }: { readonly disabled: boolean }) {
  return (
    <div className="flex items-center gap-2">
      <button
        type="button"
        className="rounded-md border border-stone-300 px-3 py-1.5 text-[12.5px] font-medium text-stone-700 transition hover:border-stone-500 hover:text-stone-900"
      >
        Request changes
      </button>
      <button
        type="button"
        disabled={disabled}
        title={disabled ? "Mark every file as viewed before approving." : "Approve & merge"}
        className="rounded-md bg-stone-900 px-3 py-1.5 text-[12.5px] font-medium text-stone-50 transition enabled:hover:bg-stone-800 disabled:cursor-not-allowed disabled:opacity-40"
      >
        Approve & merge
      </button>
    </div>
  );
}

function AgentMark() {
  return (
    <span
      aria-hidden
      className="mr-1 inline-grid h-3.5 w-3.5 place-items-center rounded-full bg-orange-100 font-mono text-[8px] font-semibold text-orange-700"
    >
      ar
    </span>
  );
}

function FileTypeBadge({ kind }: { kind: DiffFragment["kind"] }) {
  const color =
    kind === "text-unified"
      ? "bg-stone-200"
      : kind === "metadata-only"
        ? "bg-violet-200"
        : kind === "binary"
          ? "bg-amber-200"
          : kind === "too-large"
            ? "bg-amber-200"
            : kind === "kind-changed"
              ? "bg-rose-200"
              : "bg-stone-200";
  return <span aria-hidden className={`h-3.5 w-3.5 rounded-sm ${color}`} />;
}
