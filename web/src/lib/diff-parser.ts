/**
 * Diff parser — turns the text output of `GET /vcs/diff` into structured
 * fragments the UI can render.
 *
 * Contract: every shape that `src/vcs/diff.rs` (`render_text_diff`,
 * `render_grouped_text_diff`, `render_metadata_diff`, `render_content_summary`,
 * `binary_message`, `too_large_message`) can emit must round-trip through
 * this parser and produce a typed fragment. If you change Rust, change this.
 *
 * Stability: see `docs/plans/2026-05-15-capability-manifest-requirements.md`
 * §3 (`diff.format: "text/v1"`). When backend ships JSON diffs, this module
 * becomes optional.
 */

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

export type DiffFragment =
  | TextUnifiedFragment
  | MetadataOnlyFragment
  | BinaryFragment
  | TooLargeFragment
  | KindChangedFragment
  | UnknownFragment;

export type FragmentKind = DiffFragment["kind"];

export interface BaseFragment {
  readonly path: string;
}

export interface TextUnifiedFragment extends BaseFragment {
  readonly kind: "text-unified";
  readonly hunks: readonly DiffHunk[];
  /** Counts derived from the line ops; used for the file-strip stats column. */
  readonly added: number;
  readonly removed: number;
}

export interface DiffHunk {
  /** 1-based start in the before file. May be 0 for pure additions. */
  readonly beforeStart: number;
  readonly beforeCount: number;
  /** 1-based start in the after file. May be 0 for pure deletions. */
  readonly afterStart: number;
  readonly afterCount: number;
  readonly lines: readonly DiffLine[];
}

export type DiffLine =
  | { readonly type: "context"; readonly text: string }
  | { readonly type: "add"; readonly text: string }
  | { readonly type: "remove"; readonly text: string };

export interface MetadataOnlyFragment extends BaseFragment {
  readonly kind: "metadata-only";
  readonly changes: readonly MetadataChange[];
}

export interface MetadataChange {
  readonly field: string;     // "mode" | "uid" | "gid" | "mime_type" | "custom_attrs.<key>"
  readonly before: string;
  readonly after: string;
}

export interface BinaryFragment extends BaseFragment {
  readonly kind: "binary";
  readonly summary: ContentSummary | null;
}

export interface TooLargeFragment extends BaseFragment {
  readonly kind: "too-large";
  readonly summary: ContentSummary | null;
}

export interface KindChangedFragment extends BaseFragment {
  readonly kind: "kind-changed";
  readonly summary: ContentSummary | null;
}

export interface UnknownFragment extends BaseFragment {
  readonly kind: "unknown";
  readonly raw: string;
}

/**
 * Durable-mode summary block:
 *
 *   reason: <reason>
 *   before: object=<hash|<none>> size=<n> type=<file|directory|symlink|absent> mime=<mime|<unset>>
 *   after:  object=<hash|<none>> size=<n> type=<...> mime=<...>
 *
 * Either side may be `absent` (a path that was created or deleted).
 */
export interface ContentSummary {
  readonly reason: string;
  readonly before: ContentSummarySide;
  readonly after: ContentSummarySide;
}

export interface ContentSummarySide {
  readonly object: string | null;
  readonly size: number;
  readonly type: "file" | "directory" | "symlink" | "absent";
  readonly mime: string | null;
}

// ─────────────────────────────────────────────────────────────────────────────
// Parser
// ─────────────────────────────────────────────────────────────────────────────

const FILE_HEADER_RE = /^diff -- (.+)$/;
const HUNK_HEADER_RE = /^@@ -(\d+),(\d+) \+(\d+),(\d+) @@/;
const LEGACY_HUNK_HEADER_RE = /^@@$/;
const SUMMARY_KV_RE = /^(\w+)=(\S+)/;

const REASONS = {
  binary: "binary or non-UTF-8 content is not supported by text diff",
  legacyBinary: "Binary or non-UTF-8 content is not supported by text diff.",
  tooLarge: "text diff is too large to render",
  legacyTooLarge: "Text diff is too large to render.",
  kindChanged: "path kind changed; text diff is not available",
} as const;

export interface ParsedDiff {
  readonly fragments: readonly DiffFragment[];
  readonly isEmpty: boolean;
}

/**
 * Parse the full body of `GET /vcs/diff`. Returns one fragment per touched path.
 * If the body is `No changes.\n` (the Rust empty-output sentinel) we return
 * `{ fragments: [], isEmpty: true }`.
 */
export function parseDiff(body: string): ParsedDiff {
  if (!body || body.trim() === "No changes.") {
    return { fragments: [], isEmpty: true };
  }

  const lines = body.split("\n");
  const blocks = splitIntoFileBlocks(lines);
  const fragments = blocks.map(parseFileBlock);
  return { fragments, isEmpty: fragments.length === 0 };
}

/** Split the diff body into per-file blocks, each starting at a `diff -- ` line. */
function splitIntoFileBlocks(lines: readonly string[]): string[][] {
  const blocks: string[][] = [];
  let current: string[] | null = null;
  for (const line of lines) {
    if (FILE_HEADER_RE.test(line)) {
      if (current) blocks.push(current);
      current = [line];
    } else if (current) {
      current.push(line);
    }
  }
  if (current) blocks.push(current);
  return blocks;
}

function parseFileBlock(block: readonly string[]): DiffFragment {
  const header = block[0] ?? "";
  const match = header.match(FILE_HEADER_RE);
  const path = match?.[1]?.trim() ?? "<unknown>";
  const rest = block.slice(1);

  // Metadata-only blocks open with `metadata:`
  if (rest[0] === "metadata:") {
    return parseMetadataBlock(path, rest.slice(1));
  }

  // Durable structured summary opens with `reason: ...`
  if (rest[0]?.startsWith("reason: ")) {
    return parseSummaryBlock(path, rest);
  }

  // Legacy in-memory single-line markers
  const flat = rest.join("\n");
  if (flat.includes(REASONS.legacyBinary)) {
    return { kind: "binary", path, summary: null } satisfies BinaryFragment;
  }
  if (flat.includes(REASONS.legacyTooLarge)) {
    return { kind: "too-large", path, summary: null } satisfies TooLargeFragment;
  }

  // Otherwise this is a text diff (legacy or grouped).
  return parseTextBlock(path, rest);
}

// ── metadata-only ────────────────────────────────────────────────────────────
function parseMetadataBlock(path: string, lines: readonly string[]): MetadataOnlyFragment {
  const changes: MetadataChange[] = [];
  for (let i = 0; i < lines.length; i++) {
    const ln = lines[i];
    if (!ln || !ln.startsWith("- ")) continue;
    const minus = ln.slice(2);
    const plus = lines[i + 1]?.startsWith("+ ") ? lines[i + 1]!.slice(2) : null;
    if (!plus) continue;
    const colon = minus.indexOf(":");
    if (colon === -1) continue;
    const field = minus.slice(0, colon);
    const before = minus.slice(colon + 1).trim();
    const after = plus.slice(plus.indexOf(":") + 1).trim();
    changes.push({ field, before, after });
    i++;
  }
  return { kind: "metadata-only", path, changes };
}

// ── durable structured summary ───────────────────────────────────────────────
function parseSummaryBlock(path: string, lines: readonly string[]): DiffFragment {
  const reasonLine = lines[0] ?? "";
  const reason = reasonLine.slice("reason: ".length).trim();
  const beforeLine = lines.find((l) => l.startsWith("before:")) ?? "";
  const afterLine = lines.find((l) => l.startsWith("after:")) ?? "";
  const summary: ContentSummary = {
    reason,
    before: parseSummarySide(beforeLine.slice("before:".length).trim()),
    after: parseSummarySide(afterLine.slice("after:".length).trim()),
  };

  // Classify reason → fragment kind. Unknown reasons fall through to "unknown".
  switch (reason) {
    case REASONS.binary:
      return { kind: "binary", path, summary } satisfies BinaryFragment;
    case REASONS.tooLarge:
      return { kind: "too-large", path, summary } satisfies TooLargeFragment;
    case REASONS.kindChanged:
      return { kind: "kind-changed", path, summary } satisfies KindChangedFragment;
    default:
      return { kind: "unknown", path, raw: lines.join("\n") } satisfies UnknownFragment;
  }
}

function parseSummarySide(text: string): ContentSummarySide {
  // text looks like: "object=<hash|<none>> size=<n> type=<file|directory|symlink|absent> mime=<mime|<unset>>"
  // We tokenize by whitespace but mime can contain `;` for charsets. Rust never
  // emits a mime with whitespace, so this is safe.
  const tokens = text.split(/\s+/).filter(Boolean);
  let object: string | null = null;
  let size = 0;
  let type: ContentSummarySide["type"] = "absent";
  let mime: string | null = null;
  for (const tok of tokens) {
    const m = tok.match(SUMMARY_KV_RE);
    if (!m) continue;
    const key = m[1];
    const val = m[2];
    if (!key || val === undefined) continue;
    switch (key) {
      case "object":
        object = val === "<none>" ? null : val;
        break;
      case "size":
        size = Number.parseInt(val, 10) || 0;
        break;
      case "type":
        if (val === "file" || val === "directory" || val === "symlink" || val === "absent") {
          type = val;
        }
        break;
      case "mime":
        mime = val === "<unset>" ? null : val;
        break;
    }
  }
  return { object, size, type, mime };
}

// ── text diff (legacy unconditional + grouped unified) ───────────────────────
function parseTextBlock(path: string, lines: readonly string[]): TextUnifiedFragment {
  const hunks: DiffHunk[] = [];
  let added = 0;
  let removed = 0;

  // Skip the `--- a/...` and `+++ b/...` headers if present.
  let i = 0;
  while (i < lines.length && (lines[i]?.startsWith("--- ") || lines[i]?.startsWith("+++ "))) {
    i++;
  }

  // Legacy single-hunk mode: `@@\n` opens one big hunk with no positions. We
  // synthesize line numbers from the leading context/op type so the renderer
  // can still show numbered gutters.
  if (lines[i] === "@@") {
    i++;
    const { lines: hunkLines, addedN, removedN } = consumeHunkLines(lines, i);
    const synth = synthesizePositions(hunkLines);
    hunks.push({
      beforeStart: 1,
      beforeCount: synth.before,
      afterStart: 1,
      afterCount: synth.after,
      lines: hunkLines,
    });
    added += addedN;
    removed += removedN;
    return { kind: "text-unified", path, hunks, added, removed };
  }

  // Grouped unified hunks.
  while (i < lines.length) {
    const headerLine = lines[i];
    if (!headerLine) {
      i++;
      continue;
    }
    const m = headerLine.match(HUNK_HEADER_RE);
    if (!m) {
      // Tolerate unexpected blank lines between hunks.
      if (headerLine.trim() === "") {
        i++;
        continue;
      }
      // Unknown content inside a text block — bail to keep partial output usable.
      break;
    }
    const beforeStart = Number.parseInt(m[1]!, 10);
    const beforeCount = Number.parseInt(m[2]!, 10);
    const afterStart = Number.parseInt(m[3]!, 10);
    const afterCount = Number.parseInt(m[4]!, 10);
    i++;
    const { lines: hunkLines, addedN, removedN, consumed } = consumeHunkLines(lines, i);
    hunks.push({ beforeStart, beforeCount, afterStart, afterCount, lines: hunkLines });
    added += addedN;
    removed += removedN;
    i += consumed;
  }

  return { kind: "text-unified", path, hunks, added, removed };
}

function consumeHunkLines(
  src: readonly string[],
  start: number,
): { lines: DiffLine[]; addedN: number; removedN: number; consumed: number } {
  const out: DiffLine[] = [];
  let addedN = 0;
  let removedN = 0;
  let i = start;
  for (; i < src.length; i++) {
    const ln = src[i];
    if (ln === undefined) break;
    if (LEGACY_HUNK_HEADER_RE.test(ln) || HUNK_HEADER_RE.test(ln)) break;
    if (ln.startsWith("diff -- ")) break;
    if (ln.startsWith("+")) {
      out.push({ type: "add", text: ln.slice(1) });
      addedN++;
    } else if (ln.startsWith("-")) {
      out.push({ type: "remove", text: ln.slice(1) });
      removedN++;
    } else if (ln.startsWith(" ")) {
      out.push({ type: "context", text: ln.slice(1) });
    } else if (ln === "") {
      // Trailing blank lines at the end of the body — stop, don't render them.
      // But blank context lines inside a hunk come through as " " not "".
      // If the next line continues the hunk, treat this as a blank context line.
      const next = src[i + 1];
      if (next === undefined || next === "" || next.startsWith("diff -- ") || HUNK_HEADER_RE.test(next)) {
        break;
      }
      out.push({ type: "context", text: "" });
    } else {
      // Unknown line — stop to avoid mis-rendering.
      break;
    }
  }
  return { lines: out, addedN, removedN, consumed: i - start };
}

function synthesizePositions(lines: readonly DiffLine[]): { before: number; after: number } {
  let before = 0;
  let after = 0;
  for (const l of lines) {
    if (l.type === "add") after++;
    else if (l.type === "remove") before++;
    else {
      before++;
      after++;
    }
  }
  return { before, after };
}

// ─────────────────────────────────────────────────────────────────────────────
// Display helpers
// ─────────────────────────────────────────────────────────────────────────────

export function fragmentTotals(fragment: DiffFragment): { added: number; removed: number } {
  if (fragment.kind === "text-unified") return { added: fragment.added, removed: fragment.removed };
  return { added: 0, removed: 0 };
}

export function summariseFragmentKind(kind: FragmentKind): string {
  switch (kind) {
    case "text-unified": return "Text diff";
    case "metadata-only": return "Metadata change";
    case "binary": return "Binary change";
    case "too-large": return "Diff too large";
    case "kind-changed": return "Path kind changed";
    case "unknown": return "Unrecognized";
  }
}
