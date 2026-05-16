/**
 * Diff parser fixtures — every shape comes directly from `src/vcs/diff.rs`
 * and its test bodies. If Rust changes a format, a test here breaks loudly.
 */

import { describe, expect, it } from "vitest";
import { parseDiff } from "./diff-parser.ts";

// ─────────────────────────────────────────────────────────────────────────────
// Fixtures
// ─────────────────────────────────────────────────────────────────────────────

/** From `render_text_diff_preserves_legacy_local_header_and_full_equal_lines`. */
const FIXTURE_LEGACY_TEXT = [
  "diff -- /a.md",
  "--- a/a.md",
  "+++ b/a.md",
  "@@",
  " one",
  "-two",
  "+2",
  " three",
  "",
].join("\n");

/** From `grouped_text_diff_uses_unified_hunk_header_and_trims_distant_equal_lines`. */
const FIXTURE_GROUPED_TEXT = [
  "diff -- /a.md",
  "--- a/a.md",
  "+++ b/a.md",
  "@@ -5,7 +5,7 @@",
  " shared line 05",
  " shared line 06",
  " shared line 07",
  "-before",
  "+after",
  " shared line 09",
  " shared line 10",
  " shared line 11",
  "",
].join("\n");

/** From `render_metadata_diff` — mode + uid + custom_attrs changes. */
const FIXTURE_METADATA = [
  "diff -- /config.json",
  "metadata:",
  "- mode: 0644",
  "+ mode: 0755",
  "- uid: 1",
  "+ uid: 7",
  "- mime_type: text/plain",
  "+ mime_type: application/json",
  "- custom_attrs.owner: alice",
  "+ custom_attrs.owner: bob",
  "",
].join("\n");

/** From `too_large_message` / `binary_message` — in-memory legacy code path. */
const FIXTURE_LEGACY_BINARY = [
  "diff -- /assets/logo.png",
  "Binary or non-UTF-8 content is not supported by text diff.",
  "",
].join("\n");

const FIXTURE_LEGACY_TOO_LARGE = [
  "diff -- /data/dump.csv",
  "Text diff is too large to render.",
  "",
].join("\n");

/** From `render_content_summary` — durable code path with structured summary. */
const FIXTURE_DURABLE_BINARY = [
  "diff -- /contracts/loi.docx",
  "reason: binary or non-UTF-8 content is not supported by text diff",
  "before: object=a1b2c3d4 size=8421 type=file mime=application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  "after: object=e5f6a7b8 size=8442 type=file mime=application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  "",
].join("\n");

const FIXTURE_DURABLE_KIND_CHANGED = [
  "diff -- /policy",
  "reason: path kind changed; text diff is not available",
  "before: object=<none> size=0 type=directory mime=<unset>",
  "after: object=deadbeef size=42 type=file mime=text/markdown",
  "",
].join("\n");

const FIXTURE_DURABLE_TOO_LARGE = [
  "diff -- /case-files/transcript.txt",
  "reason: text diff is too large to render",
  "before: object=11111111 size=2097152 type=file mime=text/plain",
  "after: object=22222222 size=2098000 type=file mime=text/plain",
  "",
].join("\n");

/** Multi-file body — exercises the per-file block splitter. */
const FIXTURE_MULTI = [
  FIXTURE_GROUPED_TEXT.trimEnd(),
  FIXTURE_LEGACY_BINARY.trimEnd(),
  FIXTURE_METADATA.trimEnd(),
  "",
].join("\n");

const FIXTURE_EMPTY = "No changes.\n";

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

describe("diff-parser — empty + sentinel", () => {
  it("treats `No changes.` as the empty state", () => {
    const { fragments, isEmpty } = parseDiff(FIXTURE_EMPTY);
    expect(isEmpty).toBe(true);
    expect(fragments).toHaveLength(0);
  });

  it("treats truly empty bodies as the empty state", () => {
    expect(parseDiff("").isEmpty).toBe(true);
  });
});

describe("diff-parser — text-unified (grouped)", () => {
  it("parses a single grouped hunk with correct positions and counts", () => {
    const { fragments } = parseDiff(FIXTURE_GROUPED_TEXT);
    expect(fragments).toHaveLength(1);
    const fr = fragments[0]!;
    expect(fr.kind).toBe("text-unified");
    if (fr.kind !== "text-unified") throw new Error("unreachable");
    expect(fr.path).toBe("/a.md");
    expect(fr.added).toBe(1);
    expect(fr.removed).toBe(1);
    expect(fr.hunks).toHaveLength(1);
    const hunk = fr.hunks[0]!;
    expect(hunk.beforeStart).toBe(5);
    expect(hunk.beforeCount).toBe(7);
    expect(hunk.afterStart).toBe(5);
    expect(hunk.afterCount).toBe(7);
    // Lines: 3 context + 1 remove + 1 add + 3 context = 8
    expect(hunk.lines).toHaveLength(8);
    expect(hunk.lines[3]).toEqual({ type: "remove", text: "before" });
    expect(hunk.lines[4]).toEqual({ type: "add", text: "after" });
  });
});

describe("diff-parser — text-unified (legacy single-hunk)", () => {
  it("synthesizes positions when only @@ is present", () => {
    const { fragments } = parseDiff(FIXTURE_LEGACY_TEXT);
    expect(fragments).toHaveLength(1);
    const fr = fragments[0]!;
    if (fr.kind !== "text-unified") throw new Error("expected text-unified");
    expect(fr.path).toBe("/a.md");
    expect(fr.hunks).toHaveLength(1);
    const hunk = fr.hunks[0]!;
    expect(hunk.beforeStart).toBe(1);
    expect(hunk.afterStart).toBe(1);
    expect(hunk.lines.map((l) => l.type)).toEqual(["context", "remove", "add", "context"]);
    expect(fr.added).toBe(1);
    expect(fr.removed).toBe(1);
  });
});

describe("diff-parser — metadata-only", () => {
  it("collects each changed field as a before/after pair", () => {
    const { fragments } = parseDiff(FIXTURE_METADATA);
    expect(fragments).toHaveLength(1);
    const fr = fragments[0]!;
    if (fr.kind !== "metadata-only") throw new Error("expected metadata-only");
    expect(fr.path).toBe("/config.json");
    expect(fr.changes).toEqual([
      { field: "mode", before: "0644", after: "0755" },
      { field: "uid", before: "1", after: "7" },
      { field: "mime_type", before: "text/plain", after: "application/json" },
      { field: "custom_attrs.owner", before: "alice", after: "bob" },
    ]);
  });
});

describe("diff-parser — legacy binary + too-large", () => {
  it("recognises the legacy in-memory binary marker", () => {
    const { fragments } = parseDiff(FIXTURE_LEGACY_BINARY);
    expect(fragments).toHaveLength(1);
    expect(fragments[0]!.kind).toBe("binary");
    expect(fragments[0]!.path).toBe("/assets/logo.png");
    if (fragments[0]!.kind === "binary") {
      expect(fragments[0]!.summary).toBeNull();
    }
  });

  it("recognises the legacy in-memory too-large marker", () => {
    const { fragments } = parseDiff(FIXTURE_LEGACY_TOO_LARGE);
    expect(fragments).toHaveLength(1);
    expect(fragments[0]!.kind).toBe("too-large");
  });
});

describe("diff-parser — durable structured summaries", () => {
  it("parses a durable binary summary with object/size/mime", () => {
    const { fragments } = parseDiff(FIXTURE_DURABLE_BINARY);
    const fr = fragments[0]!;
    if (fr.kind !== "binary") throw new Error("expected binary");
    expect(fr.summary).not.toBeNull();
    expect(fr.summary!.before.object).toBe("a1b2c3d4");
    expect(fr.summary!.before.size).toBe(8421);
    expect(fr.summary!.before.type).toBe("file");
    expect(fr.summary!.after.size).toBe(8442);
    expect(fr.summary!.after.mime).toContain("officedocument");
  });

  it("parses a kind-changed summary with absent/present sides", () => {
    const { fragments } = parseDiff(FIXTURE_DURABLE_KIND_CHANGED);
    const fr = fragments[0]!;
    if (fr.kind !== "kind-changed") throw new Error("expected kind-changed");
    expect(fr.summary!.before.type).toBe("directory");
    expect(fr.summary!.before.object).toBeNull();
    expect(fr.summary!.before.mime).toBeNull();
    expect(fr.summary!.after.type).toBe("file");
    expect(fr.summary!.after.object).toBe("deadbeef");
  });

  it("parses a durable too-large summary", () => {
    const { fragments } = parseDiff(FIXTURE_DURABLE_TOO_LARGE);
    expect(fragments[0]!.kind).toBe("too-large");
  });
});

describe("diff-parser — multi-file body", () => {
  it("splits a body with text + binary + metadata into three fragments in order", () => {
    const { fragments } = parseDiff(FIXTURE_MULTI);
    expect(fragments.map((f) => f.kind)).toEqual(["text-unified", "binary", "metadata-only"]);
    expect(fragments.map((f) => f.path)).toEqual(["/a.md", "/assets/logo.png", "/config.json"]);
  });
});
