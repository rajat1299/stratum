import { describe, expect, it } from "vitest";
import { STRATUM_SYSTEM_PROMPT, buildStratumSystemPrompt } from "../src/index.js";

describe("buildStratumSystemPrompt", () => {
  it("describes Stratum as a mounted HTTP workspace, not POSIX or a host shell", () => {
    expect(STRATUM_SYSTEM_PROMPT).toContain("mounted Stratum workspace");
    expect(STRATUM_SYSTEM_PROMPT).toContain("HTTP API");
    expect(STRATUM_SYSTEM_PROMPT).toContain("not a POSIX shell");
    expect(STRATUM_SYSTEM_PROMPT).not.toContain("FUSE");
  });

  it("adds mounted workspace info and extra instructions", () => {
    const out = buildStratumSystemPrompt({
      mountInfo: { "/": "Case review workspace", "/runs": "Durable run artifacts" },
      extraInstructions: "Keep edits minimal.",
    });
    expect(out).toContain("- /: Case review workspace");
    expect(out).toContain("- /runs: Durable run artifacts");
    expect(out.endsWith("Keep edits minimal.")).toBe(true);
  });

  it("omits empty optional sections", () => {
    expect(buildStratumSystemPrompt({ mountInfo: {}, extraInstructions: "" })).toBe(STRATUM_SYSTEM_PROMPT);
  });
});
