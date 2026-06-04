import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { grepCommand, statusCommand } from "../src/commands.js";

type ConformanceFixture = {
  revision: string;
  modes: string[];
  cases: Array<{
    id: string;
    method: string;
    path: string;
    sdk: { bash: string | null };
  }>;
};

const fixture = JSON.parse(
  readFileSync(
    fileURLToPath(new URL("../../contracts/conformance.routes.v1.json", import.meta.url)),
    "utf8",
  ),
) as ConformanceFixture;

describe("bash sdk conformance fixture", () => {
  it("loads revision and case ids from the shared fixture", () => {
    expect(fixture.revision).toBe("2026-06-04-1");
    expect(fixture.modes).toContain("durable-cloud");
    expect(fixture.cases.some((entry) => entry.id === "unsupported.durable.workspaces")).toBe(
      true,
    );
  });

  it("documents bash sdk gaps explicitly in the fixture", () => {
    const bashMapped = fixture.cases.filter((entry) => entry.sdk.bash !== null);
    expect(bashMapped).toEqual([]);
  });

  it("keeps command builders registered for search and vcs route families", () => {
    const names = [grepCommand, statusCommand].map((command) => (command as { name: string }).name);
    expect(names).toEqual(["grep", "status"]);
  });
});
