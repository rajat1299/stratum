import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

type PackageJson = {
  exports?: Record<string, { types?: string; import?: string }>;
  files?: string[];
  scripts?: Record<string, string>;
};

const packageJson = JSON.parse(
  readFileSync(fileURLToPath(new URL("../package.json", import.meta.url)), "utf8"),
) as PackageJson;

describe("package boundary", () => {
  it("publishes only the built dist entrypoint", () => {
    expect(packageJson.exports?.["."]).toEqual({
      types: "./dist/index.d.ts",
      import: "./dist/index.js",
    });
    expect(packageJson.files).toEqual(["dist"]);
  });

  it("builds dist during package lifecycle without requiring Bun", () => {
    expect(packageJson.scripts?.build).toBe("tsc -p tsconfig.json");
    expect(packageJson.scripts?.prepare).toBe("tsc -p tsconfig.json");
    expect(packageJson.scripts?.prepack).toBe("tsc -p tsconfig.json");
  });
});
