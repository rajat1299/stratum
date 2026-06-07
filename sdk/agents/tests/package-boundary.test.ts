import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

type PackageJson = {
  exports?: Record<string, { types?: string; import?: string }>;
  files?: string[];
  scripts?: Record<string, string>;
  version?: string;
  dependencies?: Record<string, string>;
  peerDependencies?: Record<string, string>;
  peerDependenciesMeta?: Record<string, { optional?: boolean }>;
  devDependencies?: Record<string, string>;
};

const packageJson = JSON.parse(
  readFileSync(fileURLToPath(new URL("../package.json", import.meta.url)), "utf8"),
) as PackageJson;
const sdkPackageJson = JSON.parse(
  readFileSync(fileURLToPath(new URL("../../typescript/package.json", import.meta.url)), "utf8"),
) as PackageJson;
const rootExportSource = readFileSync(fileURLToPath(new URL("../src/index.ts", import.meta.url)), "utf8");

describe("agents package boundary", () => {
  it("publishes root and adapter subpath exports from dist only", () => {
    expect(packageJson.exports).toEqual({
      ".": { types: "./dist/index.d.ts", import: "./dist/index.js" },
      "./openai": { types: "./dist/openai/index.d.ts", import: "./dist/openai/index.js" },
      "./vercel": { types: "./dist/vercel/index.d.ts", import: "./dist/vercel/index.js" },
      "./langchain": { types: "./dist/langchain/index.d.ts", import: "./dist/langchain/index.js" },
      "./mastra": { types: "./dist/mastra/index.d.ts", import: "./dist/mastra/index.js" },
    });
    expect(packageJson.files).toEqual(["dist", "README.md"]);
  });

  it("pins beta target harness versions and marks framework peers optional", () => {
    expect(packageJson.scripts?.prepare).toBeUndefined();
    expect(packageJson.scripts?.build).toBe("tsc -p tsconfig.json");
    expect(packageJson.scripts?.prepack).toBe("tsc -p tsconfig.json");
    expect(sdkPackageJson.version).toBe("0.0.0-beta.0");
    expect(packageJson.dependencies?.["@stratum/sdk"]).toBe(sdkPackageJson.version);
    expect(packageJson.peerDependencies).toMatchObject({
      "@openai/agents": ">=0.11.6 <0.12.0",
      ai: ">=6.0.195 <6.1.0",
      deepagents: ">=1.10.2 <1.11.0",
      "@mastra/core": ">=1.38.0 <1.39.0",
      zod: ">=4.4.3 <5.0.0",
    });
    for (const name of ["@openai/agents", "ai", "deepagents", "@mastra/core", "zod"]) {
      expect(packageJson.peerDependenciesMeta?.[name]?.optional).toBe(true);
    }
    expect(packageJson.devDependencies).toMatchObject({
      "@openai/agents": "0.11.6",
      ai: "6.0.195",
      deepagents: "1.10.2",
      "@mastra/core": "1.38.0",
      zod: "4.4.3",
    });
  });

  it("keeps the root export free of optional framework peer imports", () => {
    for (const name of ["@openai/agents", "ai", "deepagents", "@mastra/core", "zod"]) {
      expect(rootExportSource).not.toContain(`"${name}"`);
      expect(rootExportSource).not.toContain(`'${name}'`);
    }
  });
});
