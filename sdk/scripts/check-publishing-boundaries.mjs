#!/usr/bin/env node
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const sdkRoot = join(scriptDir, "..");

let failed = false;

function fail(message) {
  console.error(message);
  failed = true;
}

function readJson(relativePath) {
  return JSON.parse(readFileSync(join(sdkRoot, relativePath), "utf8"));
}

function readText(relativePath) {
  return readFileSync(join(sdkRoot, relativePath), "utf8");
}

function expectEqual(actual, expected, label) {
  const actualJson = JSON.stringify(actual);
  const expectedJson = JSON.stringify(expected);
  if (actualJson !== expectedJson) {
    fail(`${label}: expected ${expectedJson}, got ${actualJson}`);
  }
}

function expectPresent(condition, label) {
  if (!condition) {
    fail(label);
  }
}

function tomlString(source, key) {
  const match = source.match(new RegExp(`^${key}\\s*=\\s*"([^"]+)"`, "m"));
  return match?.[1] ?? null;
}

function matrixEntry(matrix, name) {
  const found = matrix.packages?.find((entry) => entry.name === name);
  if (!found) {
    fail(`version matrix is missing ${name}`);
  }
  return found;
}

const matrixPath = join(sdkRoot, "version-matrix.json");
if (!existsSync(matrixPath)) {
  fail("Missing sdk/version-matrix.json");
} else {
  const matrix = readJson("version-matrix.json");
  expectEqual(matrix.schema_version, 1, "matrix schema version");
  expectEqual(matrix.release_channel, "private-beta", "matrix release channel");

  const sdkMatrix = matrixEntry(matrix, "@stratum/sdk");
  const agentsMatrix = matrixEntry(matrix, "@stratum/agents");
  const pythonMatrix = matrixEntry(matrix, "stratum-sdk");

  const sdkPackage = readJson("typescript/package.json");
  expectEqual(sdkPackage.name, "@stratum/sdk", "TypeScript SDK package name");
  expectEqual(sdkPackage.version, sdkMatrix?.version, "TypeScript SDK package version");
  expectEqual(sdkPackage.private, undefined, "TypeScript SDK must be publishable");
  expectEqual(sdkPackage.exports, {
    ".": {
      types: "./dist/index.d.ts",
      import: "./dist/index.js",
    },
  }, "TypeScript SDK exports");
  expectEqual(sdkPackage.files, ["dist"], "TypeScript SDK package files");
  expectEqual(sdkPackage.scripts?.build, "tsc -p tsconfig.json", "TypeScript SDK build script");
  expectEqual(sdkPackage.scripts?.prepare, "tsc -p tsconfig.json", "TypeScript SDK prepare script");
  expectEqual(sdkPackage.scripts?.prepack, "tsc -p tsconfig.json", "TypeScript SDK prepack script");

  const agentsPackage = readJson("agents/package.json");
  expectEqual(agentsPackage.name, "@stratum/agents", "Agents package name");
  expectEqual(agentsPackage.version, agentsMatrix?.version, "Agents package version");
  expectEqual(agentsPackage.private, undefined, "Agents package must be publishable");
  expectEqual(agentsPackage.dependencies?.["@stratum/sdk"], sdkMatrix?.version, "Agents SDK dependency");
  expectEqual(agentsPackage.exports, {
    ".": { types: "./dist/index.d.ts", import: "./dist/index.js" },
    "./openai": { types: "./dist/openai/index.d.ts", import: "./dist/openai/index.js" },
    "./vercel": { types: "./dist/vercel/index.d.ts", import: "./dist/vercel/index.js" },
    "./langchain": { types: "./dist/langchain/index.d.ts", import: "./dist/langchain/index.js" },
    "./mastra": { types: "./dist/mastra/index.d.ts", import: "./dist/mastra/index.js" },
  }, "Agents package exports");
  expectEqual(agentsPackage.files, ["dist", "README.md"], "Agents package files");
  expectEqual(agentsPackage.scripts?.build, "tsc -p tsconfig.json", "Agents build script");
  expectEqual(agentsPackage.scripts?.prepack, "tsc -p tsconfig.json", "Agents prepack script");
  expectEqual(agentsPackage.scripts?.prepare, undefined, "Agents prepare script should not run for workspace installs");

  const pythonPyproject = readText("python/pyproject.toml");
  const pythonInit = readText("python/src/stratum_sdk/__init__.py");
  expectEqual(tomlString(pythonPyproject, "name"), "stratum-sdk", "Python package name");
  expectEqual(tomlString(pythonPyproject, "version"), pythonMatrix?.version, "Python package version");
  expectPresent(
    pythonInit.includes(`__version__ = "${pythonMatrix?.version}"`),
    "Python runtime __version__ must match matrix",
  );
  expectPresent(
    pythonPyproject.includes('packages = ["src/stratum_sdk"]'),
    "Python wheel package boundary must be src/stratum_sdk",
  );
  expectPresent(
    pythonPyproject.includes('"src/stratum_sdk"') &&
      pythonPyproject.includes('"README.md"') &&
      pythonPyproject.includes('"LICENSE"'),
    "Python sdist boundary must include package, README, and LICENSE",
  );
  expectPresent(existsSync(join(sdkRoot, "python/src/stratum_sdk/py.typed")), "Python package must include py.typed");
  expectEqual(pythonMatrix?.python_import, "stratum_sdk", "Python import name");
  expectEqual(pythonMatrix?.version, "0.0.0b0", "Python beta version must use PEP 440 spelling");
}

if (failed) {
  process.exit(1);
}

console.log("SDK publishing boundary contract passed.");
