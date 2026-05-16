/**
 * Drift check — assert the mirrored fixtures in this directory are byte-equal
 * to the source of truth under `sdk/contracts/`.
 *
 * Per `docs/plans/2026-05-15-capability-manifest-v1-lock.md`: the v1 contract
 * is owned by backend; we mirror their checked-in JSON fixture so Vite doesn't
 * have to reach across the project-root boundary. This test is the enforcement
 * — if backend regenerates `sdk/contracts/capabilities.v1.json` and ours
 * doesn't track, CI fails loudly with a single command to fix:
 *
 *   cp sdk/contracts/capabilities.v1.json \
 *      web/src/lib/__fixtures__/capabilities.v1.json
 *   cp sdk/contracts/capabilities.v1.durable-cloud.json \
 *      web/src/lib/__fixtures__/capabilities.v1.durable-cloud.json
 *
 * This file needs node fs to reach the sibling directory; override env per-file.
 */
// @vitest-environment node

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { describe, expect, it } from "vitest";

const here = dirname(fileURLToPath(import.meta.url));
const sdkRoot = resolve(here, "../../../../sdk/contracts");
const webRoot = here;

const PAIRS = [
  "capabilities.v1.json",
  "capabilities.v1.durable-cloud.json",
] as const;

describe("capabilities fixtures — sync with sdk/contracts/", () => {
  for (const name of PAIRS) {
    it(`${name} matches sdk/contracts byte-for-byte`, () => {
      const sdk = readFileSync(resolve(sdkRoot, name), "utf8");
      const mirrored = readFileSync(resolve(webRoot, name), "utf8");
      // Helpful diff if this trips: vitest prints the structural diff.
      expect(JSON.parse(mirrored)).toEqual(JSON.parse(sdk));
      // Strict byte-equality after newline normalization (covers trailing-NL drift).
      expect(mirrored.replace(/\r\n/g, "\n")).toBe(sdk.replace(/\r\n/g, "\n"));
    });
  }
});
