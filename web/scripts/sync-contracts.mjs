#!/usr/bin/env node
/**
 * sync-contracts — copy the SDK's checked-in capability fixtures into the
 * web workspace's __fixtures__ directory.
 *
 * Why: the manifest contract is owned by backend at sdk/contracts/. The
 * web frontend mirrors those files into web/src/lib/__fixtures__/ so Vite
 * doesn't have to relax its project-root constraint. This script enforces
 * the mirror automatically on dev/test/build — no more hand-copies after
 * a backend regen, no more "drift check" CI noise.
 *
 * Tolerant by design: if the SDK contracts don't exist (fresh checkout
 * before SDK build), we warn and skip rather than failing the build.
 * The drift test at __fixtures__/capabilities-sync.test.ts is the last
 * line of defense if both directories are present but stale.
 */

import { copyFileSync, existsSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const srcRoot = resolve(here, "../../sdk/contracts");
const destRoot = resolve(here, "../src/lib/__fixtures__");

const FILES = ["capabilities.v1.json", "capabilities.v1.durable-cloud.json"];

mkdirSync(destRoot, { recursive: true });

if (!existsSync(srcRoot)) {
  console.warn(`[sync-contracts] sdk/contracts not found at ${srcRoot} — skipping.`);
  console.warn("[sync-contracts] Run `bun run build` in sdk/typescript first if the SDK is not built.");
  process.exit(0);
}

let copied = 0;
let skipped = 0;
for (const name of FILES) {
  const src = resolve(srcRoot, name);
  const dest = resolve(destRoot, name);
  if (!existsSync(src)) {
    console.warn(`[sync-contracts] skip ${name} — not in sdk/contracts`);
    skipped++;
    continue;
  }
  copyFileSync(src, dest);
  copied++;
}

console.log(`[sync-contracts] ${copied} copied, ${skipped} skipped`);
