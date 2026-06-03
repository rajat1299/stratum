import type { FileInfo, GrepMatch } from "deepagents";
import type { StratumGrepResult } from "@stratum/sdk";
import type { StratumWorkspaceEntry } from "../workspace.js";

/** Map Stratum SDK grep results into deepagents `GrepMatch[]` (no shell parsing). */
export function toGrepMatches(result: StratumGrepResult): GrepMatch[] {
  return result.results.map((match) => ({
    path: match.file,
    line: match.line_num,
    text: match.line,
  }));
}

/** Map mounted-workspace directory entries into deepagents `FileInfo[]`. */
export function toFileInfos(entries: readonly StratumWorkspaceEntry[]): FileInfo[] {
  return entries.map((entry) => ({ path: entry.path, is_dir: entry.is_dir }));
}

/** Map glob match paths (always files) into deepagents `FileInfo[]`. */
export function globToFileInfos(paths: readonly string[]): FileInfo[] {
  return paths.map((path) => ({ path, is_dir: false }));
}

/** Convert a Stratum numeric unix-seconds timestamp into an ISO 8601 string. */
export function unixSecondsToIso(seconds: number): string {
  if (!Number.isFinite(seconds)) return new Date(0).toISOString();
  return new Date(seconds * 1000).toISOString();
}
