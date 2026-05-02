import { defineCommand } from "just-bash";
import { normalizePath } from "./path-index.js";
import type { StratumFs } from "./stratum-fs.js";

function getFsVolume(ctxFs: unknown) {
  const fs = ctxFs as Partial<StratumFs>;
  return fs.volume;
}

export const statusCommand = defineCommand("status", async (_args, ctx) => {
  const volume = getFsVolume(ctx.fs);
  if (!volume) return { stdout: "", stderr: "status: Stratum volume is unavailable\n", exitCode: 1 };

  try {
    return { stdout: await volume.status(), stderr: "", exitCode: 0 };
  } catch (error) {
    return { stdout: "", stderr: `status: ${(error as Error).message}\n`, exitCode: 1 };
  }
});

export const diffCommand = defineCommand("diff", async (args, ctx) => {
  const volume = getFsVolume(ctx.fs);
  if (!volume) return { stdout: "", stderr: "diff: Stratum volume is unavailable\n", exitCode: 1 };
  if (args.length > 1) return { stdout: "", stderr: "diff: usage: diff [path]\n", exitCode: 2 };

  try {
    const path = args[0] === undefined ? undefined : normalizePath(args[0], ctx.cwd);
    return { stdout: await volume.diff(path), stderr: "", exitCode: 0 };
  } catch (error) {
    return { stdout: "", stderr: `diff: ${(error as Error).message}\n`, exitCode: 1 };
  }
});

export const commitCommand = defineCommand("commit", async (args, ctx) => {
  const volume = getFsVolume(ctx.fs);
  if (!volume) return { stdout: "", stderr: "commit: Stratum volume is unavailable\n", exitCode: 1 };

  const message = args.join(" ").trim();
  if (message === "") {
    return { stdout: "", stderr: "commit: usage: commit <message>\n", exitCode: 2 };
  }

  try {
    const result = await volume.commit(message);
    return { stdout: `${result.hash} ${result.message}\n`, stderr: "", exitCode: 0 };
  } catch (error) {
    return { stdout: "", stderr: `commit: ${(error as Error).message}\n`, exitCode: 1 };
  }
});

export const grepCommand = defineCommand("grep", async (args, ctx) => {
  const volume = getFsVolume(ctx.fs);
  if (!volume) return { stdout: "", stderr: "grep: Stratum volume is unavailable\n", exitCode: 1 };
  if (args.length === 0) return { stdout: "", stderr: "grep: usage: grep <pattern> [path]\n", exitCode: 2 };
  if (args.length > 2) return { stdout: "", stderr: "grep: usage: grep <pattern> [path]\n", exitCode: 2 };
  if (args[0]?.startsWith("-")) return { stdout: "", stderr: "grep: flags are not supported\n", exitCode: 2 };

  try {
    const path = normalizePath(args[1] ?? ".", ctx.cwd);
    const result = await volume.grep(args[0] ?? "", path);
    const stdout = result.results
      .map((match) => `${match.file}:${match.line_num}:${oneLine(match.line)}`)
      .join("\n");
    return { stdout: stdout === "" ? "" : `${stdout}\n`, stderr: "", exitCode: result.results.length === 0 ? 1 : 0 };
  } catch (error) {
    return { stdout: "", stderr: `grep: ${(error as Error).message}\n`, exitCode: 1 };
  }
});

export const sgrepCommand = defineCommand("sgrep", async () => ({
  stdout: "",
  stderr: "sgrep: semantic search is not available in @stratum/bash yet\n",
  exitCode: 2,
}));

export const stratumCommands = [statusCommand, diffCommand, commitCommand, grepCommand, sgrepCommand];

function oneLine(value: string): string {
  return value.replace(/\r/g, "\\r").replace(/\n/g, "\\n");
}
