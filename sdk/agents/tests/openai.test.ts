import { describe, expect, it } from "vitest";
import type { ShellAction } from "@openai/agents";
import { StratumHttpError } from "@stratum/sdk";
import { StratumEditor, StratumShell } from "../src/openai/index.js";
import { createFakeWorkspace } from "./helpers.js";

describe("StratumEditor", () => {
  it("createFile applies a create-mode diff and auto-creates missing parents", async () => {
    const fake = createFakeWorkspace();
    const editor = new StratumEditor(fake.workspace);

    const result = await editor.createFile({
      type: "create_file",
      path: "/data/sub/file.txt",
      diff: "+hello world\n+\n",
    });

    expect(result).toEqual({ status: "completed" });
    expect(fake.read("/data/sub/file.txt")).toBe("hello world\n");
  });

  it("updateFile applies a diff to existing content", async () => {
    const fake = createFakeWorkspace({ files: { "/notes.txt": "one\ntwo\nthree\n" } });
    const editor = new StratumEditor(fake.workspace);

    const result = await editor.updateFile({
      type: "update_file",
      path: "/notes.txt",
      diff: "@@\n one\n-two\n+TWO\n three\n",
    });

    expect(result).toEqual({ status: "completed" });
    expect(fake.read("/notes.txt")).toBe("one\nTWO\nthree\n");
  });

  it("updateFile returns failed for a missing path", async () => {
    const fake = createFakeWorkspace();
    const editor = new StratumEditor(fake.workspace);

    const result = await editor.updateFile({
      type: "update_file",
      path: "/nope.txt",
      diff: "@@\n+x\n",
    });

    expect(result).toEqual({ status: "failed", output: "File not found: /nope.txt" });
  });

  it("updateFile reports unavailable read capability explicitly", async () => {
    const fake = createFakeWorkspace({
      files: { "/notes.txt": "one\ntwo\n" },
      capabilityOverrides: (manifest) => ({
        ...manifest,
        routes: {
          ...manifest.routes,
          filesystem: {
            ...manifest.routes.filesystem,
            read: { ...manifest.routes.filesystem.read, available: false },
          },
        },
      }),
    });
    const editor = new StratumEditor(fake.workspace);

    const result = await editor.updateFile({
      type: "update_file",
      path: "/notes.txt",
      diff: "@@\n-one\n+ONE\n two\n",
    });

    expect(result).toEqual({ status: "failed", output: "filesystem.read is unavailable for this Stratum workspace." });
  });

  it("createFile masks raw backend write errors", async () => {
    const fake = createFakeWorkspace({
      writeError: new StratumHttpError(500, "deploy --token=SECRET stdout /Users/raj/backing"),
    });
    const editor = new StratumEditor(fake.workspace);

    const result = await editor.createFile({
      type: "create_file",
      path: "/out.txt",
      diff: "+hello\n",
    });

    expect(result).toEqual({ status: "failed", output: "Stratum filesystem operation failed." });
  });

  it("deleteFile removes files through the mounted volume", async () => {
    const fake = createFakeWorkspace({ files: { "/gone.txt": "bye" } });
    const editor = new StratumEditor(fake.workspace);

    const result = await editor.deleteFile({ type: "delete_file", path: "/gone.txt" });

    expect(result).toEqual({ status: "completed" });
    expect(fake.has("/gone.txt")).toBe(false);
  });

  it("deleteFile reports unavailable delete capability without deleting", async () => {
    const fake = createFakeWorkspace({
      files: { "/gone.txt": "bye" },
      capabilityOverrides: (manifest) => ({
        ...manifest,
        routes: {
          ...manifest.routes,
          filesystem: {
            ...manifest.routes.filesystem,
            delete: { ...manifest.routes.filesystem.delete, available: false },
          },
        },
      }),
    });
    const editor = new StratumEditor(fake.workspace);

    const result = await editor.deleteFile({ type: "delete_file", path: "/gone.txt" });

    expect(result).toEqual({ status: "failed", output: "filesystem.delete is unavailable for this Stratum workspace." });
    expect(fake.has("/gone.txt")).toBe(true);
  });

  it("deleteFile returns failed for a missing path", async () => {
    const fake = createFakeWorkspace();
    const editor = new StratumEditor(fake.workspace);

    const result = await editor.deleteFile({ type: "delete_file", path: "/missing.txt" });

    expect(result).toEqual({ status: "failed", output: "File not found: /missing.txt" });
  });
});

describe("StratumShell", () => {
  it("runs one command and maps stdout/stderr/exit code", async () => {
    const fake = createFakeWorkspace({
      executeAvailable: true,
      execute: () => ({ stdout: "ok\n", stderr: "warn\n", exitCode: 0 }),
    });
    const shell = new StratumShell(fake.workspace);

    const result = await shell.run({ commands: ["echo ok"] } satisfies ShellAction);

    expect(result.output).toEqual([
      { stdout: "ok\n", stderr: "warn\n", outcome: { type: "exit", exitCode: 0 } },
    ]);
    expect(fake.executeCalls.map((c) => c.command)).toEqual(["echo ok"]);
  });

  it("runs multiple commands in order", async () => {
    const fake = createFakeWorkspace({
      executeAvailable: true,
      execute: (command) => ({ stdout: `out:${command}`, stderr: "", exitCode: command === "bad" ? 1 : 0 }),
    });
    const shell = new StratumShell(fake.workspace);

    const result = await shell.run({ commands: ["first", "bad", "third"] } satisfies ShellAction);

    expect(result.output.map((o) => o.stdout)).toEqual(["out:first", "out:bad", "out:third"]);
    expect(result.output[1]?.outcome).toEqual({ type: "exit", exitCode: 1 });
    expect(fake.executeCalls.map((c) => c.command)).toEqual(["first", "bad", "third"]);
  });

  it("fails explicitly when execution is unavailable without leaking the command", async () => {
    const fake = createFakeWorkspace({ executeAvailable: false });
    const shell = new StratumShell(fake.workspace);

    let message = "";
    try {
      await shell.run({ commands: ["deploy --token=SUPER_SECRET"] } satisfies ShellAction);
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    }
    expect(message).toContain("Stratum execution is unavailable");
    expect(message).not.toContain("SUPER_SECRET");
    expect(message).not.toContain("deploy");
    expect(fake.executeCalls).toHaveLength(0);
  });

  it("masks raw backend execution errors", async () => {
    const fake = createFakeWorkspace({
      executeAvailable: true,
      execute: () => {
        throw new StratumHttpError(500, "deploy --token=SECRET stdout /tmp/stratum");
      },
    });
    const shell = new StratumShell(fake.workspace);

    let message = "";
    try {
      await shell.run({ commands: ["deploy --token=SECRET"] } satisfies ShellAction);
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    }

    expect(message).toBe("Stratum execution failed.");
    expect(message).not.toContain("SECRET");
    expect(message).not.toContain("stdout");
  });
});
