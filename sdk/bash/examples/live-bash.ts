/**
 * Runnable from the repo: `cd sdk/bash && bun run examples/live-bash.ts`
 *
 * Same environment contract as `sdk/typescript/examples/live-workspace.ts`.
 * Creates a workspace, mints a workspace token, writes a file, then runs a few
 * virtual shell commands. Does not print secrets.
 */
import { StratumClient } from "@stratum/sdk";
import { createBash } from "../src/index.js";

function requireEnv(name: string): string {
  const v = process.env[name]?.trim();
  if (!v) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return v;
}

async function main(): Promise<void> {
  if (process.env.STRATUM_SDK_LIVE !== "1") {
    console.error("Refusing to run: set STRATUM_SDK_LIVE=1");
    process.exit(1);
  }

  const baseUrl = requireEnv("STRATUM_SDK_LIVE_BASE_URL");
  const adminUser = requireEnv("STRATUM_SDK_LIVE_ADMIN_USER");
  const agentToken = requireEnv("STRATUM_SDK_LIVE_AGENT_TOKEN");

  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  const name = `example-bash-${suffix}`;
  const rootPath = `/sdk-smoke/${suffix}`;

  const admin = new StratumClient({
    baseUrl,
    auth: { type: "user", username: adminUser },
  });

  const workspace = await admin.workspaces.create({ name, root_path: rootPath });
  const issued = await admin.workspaces.issueToken(workspace.id, {
    name: `${name}-token`,
    agent_token: agentToken,
    read_prefixes: [rootPath],
    write_prefixes: [rootPath],
  });

  const writer = new StratumClient({
    baseUrl,
    auth: {
      type: "workspace",
      workspaceId: workspace.id,
      workspaceToken: issued.workspace_token,
    },
  });

  await writer.fs.mkdir("/docs");
  await writer.fs.writeFile("/docs/README.md", "hello from bash live example");

  const { bash, refresh } = await createBash({
    baseUrl,
    workspaceId: workspace.id,
    workspaceToken: issued.workspace_token,
  });

  await refresh();

  const cat = await bash.exec("cat /docs/README.md");
  const pwd = await bash.exec("pwd");
  const sgrep = await bash.exec("sgrep test");

  console.log(
    JSON.stringify({
      workspaceId: workspace.id,
      workspaceRoot: rootPath,
      pwd: pwd.stdout.trim(),
      catExit: cat.exitCode,
      catBytes: cat.stdout.length,
      sgrepExit: sgrep.exitCode,
      sgrepUnsupported: sgrep.stderr.includes("semantic search"),
    }),
  );
}

main().catch((err: unknown) => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
