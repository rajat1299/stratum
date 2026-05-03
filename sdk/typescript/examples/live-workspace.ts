/**
 * Runnable from the repo: `cd sdk/typescript && bun run examples/live-workspace.ts`
 *
 * Requires `stratum-server` already listening and:
 *   STRATUM_SDK_LIVE=1
 *   STRATUM_SDK_LIVE_BASE_URL
 *   STRATUM_SDK_LIVE_ADMIN_USER
 *   STRATUM_SDK_LIVE_AGENT_TOKEN
 *
 * Prints workspace id and path only (never the workspace bearer token).
 */
import { StratumClient } from "../src/index.js";

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
  const name = `example-${suffix}`;
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

  const client = new StratumClient({
    baseUrl,
    auth: {
      type: "workspace",
      workspaceId: workspace.id,
      workspaceToken: issued.workspace_token,
    },
  });

  await client.fs.mkdir("/docs");
  await client.fs.writeFile("/docs/README.md", "hello from TypeScript live example");

  const direct = await client.fs.readFile("/docs/README.md");
  const volume = client.mount({ cwd: "/" });
  const mounted = await volume.readFile("/docs/README.md");

  console.log(
    JSON.stringify({
      workspaceId: workspace.id,
      workspaceRoot: rootPath,
      readMatches: direct === mounted,
      bytesRead: direct.length,
    }),
  );
}

main().catch((err: unknown) => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
