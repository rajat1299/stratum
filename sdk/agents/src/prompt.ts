export const STRATUM_SYSTEM_PROMPT = `You are working in a mounted Stratum workspace exposed through the Stratum HTTP API.

Paths are workspace-relative Unix-style paths, but this is not a POSIX shell or a local filesystem mount. Use the provided Stratum tools to read, list, write, and edit files. Use execution only when the workspace capability manifest says Stratum execution is available.

Stratum records execution under /runs/<run-id>/ when execution is enabled. Do not claim access to arbitrary host files, host environment variables, network credentials, or local shell state.`;

export interface BuildStratumSystemPromptOptions {
  readonly mountInfo?: Record<string, string>;
  readonly extraInstructions?: string;
}

export function buildStratumSystemPrompt(options: BuildStratumSystemPromptOptions = {}): string {
  const parts = [STRATUM_SYSTEM_PROMPT];
  const entries = Object.entries(options.mountInfo ?? {});
  if (entries.length > 0) {
    parts.push("");
    parts.push("Mounted workspace areas:");
    for (const [path, description] of entries) {
      parts.push(`- ${path}: ${description}`);
    }
  }
  if (options.extraInstructions !== undefined && options.extraInstructions.length > 0) {
    parts.push("");
    parts.push(options.extraInstructions);
  }
  return parts.join("\n");
}
