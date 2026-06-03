// Provider-free root exports for the Stratum agent adapter pack.
//
// Framework-specific adapters are reached through the subpath exports
// (`@stratum/agents/openai`, `/vercel`, `/langchain`, `/mastra`) so consumers do
// not need every harness installed to use one adapter.

export { STRATUM_SYSTEM_PROMPT, buildStratumSystemPrompt } from "./prompt.js";
export type { BuildStratumSystemPromptOptions } from "./prompt.js";

export {
  StratumAgentWorkspace,
  joinWorkspacePath,
  parentOf,
  isHttpNotFound,
} from "./workspace.js";
export type {
  StratumAgentWorkspaceOptions,
  StratumAgentExecuteResult,
  StratumWorkspaceEntry,
} from "./workspace.js";

export {
  classifyMime,
  extensionOf,
  isPresentableBinaryMime,
  isTextMime,
  mimeForPath,
  resolveMimeType,
} from "./mime.js";
export type { StratumContentClass } from "./mime.js";
