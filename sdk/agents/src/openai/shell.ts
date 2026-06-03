import type { Shell, ShellAction, ShellOutputResult, ShellResult } from "@openai/agents";
import { StratumAgentWorkspace } from "../workspace.js";

/**
 * OpenAI Agents {@link Shell} backed by the Stratum `/execute` route.
 *
 * Commands run only through Stratum execution; there is no host-shell fallback.
 * When execution is unavailable the underlying workspace throws an
 * `UnsupportedFeatureError` whose message never echoes the command.
 */
export class StratumShell implements Shell {
  constructor(private readonly workspace: StratumAgentWorkspace) {}

  async run(action: ShellAction): Promise<ShellResult> {
    const output: ShellOutputResult[] = [];
    for (const command of action.commands) {
      const result = await this.workspace.execute(command);
      output.push({
        stdout: result.stdout,
        stderr: result.stderr,
        outcome: { type: "exit", exitCode: result.exitCode ?? 1 },
      });
    }
    return { output };
  }
}
