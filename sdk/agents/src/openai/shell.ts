import type { Shell, ShellAction, ShellOutputResult, ShellResult } from "@openai/agents";
import { UnsupportedFeatureError } from "@stratum/sdk";
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
      let result: Awaited<ReturnType<StratumAgentWorkspace["execute"]>>;
      try {
        result = await this.workspace.execute(command);
      } catch (error) {
        if (error instanceof UnsupportedFeatureError) throw error;
        throw new Error("Stratum execution failed.");
      }
      output.push({
        stdout: result.stdout,
        stderr: result.stderr,
        outcome: { type: "exit", exitCode: result.exitCode ?? 1 },
      });
    }
    return { output };
  }
}
