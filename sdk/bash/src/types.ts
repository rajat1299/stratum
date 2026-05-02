import type { Bash, BashLogger, BashOptions } from "just-bash";
import type { StratumClientOptions } from "./client.js";
import type { StratumFs } from "./stratum-fs.js";
import type { SessionCacheOptions } from "./session-cache.js";
import type { StratumVolume } from "./volume.js";

type ExecutionLimits = NonNullable<BashOptions["executionLimits"]>;

export interface CreateBashOptions extends StratumClientOptions {
  readonly env?: Record<string, string>;
  readonly executionLimits?: ExecutionLimits;
  readonly logger?: BashLogger;
  readonly cacheOptions?: SessionCacheOptions;
}

export interface CreateBashResult {
  readonly bash: Bash;
  readonly volume: StratumVolume;
  readonly fs: StratumFs;
  readonly toolDescription: string;
  readonly refresh: () => Promise<void>;
}
