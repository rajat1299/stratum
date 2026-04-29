import { Container, getContainer } from "@cloudflare/containers";

export interface Env {
  StratumGateway: DurableObjectNamespace<StratumGateway>;
  STRATUM_LISTEN: string;
  STRATUM_DATA_DIR: string;
  STRATUM_R2_BUCKET: string;
  STRATUM_R2_ENDPOINT: string;
  STRATUM_R2_ACCESS_KEY_ID: string;
  STRATUM_R2_SECRET_ACCESS_KEY: string;
  STRATUM_R2_REGION: string;
  STRATUM_R2_PREFIX: string;
}

export class StratumGateway extends Container<Env> {
  defaultPort = 8080;
  sleepAfter = "10m";

  envVars = {
    STRATUM_LISTEN: this.env.STRATUM_LISTEN,
    STRATUM_DATA_DIR: this.env.STRATUM_DATA_DIR,
    STRATUM_R2_BUCKET: this.env.STRATUM_R2_BUCKET,
    STRATUM_R2_ENDPOINT: this.env.STRATUM_R2_ENDPOINT,
    STRATUM_R2_ACCESS_KEY_ID: this.env.STRATUM_R2_ACCESS_KEY_ID,
    STRATUM_R2_SECRET_ACCESS_KEY: this.env.STRATUM_R2_SECRET_ACCESS_KEY,
    STRATUM_R2_REGION: this.env.STRATUM_R2_REGION,
    STRATUM_R2_PREFIX: this.env.STRATUM_R2_PREFIX,
  };
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const container = getContainer(env.StratumGateway, "stratum-gateway");
    return container.fetch(request);
  },
};
