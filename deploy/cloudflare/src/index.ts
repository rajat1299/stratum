import { Container, getContainer } from "@cloudflare/containers";

export interface Env {
  LatticeGateway: DurableObjectNamespace<LatticeGateway>;
  LATTICE_LISTEN: string;
  LATTICE_DATA_DIR: string;
  LATTICE_R2_BUCKET: string;
  LATTICE_R2_ENDPOINT: string;
  LATTICE_R2_ACCESS_KEY_ID: string;
  LATTICE_R2_SECRET_ACCESS_KEY: string;
  LATTICE_R2_REGION: string;
  LATTICE_R2_PREFIX: string;
}

export class LatticeGateway extends Container<Env> {
  defaultPort = 8080;
  sleepAfter = "10m";

  envVars = {
    LATTICE_LISTEN: this.env.LATTICE_LISTEN,
    LATTICE_DATA_DIR: this.env.LATTICE_DATA_DIR,
    LATTICE_R2_BUCKET: this.env.LATTICE_R2_BUCKET,
    LATTICE_R2_ENDPOINT: this.env.LATTICE_R2_ENDPOINT,
    LATTICE_R2_ACCESS_KEY_ID: this.env.LATTICE_R2_ACCESS_KEY_ID,
    LATTICE_R2_SECRET_ACCESS_KEY: this.env.LATTICE_R2_SECRET_ACCESS_KEY,
    LATTICE_R2_REGION: this.env.LATTICE_R2_REGION,
    LATTICE_R2_PREFIX: this.env.LATTICE_R2_PREFIX,
  };
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const container = getContainer(env.LatticeGateway, "lattice-gateway");
    return container.fetch(request);
  },
};
