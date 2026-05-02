export class StratumHttpError extends Error {
  readonly status: number;
  readonly body: string;

  constructor(status: number, body: string, message = errorMessageFromBody(status, body)) {
    super(message);
    this.name = "StratumHttpError";
    this.status = status;
    this.body = body;
  }
}

export class UnsupportedFeatureError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnsupportedFeatureError";
  }
}

function errorMessageFromBody(status: number, body: string): string {
  try {
    const parsed = JSON.parse(body) as { error?: unknown };
    if (typeof parsed.error === "string" && parsed.error !== "") {
      return parsed.error;
    }
  } catch {
    // Preserve the raw response body on StratumHttpError for non-JSON failures.
  }

  return body === "" ? `Stratum request failed with status ${status}` : body;
}
