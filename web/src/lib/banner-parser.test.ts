import { beforeEach, describe, expect, it, vi } from "vitest";
import { __resetBannerWarnCache, parseBanner } from "./banner-parser.ts";

function silentLogger() {
  return { warn: vi.fn() };
}

beforeEach(() => {
  __resetBannerWarnCache();
});

describe("parseBanner — accepts the spec'd shape", () => {
  it("accepts an info banner", () => {
    const logger = silentLogger();
    expect(parseBanner({ kind: "info", text: "Hello" }, { logger })).toEqual({
      kind: "info",
      text: "Hello",
    });
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it("accepts a warn banner", () => {
    expect(parseBanner({ kind: "warn", text: "Maintenance at 14:00 UTC" }, { logger: silentLogger() })).toEqual({
      kind: "warn",
      text: "Maintenance at 14:00 UTC",
    });
  });
});

describe("parseBanner — accepts the no-banner state without warning", () => {
  it("returns null for null", () => {
    const logger = silentLogger();
    expect(parseBanner(null, { logger })).toBeNull();
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it("returns null for undefined", () => {
    const logger = silentLogger();
    expect(parseBanner(undefined, { logger })).toBeNull();
    expect(logger.warn).not.toHaveBeenCalled();
  });
});

describe("parseBanner — rejects malformed shapes", () => {
  it("rejects scalars", () => {
    const logger = silentLogger();
    expect(parseBanner("oops", { logger })).toBeNull();
    expect(parseBanner(42, { logger })).toBeNull();
    expect(parseBanner(true, { logger })).toBeNull();
    expect(logger.warn).toHaveBeenCalled();
  });

  it("rejects arrays", () => {
    expect(parseBanner(["info", "msg"], { logger: silentLogger() })).toBeNull();
  });

  it("rejects unknown kind values", () => {
    expect(parseBanner({ kind: "error", text: "x" }, { logger: silentLogger() })).toBeNull();
    expect(parseBanner({ kind: "INFO", text: "x" }, { logger: silentLogger() })).toBeNull();
    expect(parseBanner({ kind: 42, text: "x" }, { logger: silentLogger() })).toBeNull();
  });

  it("rejects missing or non-string text", () => {
    expect(parseBanner({ kind: "info" }, { logger: silentLogger() })).toBeNull();
    expect(parseBanner({ kind: "info", text: "" }, { logger: silentLogger() })).toBeNull();
    expect(parseBanner({ kind: "info", text: 42 }, { logger: silentLogger() })).toBeNull();
  });

  it("rejects text longer than 280 chars (the bounded contract)", () => {
    const long = "x".repeat(281);
    expect(parseBanner({ kind: "info", text: long }, { logger: silentLogger() })).toBeNull();
    const ok = "x".repeat(280);
    expect(parseBanner({ kind: "info", text: ok }, { logger: silentLogger() })).toEqual({
      kind: "info",
      text: ok,
    });
  });

  it("rejects unknown keys — closed contract", () => {
    expect(
      parseBanner({ kind: "info", text: "hi", action_url: "https://evil/" }, { logger: silentLogger() }),
    ).toBeNull();
  });
});

describe("parseBanner — warn-once semantics", () => {
  it("logs the same rejection only once per session", () => {
    const logger = silentLogger();
    parseBanner({ kind: "boom", text: "x" }, { logger });
    parseBanner({ kind: "boom", text: "x" }, { logger });
    parseBanner({ kind: "boom", text: "x" }, { logger });
    expect(logger.warn).toHaveBeenCalledTimes(1);
  });

  it("logs different rejections independently", () => {
    const logger = silentLogger();
    parseBanner({ kind: "boom", text: "x" }, { logger });
    parseBanner({ kind: "info", text: "" }, { logger });
    expect(logger.warn).toHaveBeenCalledTimes(2);
  });

  it("survives a circular-reference value without throwing", () => {
    const circular: Record<string, unknown> = { kind: "info", text: "ok" };
    circular["self"] = circular;
    const logger = silentLogger();
    // unknown key "self" → reject, log once, no crash
    expect(parseBanner(circular, { logger })).toBeNull();
    expect(logger.warn).toHaveBeenCalledTimes(1);
  });
});
