/**
 * parseBanner — validator for `hints.banner` from GET /v1/capabilities.
 *
 * Background:
 *   Frontend requested a typed union `{ kind: "info"|"warn", text: string } | null`
 *   in `docs/plans/2026-05-15-capability-manifest-requirements.md` §3.
 *   Backend shipped `banner: Option<serde_json::Value>` instead — anything
 *   the server feels like serializing makes it through to us.
 *
 *   Per CTO review on 2026-05-15: "Don't let untyped JSON leak into
 *   components." This module is the perimeter. It accepts only the shape
 *   we asked for; anything else returns `null` and logs once at startup.
 *
 *   When backend tightens their Rust struct to a typed enum (tracked in
 *   `docs/plans/2026-05-15-capability-manifest-v1-lock.md`), this validator
 *   becomes a noop — the type system will already enforce the shape. We
 *   keep the file around as defense-in-depth.
 */

export type BannerKind = "info" | "warn";

/** The exact shape the UI is allowed to consume. */
export interface Banner {
  readonly kind: BannerKind;
  readonly text: string;
}

const KIND_VALUES = new Set<BannerKind>(["info", "warn"]);
const MAX_TEXT_LENGTH = 280;

/** Per-process flag so we only log a given rejected shape once. */
const seenRejections = new Set<string>();

export interface ParseBannerOptions {
  /** Where to emit a one-time warning when a shape is rejected. Defaults to console. */
  readonly logger?: Pick<Console, "warn">;
}

/**
 * Validate an untyped `hints.banner` value. Returns the typed Banner on
 * success, or `null` for any of:
 *   - `null` / `undefined` (the "no banner" state — not an error)
 *   - non-object values (string, number, array, boolean)
 *   - missing or non-string `text`
 *   - missing or out-of-enum `kind`
 *   - `text` longer than 280 chars (banner is meant to be short)
 *   - any extra unknown keys (fail closed — defense against future drift)
 *
 * The first time we reject a particular shape we log a warning so this
 * leaks into the dev console exactly once per session per shape, not on
 * every render.
 */
export function parseBanner(value: unknown, options: ParseBannerOptions = {}): Banner | null {
  if (value === null || value === undefined) return null;

  // Reject scalars and arrays before the property checks.
  if (typeof value !== "object" || Array.isArray(value)) {
    warnOnce(value, "banner is not an object", options);
    return null;
  }

  const v = value as Record<string, unknown>;

  // Unknown keys are a red flag — the contract is intentionally small.
  for (const key of Object.keys(v)) {
    if (key !== "kind" && key !== "text") {
      warnOnce(value, `banner has unexpected key "${key}"`, options);
      return null;
    }
  }

  // kind must be in the enum
  const kind = v["kind"];
  if (typeof kind !== "string" || !KIND_VALUES.has(kind as BannerKind)) {
    warnOnce(value, `banner.kind is not "info" or "warn"`, options);
    return null;
  }

  // text must be a non-empty bounded string
  const text = v["text"];
  if (typeof text !== "string" || text.length === 0) {
    warnOnce(value, "banner.text is missing or empty", options);
    return null;
  }
  if (text.length > MAX_TEXT_LENGTH) {
    warnOnce(value, `banner.text exceeds ${MAX_TEXT_LENGTH} chars`, options);
    return null;
  }

  return { kind: kind as BannerKind, text };
}

function warnOnce(value: unknown, reason: string, options: ParseBannerOptions): void {
  // Stable key per (reason, fingerprint) — fingerprint is the JSON of the
  // value truncated, so spammy rejections collapse to one log line.
  let fingerprint = "non-json";
  try {
    fingerprint = JSON.stringify(value).slice(0, 120);
  } catch {
    // value contained a circular reference — keep the default
  }
  const key = `${reason}|${fingerprint}`;
  if (seenRejections.has(key)) return;
  seenRejections.add(key);
  const logger = options.logger ?? console;
  logger.warn(
    `[capabilities] hints.banner rejected: ${reason}. Server may have shipped an ` +
      `untyped banner shape; see docs/plans/2026-05-15-capability-manifest-v1-lock.md`,
    { value },
  );
}

/** Test seam — reset the once-per-session rejection cache. */
export function __resetBannerWarnCache(): void {
  seenRejections.clear();
}
