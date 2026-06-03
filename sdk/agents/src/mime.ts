// Stratum-specific content type helpers shared by the adapters.
//
// These classify a workspace file as text, presentable binary (forwarded to a
// model as a multimodal attachment), or unsupported binary (metadata stub only).
// They never read host files and never shell out; they only inspect the path
// extension and the Stratum-reported mime type.

const TEXT_EXTENSIONS = new Set([
  "txt",
  "md",
  "markdown",
  "json",
  "jsonl",
  "yaml",
  "yml",
  "csv",
  "tsv",
  "xml",
  "html",
  "htm",
  "js",
  "mjs",
  "cjs",
  "ts",
  "tsx",
  "jsx",
  "py",
  "rb",
  "rs",
  "go",
  "java",
  "kt",
  "c",
  "cpp",
  "cc",
  "h",
  "hpp",
  "sh",
  "bash",
  "zsh",
  "sql",
  "log",
  "env",
  "ini",
  "toml",
  "conf",
  "cfg",
]);

const PRESENTABLE_BINARY_MIMES = new Set([
  "application/pdf",
  "image/png",
  "image/jpeg",
  "image/gif",
  "image/webp",
]);

/** How a workspace file should be surfaced to a model. */
export type StratumContentClass = "text" | "media" | "binary";

export function extensionOf(path: string): string {
  const dot = path.lastIndexOf(".");
  const slash = path.lastIndexOf("/");
  if (dot < 0 || dot < slash) return "";
  return path.slice(dot + 1).toLowerCase();
}

/** Best-effort mime type derived from the file extension only. */
export function mimeForPath(path: string): string {
  const ext = extensionOf(path);
  if (ext === "json" || ext === "jsonl") return "application/json";
  if (ext === "yaml" || ext === "yml") return "application/yaml";
  if (ext === "xml") return "application/xml";
  if (ext === "csv") return "text/csv";
  if (ext === "html" || ext === "htm") return "text/html";
  if (ext === "md" || ext === "markdown") return "text/markdown";
  if (TEXT_EXTENSIONS.has(ext)) return "text/plain";
  if (ext === "png") return "image/png";
  if (ext === "jpg" || ext === "jpeg") return "image/jpeg";
  if (ext === "gif") return "image/gif";
  if (ext === "webp") return "image/webp";
  if (ext === "pdf") return "application/pdf";
  return "application/octet-stream";
}

/**
 * Resolve the mime type for a path, preferring the Stratum-reported mime type
 * when present and falling back to an extension-derived guess.
 */
export function resolveMimeType(path: string, statMime?: string | null): string {
  if (statMime !== undefined && statMime !== null && statMime !== "") {
    return statMime;
  }
  return mimeForPath(path);
}

export function isTextMime(mime: string): boolean {
  return (
    mime.startsWith("text/") ||
    mime === "application/json" ||
    mime === "application/xml" ||
    mime === "application/yaml"
  );
}

export function isPresentableBinaryMime(mime: string): boolean {
  return PRESENTABLE_BINARY_MIMES.has(mime);
}

/** Classify a mime type into the content class an adapter should surface. */
export function classifyMime(mime: string): StratumContentClass {
  if (isTextMime(mime)) return "text";
  if (isPresentableBinaryMime(mime)) return "media";
  return "binary";
}
