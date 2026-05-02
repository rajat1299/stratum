import { StratumHttpError } from "./client.js";

export class FsError extends Error {
  constructor(
    public readonly code: string,
    public readonly errno: number,
    message: string,
  ) {
    super(message);
    this.name = "FsError";
  }
}

const make = (code: string, errno: number, suffix: string): FsError =>
  new FsError(code, errno, `${code}: ${suffix}`);

export const enoent = (path: string): FsError =>
  make("ENOENT", -2, `no such file or directory, '${path}'`);

export const eperm = (path: string, op?: string): FsError =>
  make("EPERM", -1, `operation not permitted${op ? `, ${op}` : ""} '${path}'`);

export const eio = (reason: string): FsError => make("EIO", -5, `I/O error, ${reason}`);

export const eisdir = (path: string): FsError =>
  make("EISDIR", -21, `is a directory, '${path}'`);

export const enotdir = (path: string): FsError =>
  make("ENOTDIR", -20, `not a directory, '${path}'`);

export const enotempty = (path: string): FsError =>
  make("ENOTEMPTY", -39, `directory not empty, '${path}'`);

export const eexist = (path: string): FsError =>
  make("EEXIST", -17, `file already exists, '${path}'`);

export const enosys = (op: string): FsError =>
  make("ENOSYS", -38, `function not supported, ${op}`);

export const einval = (reason: string): FsError =>
  make("EINVAL", -22, `invalid argument, ${reason}`);

export function toFsError(error: unknown, path: string, operation: string): FsError {
  if (error instanceof FsError) return error;

  if (error instanceof StratumHttpError) {
    if (error.status === 404) return enoent(path);
    if (error.status === 409) return eexist(path);
    if (error.status === 403) return eperm(path, operation);
    if (error.status === 400) {
      return errorFromMessage(error.message, path, operation) ?? einval(`${operation} '${path}': ${error.message}`);
    }
    return eio(`${operation} '${path}': ${error.message}`);
  }

  const message = error instanceof Error ? error.message : String(error);
  return errorFromMessage(message, path, operation) ?? eio(`${operation} '${path}': ${message}`);
}

function errorFromMessage(message: string, path: string, operation: string): FsError | null {
  if (/\bENOENT\b|not found|missing|no such file/i.test(message)) return enoent(path);
  if (/\bENOTDIR\b|not a directory/i.test(message)) return enotdir(path);
  if (/\bEISDIR\b|is a directory/i.test(message)) return eisdir(path);
  if (/\bEEXIST\b|already exists/i.test(message)) return eexist(path);
  if (/\bENOTEMPTY\b|not empty/i.test(message)) return enotempty(path);
  if (/\bEPERM\b|not permitted|permission/i.test(message)) return eperm(path, operation);
  return null;
}
