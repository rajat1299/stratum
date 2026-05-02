export const TOOL_DESCRIPTION = [
  "Run a bash-like shell over a Stratum workspace rooted at '/'.",
  "Paths are Stratum workspace paths; use absolute paths such as /docs/file.txt or paths relative to the current directory.",
  "Supported file commands include cat, echo redirection, mkdir, ls, rm, mv, cp, pwd, and common text utilities provided by just-bash.",
  "Stratum workspace commands: status shows workspace VCS status, diff [path] shows the workspace diff, and commit <message> creates a workspace commit.",
  "grep <pattern> [path] delegates to Stratum grep and prints file:line:line for matches.",
  "chmod, symlink, hard link, readlink, utimes, and semantic search are not supported.",
].join("\n");
