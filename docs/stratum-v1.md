# Stratum v1 Baseline

Stratum v1 is a Rust virtual filesystem for durable markdown workspaces. It keeps the imported project's proven engineering shape while exposing a Stratum-branded product surface.

## Preserved Architecture

- `StratumDb` is the concurrent core, backed by `Arc<RwLock<DbInner>>`.
- `VirtualFs` owns inode-based file, directory, symlink, metadata, search, copy, move, and permission behavior.
- `auth/` provides users, groups, sessions, permission checks, delegation, and agent tokens.
- `store/` and `vcs/` provide content-addressed objects, commits, log, status, and revert.
- `persist.rs` stores filesystem, auth, and version-control state under `.vfs/state.bin`.
- `server/` exposes the HTTP API with bearer, user, and workspace-scoped auth.
- `src/bin/stratum_mcp.rs` exposes MCP tools and resources for AI agents.
- `src/bin/stratumctl.rs` is the remote-first CLI over the HTTP/gateway API.
- `fuse_mount.rs` and `src/bin/stratum_mount.rs` provide an optional POSIX/FUSE mount behind the `fuser` feature.

## Public v1 Surface

| Surface | Name |
|---|---|
| Interactive CLI | `stratum` |
| HTTP server | `stratum-server` |
| MCP server | `stratum-mcp` |
| Remote CLI | `stratumctl` |
| Optional FUSE mount | `stratum-mount` |
| Main env prefix | `STRATUM_` |
| Workspace header | `X-Stratum-Workspace` |
| Directory write header | `X-Stratum-Type: directory` |
| MCP resources | `stratum://tree`, `stratum://files/<path>` |

## v1 Constraints

- Markdown-only mode remains the default product mode.
- POSIX-compatible non-markdown files are available only through `STRATUM_COMPAT_TARGET=posix` or the FUSE mount path.
- Persistence format is inherited from the imported implementation, so v2 should treat data migration deliberately before changing serialized structs.
- The source import should be license-reviewed before redistribution; the imported README identified the original project as MIT.
