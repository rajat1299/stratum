# Lattice v1 Baseline

Lattice v1 is a Rust virtual filesystem for durable markdown workspaces. It keeps the imported project's proven engineering shape while exposing a Lattice-branded product surface.

## Preserved Architecture

- `LatticeDb` is the concurrent core, backed by `Arc<RwLock<DbInner>>`.
- `VirtualFs` owns inode-based file, directory, symlink, metadata, search, copy, move, and permission behavior.
- `auth/` provides users, groups, sessions, permission checks, delegation, and agent tokens.
- `store/` and `vcs/` provide content-addressed objects, commits, log, status, and revert.
- `persist.rs` stores filesystem, auth, and version-control state under `.vfs/state.bin`.
- `server/` exposes the HTTP API with bearer, user, and workspace-scoped auth.
- `src/bin/lattice_mcp.rs` exposes MCP tools and resources for AI agents.
- `src/bin/latticectl.rs` is the remote-first CLI over the HTTP/gateway API.
- `fuse_mount.rs` and `src/bin/lattice_mount.rs` provide an optional POSIX/FUSE mount behind the `fuser` feature.

## Public v1 Surface

| Surface | Name |
|---|---|
| Interactive CLI | `lattice` |
| HTTP server | `lattice-server` |
| MCP server | `lattice-mcp` |
| Remote CLI | `latticectl` |
| Optional FUSE mount | `lattice-mount` |
| Main env prefix | `LATTICE_` |
| Workspace header | `X-Lattice-Workspace` |
| Directory write header | `X-Lattice-Type: directory` |
| MCP resources | `lattice://tree`, `lattice://files/<path>` |

## v1 Constraints

- Markdown-only mode remains the default product mode.
- POSIX-compatible non-markdown files are available only through `LATTICE_COMPAT_TARGET=posix` or the FUSE mount path.
- Persistence format is inherited from the imported implementation, so v2 should treat data migration deliberately before changing serialized structs.
- The source import should be license-reviewed before redistribution; the imported README identified the original project as MIT.
