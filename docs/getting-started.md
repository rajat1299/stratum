# Getting Started

This guide walks you through installing, building, and running stratum for the first time.

## Prerequisites

- **Rust toolchain** (1.85+) — install from [rustup.rs](https://rustup.rs)
- macOS, Linux, or WSL on Windows

Verify your installation:

```bash
rustc --version
cargo --version
```

## Building from Source

Clone the repository and build the release binaries:

```bash
git clone <repo-url> stratum
cd stratum
cargo build --release
```

This produces three binaries in `target/release/`:

| Binary | Purpose |
|---|---|
| `stratum` | Interactive CLI/REPL |
| `stratum-server` | HTTP/REST API server |
| `stratum-mcp` | MCP server for AI agents |
| `stratumctl` | Remote-first CLI over the HTTP/gateway surface |

## First Run — CLI

Start the interactive shell:

```bash
cargo run --release --bin stratum
```

On first launch, there are no users besides `root`. stratum prompts you to create an admin account, automatically creates your home directory, and drops you right in:

```
stratum v1.0.0 — Stratum Virtual File System

Welcome! Let's set up your account.
Admin username: alice

Created admin 'alice' (uid=1, groups=[alice, wheel])
Home directory: /home/alice

Type 'help' for available commands, 'exit' to quit.

alice@stratum:~ $
```

You're now in your home directory (`~` is `/home/alice`), ready to start working immediately — no setup required.

### Try It Out

```
alice@stratum:~ $ whoami
alice

alice@stratum:~ $ pwd
/home/alice

alice@stratum:~ $ touch hello.md
alice@stratum:~ $ write hello.md # Welcome to stratum
alice@stratum:~ $ cat hello.md
# Welcome to stratum

alice@stratum:~ $ mkdir docs
alice@stratum:~ $ touch docs/readme.md
alice@stratum:~ $ write docs/readme.md # My Project

alice@stratum:~ $ ls -l
drwxr-xr-x alice    alice           1 Apr 13 10:30 docs/
-rw-r--r-- alice    alice          23 Apr 13 10:30 hello.md

alice@stratum:~ $ tree
.
├── docs/
│   └── readme.md
└── hello.md

alice@stratum:~ $ commit initial setup
[0c5fd42b] initial setup
```

Type `exit` or `quit` to leave. Your data is automatically saved to `.vfs/state.bin` and restored on next launch.
Hosted workspace metadata is stored separately at `.vfs/workspaces.bin` under `STRATUM_DATA_DIR`.

### A Note on the Root Directory

The root directory `/` is owned by `root:root` with mode `0755`, just like a real Unix system. Regular users (including admins) work inside their home directory. If you need to create top-level directories, switch to root:

```
alice@stratum:~ $ su root
root@stratum:~ $ mkdir /shared
root@stratum:~ $ chmod 777 /shared
root@stratum:~ $ su alice
```

## First Run — HTTP Server

Start the REST API:

```bash
STRATUM_LISTEN=127.0.0.1:3000 cargo run --release --bin stratum-server
```

The server is now accepting requests:

```bash
# Write a file
curl -X PUT http://localhost:3000/fs/notes/readme.md \
  -H "Authorization: User alice" \
  -d "# My Notes"

# Read it back
curl http://localhost:3000/fs/notes/readme.md

# Check health
curl http://localhost:3000/health
```

See the [HTTP API Guide](http-api-guide.md) for the full endpoint reference.

## First Run — MCP Server

For AI agent integration (Cursor, Claude Desktop, etc.):

```bash
cargo run --release --bin stratum-mcp
```

The MCP server communicates over stdio. Add it to your MCP client config — for example, in Cursor's `mcp.json`:

```json
{
  "mcpServers": {
    "stratum": {
      "command": "/absolute/path/to/target/release/stratum-mcp",
      "env": {
        "STRATUM_DATA_DIR": "/path/to/your/data"
      }
    }
  }
}
```

See the [MCP Guide](mcp-guide.md) for tool descriptions and usage.

## First Run — Remote CLI

The `stratumctl` binary is a thin client over the HTTP API and future hosted gateway.

```bash
cargo run --release --bin stratumctl -- --url http://127.0.0.1:3000 health
```

Examples:

```bash
# As a named user
cargo run --release --bin stratumctl -- --url http://127.0.0.1:3000 --user alice ls /incidents

# As an agent token
cargo run --release --bin stratumctl -- --url http://127.0.0.1:3000 --token "$STRATUM_TOKEN" grep timeout /runbooks

# Workspace-scoped hosted token
cargo run --release --bin stratumctl -- \
  --url http://127.0.0.1:3000 \
  --workspace-id "<workspace-uuid>" \
  --workspace-token "<workspace-secret>" \
  ls /read

# Issue a scoped workspace token as an admin
cargo run --release --bin stratumctl -- \
  --url http://127.0.0.1:3000 \
  --user root \
  workspace issue-token "<workspace-uuid>" ci-token "<agent-token>" \
  --read-prefix /incidents/checkout-latency/read \
  --write-prefix /incidents/checkout-latency/work
```

If no `--read-prefix` or `--write-prefix` flags are supplied, the issued workspace token defaults both scopes to the workspace root. Repeating a flag adds another allowed backing prefix. Workspace bearer tokens expose the workspace root as `/` for filesystem, search, and tree routes, so clients use workspace-relative paths like `/read` rather than `/incidents/checkout-latency/read`. Workspace bearer tokens cannot manage workspace metadata. Global VCS routes remain admin-gated.

## Configuration

All configuration is through environment variables. Set them before launching any binary:

| Variable | Default | Description |
|---|---|---|
| `STRATUM_DATA_DIR` | Current working directory | Where `.vfs/state.bin` is stored |
| `STRATUM_WORKSPACE_METADATA_PATH` | `<STRATUM_DATA_DIR>/.vfs/workspaces.bin` | Hosted workspace metadata file |
| `STRATUM_LISTEN` | `127.0.0.1:3000` | HTTP server bind address |
| `STRATUM_AUTOSAVE_SECS` | `5` | Auto-save interval (seconds) |
| `STRATUM_AUTOSAVE_WRITES` | `100` | Auto-save after N write operations |
| `STRATUM_MAX_FILE_SIZE` | `10485760` (10 MB) | Maximum file size in bytes |
| `RUST_LOG` | `stratum=info` | Log verbosity (`debug`, `trace`, etc.) |

Example — custom data directory with verbose logging:

```bash
STRATUM_DATA_DIR=/var/data/stratum \
RUST_LOG=stratum=debug \
cargo run --release --bin stratum
```

## Data Persistence

stratum stores filesystem, user, and version history state in one binary file:

```
<STRATUM_DATA_DIR>/.vfs/state.bin
```

Hosted workspace records and workspace-scoped token hashes and prefixes are stored separately:

```
<STRATUM_DATA_DIR>/.vfs/workspaces.bin
```

- **Auto-save** runs every 5 seconds (or after 100 writes, whichever comes first)
- **On exit**, the CLI and HTTP server perform a final save
- **Atomic writes** — state files are written to temp files first, then renamed, so a crash never corrupts committed data

To start fresh, delete the state directory:

```bash
rm -rf .vfs/
```

## Subsequent Logins

After the first run, subsequent CLI launches show a login prompt and automatically navigate to your home directory:

```
stratum v1.0.0 — Loaded from disk (1 commits, 7 objects)
Login as: alice
Logged in as 'alice' (uid=1, gid=2)

alice@stratum:~ $
```

All files, users, and version history are restored from the previous session.

## What's Next

- [User Management](user-management.md) — create users, groups, set permissions
- [Filesystem Guide](filesystem-guide.md) — files, directories, search, pipes
- [Version Control](version-control.md) — commit, log, revert
- [HTTP API Guide](http-api-guide.md) — REST endpoints with curl examples
- [MCP Guide](mcp-guide.md) — AI agent integration
