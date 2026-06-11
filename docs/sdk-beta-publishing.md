# SDK Beta Publishing

Status: Task 14 / SDK beta publishing closeout. The source of truth for package
versions is `sdk/version-matrix.json`.

## Version Matrix

| Package | Registry | Version | Notes |
|---|---|---|---|
| `@stratum/sdk` | npm | `0.0.0-beta.0` | Core TypeScript SDK, ESM, `dist` entrypoint. |
| `@stratum/agents` | npm | `0.0.0-beta.0` | Agent adapters; depends exactly on `@stratum/sdk@0.0.0-beta.0`. |
| `stratum-sdk` | PyPI | `0.0.0b0` | Python uses PEP 440 beta spelling; import name is `stratum_sdk`. |

The npm and Python spellings intentionally differ because Python package
versions must follow PEP 440.

## CI Gates

CI does not publish packages or hold registry tokens. It verifies publish
readiness by running:

```bash
cd sdk
bun run check:publishing
bun run --cwd typescript typecheck
bun run --cwd typescript test:run
bun run --cwd typescript build
bun run --cwd agents typecheck
bun run --cwd agents test:run
bun run --cwd agents build
```

It also runs `npm pack --dry-run` in `sdk/typescript` and `sdk/agents`, and
builds the Python wheel/sdist into a temporary directory after installing
`sdk/python` in a temporary virtual environment.

## Manual Release Order

Release only from a clean checkout at the commit being shipped.

1. Publish `@stratum/sdk`:

   ```bash
   cd sdk/typescript
   npm pack --dry-run
   npm publish --tag beta
   ```

2. Publish `@stratum/agents` after `@stratum/sdk` is visible on npm:

   ```bash
   cd sdk/agents
   npm pack --dry-run
   npm publish --tag beta
   ```

3. Build and upload Python `stratum-sdk`:

   ```bash
   cd sdk/python
   python3 -m venv /tmp/stratum-sdk-publish
   /tmp/stratum-sdk-publish/bin/python -m pip install --upgrade pip build twine
   /tmp/stratum-sdk-publish/bin/python -m build --outdir /tmp/stratum-sdk-dist
   /tmp/stratum-sdk-publish/bin/python -m twine check /tmp/stratum-sdk-dist/*
   /tmp/stratum-sdk-publish/bin/python -m twine upload /tmp/stratum-sdk-dist/*
   ```

Do not publish from CI in this slice. Keep npm/PyPI tokens in operator-controlled
secret storage and do not paste them into docs, logs, or issue comments.

## Boundary Expectations

- `@stratum/sdk` publishes `dist` as the only package-controlled file boundary;
  npm still includes package metadata and README/license files by default.
- `@stratum/agents` publishes `dist` and `README.md`; optional framework
  harnesses remain peer dependencies.
- `stratum-sdk` builds from `src/stratum_sdk`, includes `py.typed`, and ships
  README, LICENSE, tests, examples, and `pyproject.toml` in the sdist.
- `@stratum/agents` must not import optional framework peers from its root
  export; consumers choose adapter subpaths.
