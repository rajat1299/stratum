# Cloudflare Deployment

This directory packages `lattice-server` for a Cloudflare-first hosted deployment path.

## What This Deploy Path Does

- runs the Rust HTTP gateway in a Cloudflare Container
- fronts it with a Worker entrypoint
- uses Cloudflare environment variables and secrets for configuration
- is designed to pair with Cloudflare R2 for remote blob and snapshot storage

## Files

- `Dockerfile` — builds and runs `lattice-server`
- `wrangler.jsonc` — Cloudflare configuration
- `src/index.ts` — Worker + Container entrypoint

## Required Secrets And Vars

Set these before deploy:

- `LATTICE_LISTEN`
- `LATTICE_DATA_DIR`
- `LATTICE_R2_BUCKET`
- `LATTICE_R2_ENDPOINT`
- `LATTICE_R2_ACCESS_KEY_ID`
- `LATTICE_R2_SECRET_ACCESS_KEY`
- `LATTICE_R2_REGION`
- `LATTICE_R2_PREFIX`

## Notes

- The current codebase includes the R2 blob backend implementation and hosted workspace metadata plane.
- The metadata plane defaults to an in-memory store inside the gateway today; replace that with a durable hosted store before production.
- This deployment path is for Path A: remote workspace service first. It is not a native macOS mount.
