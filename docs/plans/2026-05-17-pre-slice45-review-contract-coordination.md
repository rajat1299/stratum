# Pre-Slice 4.5 Review Contract Coordination

**Goal:** Resolve the two frontend/backend review API contract points before CR detail work depends on the backend shape.

**Architecture:** Keep review policy resolution server-side. The frontend should not infer protected-rule behavior from the capability manifest alone, and backend should not introduce a CR-scoped diff route unless the contract intentionally changes.

**Current Baseline:** Slice 4 added `require_all_files_viewed` to protected ref/path rules and advertises the default in the capability manifest. `GET /change-requests/{id}` still returns `change_request` plus `approval_state`, and `GET /vcs/diff` currently models only the optional `path` query parameter.

## Required Backend Contract Decisions

1. `GET /change-requests/{id}` must return a CR-level resolved `require_all_files_viewed: bool`.
   - The value must be computed from the active protected ref rule matching the CR target ref and active protected path rules matching changed paths between `base_commit` and `head_commit`.
   - The manifest `require_all_files_viewed_default` is not sufficient for the CR detail screen because it does not identify the rule that matched a specific change request.
   - SDK response types should carry the resolved field once the route returns it.

2. Frontend should use `GET /vcs/diff?base=<base_commit>&head=<head_commit>` for CR detail diffs unless backend explicitly changes the contract.
   - No CR-scoped `GET /change-requests/{id}/diff` endpoint is planned for the current frontend flow.
   - Backend must make `/vcs/diff` honor explicit `base` and `head` query parameters before the frontend flow ships against real data.
   - Keep existing path filtering behavior compatible with the explicit commit pair.

## Inspected Code

- `src/server/routes_review.rs`: route table has `GET /change-requests/{id}` and no CR diff route; the detail handler returns `change_json(...)`.
- `src/server/routes_review.rs`: `change_json` returns `change_request` and `approval_state`.
- `src/review.rs`: protected ref/path rules persist `require_all_files_viewed`, while approval policy decisions currently resolve approval counts and matched rule IDs.
- `src/server/routes_vcs.rs`: `DiffQuery` currently models only optional `path`, and `/vcs/diff` passes only that filter into the diff path.
- `sdk/typescript/src/types.ts` and `sdk/python/src/stratum_sdk/types.py`: CR response types do not yet carry a resolved `require_all_files_viewed` field.

## Exit Criterion

- The next backend slice plan either includes these two route-shape changes directly or explicitly defers them with a written frontend-facing alternative.
