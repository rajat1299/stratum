export interface StratumClientOptions {
  readonly baseUrl: string;
  readonly fetch?: typeof fetch;
  readonly auth?: StratumAuth;
  readonly workspaceId?: string;
  readonly workspaceToken?: string;
  readonly idempotencyKeyPrefix?: string;
}

export type StratumAuth =
  | { readonly type: "user"; readonly username: string }
  | { readonly type: "bearer"; readonly token: string }
  | { readonly type: "workspace"; readonly workspaceId: string; readonly workspaceToken: string };

export interface StratumMutationOptions {
  readonly idempotencyKey?: string;
}

export type StratumRequestBody = BodyInit | Uint8Array;

export interface StratumDirectoryEntry {
  readonly name: string;
  readonly kind?: string;
  readonly is_dir: boolean;
  readonly is_symlink: boolean;
  readonly size: number;
  readonly mode: string;
  readonly uid: number;
  readonly gid: number;
  readonly modified: number;
}

export interface StratumDirectoryListing {
  readonly path: string;
  readonly entries: readonly StratumDirectoryEntry[];
}

export interface StratumStat {
  readonly inode_id: number;
  readonly kind: string;
  readonly size: number;
  readonly mode: string;
  readonly uid: number;
  readonly gid: number;
  readonly created: number;
  readonly modified: number;
  readonly mime_type: string | null;
  readonly content_hash: string | null;
  readonly custom_attrs: Record<string, string>;
}

export interface StratumWriteOptions extends StratumMutationOptions {
  readonly mimeType?: string;
}

export interface StratumWriteResult {
  readonly written: string;
  readonly size: number;
}

export interface StratumMkdirResult {
  readonly created: string;
  readonly type: "directory";
}

export interface StratumMetadataPatch {
  readonly mime_type?: string | null;
  readonly custom_attrs?: Record<string, string>;
  readonly remove_custom_attrs?: readonly string[];
}

export interface StratumMetadataPatchResult {
  readonly metadata_updated: string;
  readonly changed: boolean;
  readonly mime_type: string | null;
  readonly custom_attr_keys: readonly string[];
  readonly custom_attrs_set: readonly string[];
  readonly custom_attrs_removed: readonly string[];
}

export interface StratumDeleteResult {
  readonly deleted: string;
}

export interface StratumCopyResult {
  readonly copied: string;
  readonly to: string;
}

export interface StratumMoveResult {
  readonly moved: string;
  readonly to: string;
}

export interface StratumGrepOptions {
  readonly path?: string;
  readonly recursive?: boolean;
}

export interface StratumGrepResult {
  readonly results: readonly StratumGrepMatch[];
  readonly count: number;
}

export interface StratumGrepMatch {
  readonly file: string;
  readonly line_num: number;
  readonly line: string;
}

export interface StratumFindOptions {
  readonly path?: string;
}

export interface StratumFindResult {
  readonly results: readonly string[];
  readonly count: number;
}

export interface StratumCommitResult {
  readonly hash: string;
  readonly message: string;
  readonly author: string;
}

export interface StratumCommitLog {
  readonly commits: readonly StratumCommitInfo[];
}

export interface StratumCommitInfo {
  readonly hash: string;
  readonly message: string;
  readonly author: string;
  readonly timestamp: number;
}

export interface StratumRevertResult {
  readonly reverted_to: string;
}

export interface StratumRef {
  readonly name: string;
  readonly target: string;
  readonly version: number;
}

export interface StratumRefsResult {
  readonly refs: readonly StratumRef[];
}

export interface CreateRefRequest {
  readonly name: string;
  readonly target: string;
}

export interface UpdateRefRequest {
  readonly target: string;
  readonly expected_target: string;
  readonly expected_version: number;
}

export interface ProtectedRefRuleRequest {
  readonly ref_name: string;
  readonly required_approvals: number;
  readonly require_all_files_viewed?: boolean;
}

export interface ProtectedRefRule {
  readonly id: string;
  readonly ref_name: string;
  readonly required_approvals: number;
  readonly require_all_files_viewed: boolean;
  readonly created_by: number;
  readonly active: boolean;
}

export interface ProtectedRefRuleListResponse {
  readonly rules: readonly ProtectedRefRule[];
}

export interface ProtectedPathRuleRequest {
  readonly path_prefix: string;
  readonly target_ref?: string;
  readonly required_approvals: number;
  readonly require_all_files_viewed?: boolean;
}

export interface ProtectedPathRule {
  readonly id: string;
  readonly path_prefix: string;
  readonly target_ref: string | null;
  readonly required_approvals: number;
  readonly require_all_files_viewed: boolean;
  readonly created_by: number;
  readonly active: boolean;
}

export interface ProtectedPathRuleListResponse {
  readonly rules: readonly ProtectedPathRule[];
}

export interface ChangeRequestCreateRequest {
  readonly title: string;
  readonly description?: string | null;
  readonly source_ref: string;
  readonly target_ref: string;
}

export interface ChangeRequest {
  readonly id: string;
  readonly title: string;
  readonly description: string | null;
  readonly source_ref: string;
  readonly target_ref: string;
  readonly base_commit: string;
  readonly head_commit: string;
  readonly status: "open" | "merged" | "rejected";
  readonly created_by: number;
  readonly version: number;
}

export type ApprovalState = ApprovalPolicyDecision | ApprovalStateUnavailable;

export interface ApprovalPolicyDecision {
  readonly change_request_id: string;
  readonly required_approvals: number;
  readonly approval_count: number;
  readonly approved_by: readonly number[];
  readonly required_reviewers: readonly number[];
  readonly approved_required_reviewers: readonly number[];
  readonly missing_required_reviewers: readonly number[];
  readonly approved: boolean;
  readonly matched_ref_rules: readonly string[];
  readonly matched_path_rules: readonly string[];
}

export interface ApprovalStateUnavailable {
  readonly available: false;
  readonly error: string;
}

export interface ChangeRequestResponse {
  readonly change_request: ChangeRequest;
  readonly approval_state: ApprovalState;
}

export interface ChangeRequestListResponse {
  readonly change_requests: readonly ChangeRequestResponse[];
}

export interface ApprovalRequest {
  readonly comment?: string;
}

export interface ApprovalRecord {
  readonly id: string;
  readonly change_request_id: string;
  readonly head_commit: string;
  readonly approved_by: number;
  readonly comment: string | null;
  readonly active: boolean;
  readonly dismissed_by?: number | null;
  readonly dismissal_reason?: string | null;
  readonly version: number;
}

export interface ApprovalResponse {
  readonly approval: ApprovalRecord;
  readonly created?: boolean;
  readonly dismissed?: boolean;
  readonly approval_state: ApprovalState;
}

export interface ApprovalListResponse {
  readonly approvals: readonly ApprovalRecord[];
  readonly approval_state?: ApprovalState;
}

export interface ReviewerRequest {
  readonly reviewer_uid: number;
  readonly required?: boolean;
}

export interface ReviewerAssignment {
  readonly id: string;
  readonly change_request_id: string;
  readonly reviewer: number;
  readonly assigned_by: number;
  readonly required: boolean;
  readonly active: boolean;
  readonly version: number;
}

export interface ReviewerResponse {
  readonly assignment: ReviewerAssignment;
  readonly created: boolean;
  readonly updated: boolean;
  readonly approval_state: ApprovalState;
}

export interface ReviewerListResponse {
  readonly assignments: readonly ReviewerAssignment[];
  readonly approval_state: ApprovalState;
}

export interface CommentRequest {
  readonly body: string;
  readonly path?: string;
  readonly kind?: "general" | "changes_requested";
}

export interface ReviewComment {
  readonly id: string;
  readonly change_request_id: string;
  readonly author: number;
  readonly body: string;
  readonly path: string | null;
  readonly kind: "general" | "changes_requested";
  readonly active: boolean;
  readonly version: number;
}

export interface CommentResponse {
  readonly comment: ReviewComment;
  readonly created: boolean;
  readonly approval_state: ApprovalState;
}

export interface CommentListResponse {
  readonly comments: readonly ReviewComment[];
  readonly approval_state: ApprovalState;
}

export interface DismissApprovalRequest {
  readonly reason?: string;
}

export interface RunCreateRequest {
  readonly run_id?: string;
  readonly prompt: string;
  readonly command: string;
  readonly stdout?: string;
  readonly stderr?: string;
  readonly result?: string;
  readonly status?: "queued" | "running" | "succeeded" | "failed" | "cancelled" | "timed_out";
  readonly exit_code?: number;
  readonly source_commit?: string;
  readonly started_at?: string;
  readonly ended_at?: string;
}

export interface RunCreateResponse {
  readonly run_id: string;
  readonly root: string;
  readonly artifacts: string;
  readonly files: Record<string, string>;
}

export interface RunFilePreview {
  readonly path: string;
  readonly kind: string;
  readonly size: number;
  readonly modified: number;
  readonly encoding: "utf-8" | "binary";
  readonly content_preview: string | null;
  readonly content_truncated: boolean;
}

export interface RunRecord {
  readonly run_id: string;
  readonly root: string;
  readonly artifacts: string;
  readonly files: Record<string, RunFilePreview>;
}

export interface WorkspaceCreateRequest {
  readonly name: string;
  readonly root_path: string;
  readonly base_ref?: string;
  readonly session_ref?: string | null;
}

export interface WorkspaceRecord {
  readonly id: string;
  readonly name: string;
  readonly root_path: string;
  readonly head_commit: string | null;
  readonly version: number;
  readonly base_ref?: string;
  readonly session_ref?: string | null;
}

export interface WorkspaceListResponse {
  readonly workspaces: readonly WorkspaceRecord[];
}

export interface IssueWorkspaceTokenOptions {
  readonly name: string;
  readonly agent_token: string;
  readonly read_prefixes?: readonly string[];
  readonly write_prefixes?: readonly string[];
}

export interface IssueWorkspaceTokenResponse {
  readonly workspace_id: string;
  readonly token_id: string;
  readonly name: string;
  readonly workspace_token: string;
  readonly agent_uid: number;
  readonly read_prefixes: readonly string[];
  readonly write_prefixes: readonly string[];
  readonly base_ref: string;
  readonly session_ref: string | null;
}

export interface CapabilityManifest {
  readonly revision: string;
  readonly server: CapabilityServer;
  readonly auth: CapabilityAuth;
  readonly routes: CapabilityRoutes;
  readonly diff: CapabilityDiff;
  readonly protection: CapabilityProtection;
  readonly idempotency: CapabilityIdempotency;
  readonly recovery: CapabilityRecovery;
  readonly limits: CapabilityLimits;
  readonly hints: CapabilityHints;
}

export interface CapabilityServer {
  readonly name: string;
  readonly version: string;
  readonly build: string | null;
  readonly backend_mode: "local" | "durable";
  readonly core_runtime: "local-state" | "durable-cloud";
  readonly build_features: readonly string[];
}

export interface CapabilityAuth {
  readonly modes: readonly ("user" | "bearer" | "workspace")[];
  readonly providers: readonly CapabilityAuthProvider[];
}

export interface CapabilityAuthProvider {
  readonly id: string;
  readonly label: string;
  readonly default: boolean;
  readonly available: boolean;
}

export interface CapabilityRoutes {
  readonly filesystem: CapabilityFilesystemRoutes;
  readonly search: CapabilitySearchRoutes;
  readonly vcs: CapabilityVcsRoutes;
  readonly review: CapabilityReviewRoutes;
  readonly workspaces: CapabilityWorkspaceRoutes;
  readonly audit: CapabilityRouteOperation;
  readonly runs: CapabilityRouteOperation;
}

export interface CapabilityFilesystemRoutes {
  readonly read: CapabilityRouteOperation;
  readonly list: CapabilityRouteOperation;
  readonly stat: CapabilityRouteOperation;
  readonly write: CapabilityRouteOperation;
  readonly delete: CapabilityRouteOperation;
  readonly patch: CapabilityRouteOperation;
  readonly copy: CapabilityRouteOperation;
  readonly move: CapabilityRouteOperation;
}

export interface CapabilitySearchRoutes {
  readonly grep: CapabilityRouteOperation;
  readonly find: CapabilityRouteOperation;
  readonly tree: CapabilityRouteOperation;
  readonly semantic: CapabilityRouteOperation;
}

export interface CapabilityVcsRoutes {
  readonly log: CapabilityRouteOperation;
  readonly status: CapabilityRouteOperation;
  readonly diff: CapabilityRouteOperation;
  readonly refs: CapabilityVcsRefRoutes;
  readonly commit: CapabilityRouteOperation;
  readonly revert: CapabilityRouteOperation;
  readonly recovery: CapabilityRouteOperation;
}

export interface CapabilityVcsRefRoutes {
  readonly list: CapabilityRouteOperation;
  readonly create: CapabilityRouteOperation;
  readonly update: CapabilityRouteOperation;
}

export interface CapabilityReviewRoutes {
  readonly change_requests: CapabilityRouteOperation;
  readonly approvals: CapabilityRouteOperation;
  readonly reviewers: CapabilityRouteOperation;
  readonly comments: CapabilityRouteOperation;
  readonly merge: CapabilityRouteOperation;
  readonly reject: CapabilityRouteOperation;
  readonly dismiss: CapabilityRouteOperation;
}

export interface CapabilityWorkspaceRoutes {
  readonly list: CapabilityRouteOperation;
  readonly create: CapabilityRouteOperation;
  readonly issue_token: CapabilityRouteOperation;
  readonly revoke_token: CapabilityRouteOperation;
}

export interface CapabilityRouteOperation {
  readonly available: boolean;
  readonly admin: boolean;
  readonly idempotent?: boolean;
  readonly reason?: string;
  readonly tracking_ref?: string;
  readonly blocked_when?: readonly string[];
  readonly requires?: readonly string[];
  readonly execution?: boolean;
  readonly notes?: string;
}

export interface CapabilityDiff {
  readonly format: "text/v1" | string;
  readonly max_text_diff_bytes: number;
  readonly max_text_diff_cells: number;
  readonly context_lines: number;
  readonly supported_fragment_kinds: readonly string[];
  readonly json_format_available: boolean;
}

export interface CapabilityProtection {
  readonly ref_rules: CapabilityProtectionRules;
  readonly path_rules: CapabilityProtectionRules;
}

export interface CapabilityProtectionRules {
  readonly available: boolean;
  readonly required_approvals_max: number;
  readonly require_all_files_viewed_default?: boolean;
  readonly target_ref_optional?: boolean;
}

export interface CapabilityIdempotency {
  readonly header: "Idempotency-Key" | string;
  readonly max_key_bytes: number;
  readonly stale_pending_seconds: number;
  readonly completed_retention_seconds: number;
  readonly endpoints_supported: readonly string[];
}

export interface CapabilityRecovery {
  readonly available: boolean;
  readonly phases: readonly string[];
  readonly destructive_cleanup_enabled: boolean;
  readonly scheduler_present: boolean;
}

export interface CapabilityLimits {
  readonly max_file_size_bytes: number;
  readonly max_inodes: number;
  readonly max_depth: number;
  readonly audit_default_limit: number;
  readonly audit_max_limit: number;
  readonly log_max_limit: number;
}

export interface CapabilityHints {
  readonly banner: CapabilityBanner | null;
  readonly branding: unknown | null;
  readonly support_url: string | null;
}

export interface CapabilityBanner {
  readonly kind: "info" | "warn";
  readonly text: string;
}
