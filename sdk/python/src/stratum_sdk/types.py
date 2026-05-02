"""TypedDict shapes aligned with sdk/typescript/src/types.ts (HTTP JSON field names)."""

from __future__ import annotations

from typing import Literal, NotRequired, TypedDict


class StratumDirectoryEntry(TypedDict):
    name: str
    kind: NotRequired[str]
    is_dir: bool
    is_symlink: bool
    size: int
    mode: str
    uid: int
    gid: int
    modified: int


class StratumDirectoryListing(TypedDict):
    path: str
    entries: list[StratumDirectoryEntry]


class StratumStat(TypedDict):
    inode_id: int
    kind: str
    size: int
    mode: str
    uid: int
    gid: int
    created: int
    modified: int
    mime_type: str | None
    content_hash: str | None
    custom_attrs: dict[str, str]


class StratumWriteResult(TypedDict):
    written: str
    size: int


class StratumMkdirResult(TypedDict):
    created: str
    type: Literal["directory"]


class StratumMetadataPatch(TypedDict, total=False):
    mime_type: str | None
    custom_attrs: dict[str, str]
    remove_custom_attrs: list[str]


class StratumMetadataPatchResult(TypedDict):
    metadata_updated: str
    changed: bool
    mime_type: str | None
    custom_attr_keys: list[str]
    custom_attrs_set: list[str]
    custom_attrs_removed: list[str]


class StratumDeleteResult(TypedDict):
    deleted: str


class StratumCopyResult(TypedDict):
    copied: str
    to: str


class StratumMoveResult(TypedDict):
    moved: str
    to: str


class StratumGrepMatch(TypedDict):
    file: str
    line_num: int
    line: str


class StratumGrepResult(TypedDict):
    results: list[StratumGrepMatch]
    count: int


class StratumFindResult(TypedDict):
    results: list[str]
    count: int


class StratumCommitResult(TypedDict):
    hash: str
    message: str
    author: str


class StratumCommitInfo(TypedDict):
    hash: str
    message: str
    author: str
    timestamp: int


class StratumCommitLog(TypedDict):
    commits: list[StratumCommitInfo]


class StratumRevertResult(TypedDict):
    reverted_to: str


class StratumRef(TypedDict):
    name: str
    target: str
    version: int


class StratumRefsResult(TypedDict):
    refs: list[StratumRef]


class CreateRefRequest(TypedDict):
    name: str
    target: str


class UpdateRefRequest(TypedDict):
    target: str
    expected_target: str
    expected_version: int


class ProtectedRefRuleRequest(TypedDict):
    ref_name: str
    required_approvals: int


class ProtectedRefRule(TypedDict):
    id: str
    ref_name: str
    required_approvals: int
    created_by: int
    active: bool


class ProtectedRefRuleListResponse(TypedDict):
    rules: list[ProtectedRefRule]


class ProtectedPathRuleRequest(TypedDict):
    path_prefix: str
    required_approvals: int
    target_ref: NotRequired[str | None]


class ProtectedPathRule(TypedDict):
    id: str
    path_prefix: str
    target_ref: str | None
    required_approvals: int
    created_by: int
    active: bool


class ProtectedPathRuleListResponse(TypedDict):
    rules: list[ProtectedPathRule]


class ChangeRequestCreateRequest(TypedDict):
    title: str
    source_ref: str
    target_ref: str
    description: NotRequired[str | None]


class ChangeRequest(TypedDict):
    id: str
    title: str
    description: str | None
    source_ref: str
    target_ref: str
    base_commit: str
    head_commit: str
    status: Literal["open", "merged", "rejected"]
    created_by: int
    version: int


class ApprovalPolicyDecision(TypedDict):
    change_request_id: str
    required_approvals: int
    approval_count: int
    approved_by: list[int]
    required_reviewers: list[int]
    approved_required_reviewers: list[int]
    missing_required_reviewers: list[int]
    approved: bool
    matched_ref_rules: list[str]
    matched_path_rules: list[str]


class ApprovalStateUnavailable(TypedDict):
    available: Literal[False]
    error: str


ApprovalState = ApprovalPolicyDecision | ApprovalStateUnavailable


class ChangeRequestResponse(TypedDict):
    change_request: ChangeRequest
    approval_state: ApprovalState


class ChangeRequestListResponse(TypedDict):
    change_requests: list[ChangeRequestResponse]


class ApprovalRequest(TypedDict, total=False):
    comment: str


class ApprovalRecord(TypedDict):
    id: str
    change_request_id: str
    head_commit: str
    approved_by: int
    comment: str | None
    active: bool
    version: int
    dismissed_by: NotRequired[int | None]
    dismissal_reason: NotRequired[str | None]


class ApprovalResponse(TypedDict):
    approval: ApprovalRecord
    approval_state: ApprovalState
    created: NotRequired[bool]
    dismissed: NotRequired[bool]


class ApprovalListResponse(TypedDict):
    approvals: list[ApprovalRecord]
    approval_state: NotRequired[ApprovalState]


class ReviewerRequest(TypedDict):
    reviewer_uid: int
    required: NotRequired[bool]


class ReviewerAssignment(TypedDict):
    id: str
    change_request_id: str
    reviewer: int
    assigned_by: int
    required: bool
    active: bool
    version: int


class ReviewerResponse(TypedDict):
    assignment: ReviewerAssignment
    created: bool
    updated: bool
    approval_state: ApprovalState


class ReviewerListResponse(TypedDict):
    assignments: list[ReviewerAssignment]
    approval_state: ApprovalState


class CommentRequest(TypedDict):
    body: str
    path: NotRequired[str]
    kind: NotRequired[Literal["general", "changes_requested"]]


class ReviewComment(TypedDict):
    id: str
    change_request_id: str
    author: int
    body: str
    path: str | None
    kind: Literal["general", "changes_requested"]
    active: bool
    version: int


class CommentResponse(TypedDict):
    comment: ReviewComment
    created: bool
    approval_state: ApprovalState


class CommentListResponse(TypedDict):
    comments: list[ReviewComment]
    approval_state: ApprovalState


class DismissApprovalRequest(TypedDict, total=False):
    reason: str


class RunCreateRequest(TypedDict):
    prompt: str
    command: str
    run_id: NotRequired[str]
    stdout: NotRequired[str]
    stderr: NotRequired[str]
    result: NotRequired[str]
    status: NotRequired[
        Literal["queued", "running", "succeeded", "failed", "cancelled", "timed_out"]
    ]
    exit_code: NotRequired[int]
    source_commit: NotRequired[str]
    started_at: NotRequired[str]
    ended_at: NotRequired[str]


class RunCreateResponse(TypedDict):
    run_id: str
    root: str
    artifacts: str
    files: dict[str, str]


class RunFilePreview(TypedDict):
    path: str
    kind: str
    size: int
    modified: int
    encoding: Literal["utf-8", "binary"]
    content_preview: str | None
    content_truncated: bool


class RunRecord(TypedDict):
    run_id: str
    root: str
    artifacts: str
    files: dict[str, RunFilePreview]


class WorkspaceCreateRequest(TypedDict):
    name: str
    root_path: str
    base_ref: NotRequired[str]
    session_ref: NotRequired[str | None]


class WorkspaceRecord(TypedDict):
    id: str
    name: str
    root_path: str
    head_commit: str | None
    version: int
    base_ref: NotRequired[str]
    session_ref: NotRequired[str | None]


class WorkspaceListResponse(TypedDict):
    workspaces: list[WorkspaceRecord]


class IssueWorkspaceTokenOptions(TypedDict):
    name: str
    agent_token: str
    read_prefixes: NotRequired[list[str]]
    write_prefixes: NotRequired[list[str]]


class IssueWorkspaceTokenResponse(TypedDict):
    workspace_id: str
    token_id: str
    name: str
    workspace_token: str
    agent_uid: int
    read_prefixes: list[str]
    write_prefixes: list[str]
    base_ref: str
    session_ref: str | None
