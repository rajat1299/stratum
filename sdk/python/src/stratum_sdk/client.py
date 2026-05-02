"""High-level Stratum client mirroring sdk/typescript/src/client.ts (sync)."""

from __future__ import annotations

from typing import Any, cast

import httpx

from stratum_sdk.errors import UnsupportedFeatureError
from stratum_sdk.http import AuthType, StratumHttpClient, WorkspaceAuth
from stratum_sdk.paths import encode_route_segment, fs_route, normalize_route_path, ref_route, tree_route
from stratum_sdk.types import (
    ApprovalListResponse,
    ApprovalRequest,
    ApprovalResponse,
    ChangeRequestCreateRequest,
    ChangeRequestListResponse,
    ChangeRequestResponse,
    CommentListResponse,
    CommentRequest,
    CommentResponse,
    CreateRefRequest,
    DismissApprovalRequest,
    IssueWorkspaceTokenOptions,
    IssueWorkspaceTokenResponse,
    ProtectedPathRule,
    ProtectedPathRuleListResponse,
    ProtectedPathRuleRequest,
    ProtectedRefRule,
    ProtectedRefRuleListResponse,
    ProtectedRefRuleRequest,
    ReviewerListResponse,
    ReviewerRequest,
    ReviewerResponse,
    RunCreateRequest,
    RunCreateResponse,
    RunRecord,
    StratumCommitLog,
    StratumCommitResult,
    StratumCopyResult,
    StratumDeleteResult,
    StratumDirectoryListing,
    StratumFindResult,
    StratumGrepResult,
    StratumMetadataPatch,
    StratumMetadataPatchResult,
    StratumMkdirResult,
    StratumMoveResult,
    StratumRef,
    StratumRefsResult,
    StratumRevertResult,
    StratumStat,
    StratumWriteResult,
    UpdateRefRequest,
    WorkspaceCreateRequest,
    WorkspaceListResponse,
    WorkspaceRecord,
)


class StratumClient:
    """Public SDK entrypoint; synchronous HTTP via :class:`StratumHttpClient`."""

    fs: FilesystemClient
    search: SearchClient
    vcs: VcsClient
    reviews: ReviewsClient
    runs: RunsClient
    workspaces: WorkspacesClient

    def __init__(
        self,
        base_url: str,
        auth: AuthType = None,
        *,
        workspace_id: str | None = None,
        workspace_token: str | None = None,
        http_client: httpx.Client | None = None,
        timeout: float | httpx.Timeout | None = None,
        idempotency_key_prefix: str = "stratum-python-sdk",
    ) -> None:
        resolved = _resolve_auth(auth, workspace_id, workspace_token)
        self._http = StratumHttpClient(
            base_url,
            resolved,
            client=http_client,
            timeout=timeout,
            idempotency_key_prefix=idempotency_key_prefix,
        )
        self.fs = FilesystemClient(self._http)
        self.search = SearchClient(self._http)
        self.vcs = VcsClient(self._http)
        self.reviews = ReviewsClient(self._http)
        self.runs = RunsClient(self._http)
        self.workspaces = WorkspacesClient(self._http)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> StratumClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def read_file(self, path: str) -> str:
        return self.fs.read_file(path)

    def read_file_bytes(self, path: str) -> bytes:
        return self.fs.read_file_bytes(path)

    def write_file(
        self,
        path: str,
        content: str | bytes,
        *,
        mime_type: str | None = None,
        idempotency_key: str | None = None,
    ) -> StratumWriteResult:
        return self.fs.write_file(path, content, mime_type=mime_type, idempotency_key=idempotency_key)

    def mkdir(self, path: str, *, idempotency_key: str | None = None) -> StratumMkdirResult:
        return self.fs.mkdir(path, idempotency_key=idempotency_key)

    def list_directory(self, path: str = "") -> StratumDirectoryListing:
        return self.fs.list_directory(path)

    def stat(self, path: str) -> StratumStat:
        return self.fs.stat(path)

    def delete_path(
        self,
        path: str,
        recursive: bool = False,
        *,
        idempotency_key: str | None = None,
    ) -> StratumDeleteResult:
        return self.fs.delete_path(path, recursive, idempotency_key=idempotency_key)

    def copy_path(
        self,
        source: str,
        destination: str,
        *,
        idempotency_key: str | None = None,
    ) -> StratumCopyResult:
        return self.fs.copy_path(source, destination, idempotency_key=idempotency_key)

    def move_path(
        self,
        source: str,
        destination: str,
        *,
        idempotency_key: str | None = None,
    ) -> StratumMoveResult:
        return self.fs.move_path(source, destination, idempotency_key=idempotency_key)

    def grep(self, pattern: str, *, path: str = "", recursive: bool = True) -> StratumGrepResult:
        return self.search.grep(pattern, path=path, recursive=recursive)

    def find(self, name: str, *, path: str = "") -> StratumFindResult:
        return self.search.find(name, path=path)

    def tree(self, path: str = "") -> str:
        return self.search.tree(path)

    def status(self) -> str:
        return self.vcs.status()

    def diff(self, path: str | None = None) -> str:
        return self.vcs.diff(path)

    def commit(self, message: str, *, idempotency_key: str | None = None) -> StratumCommitResult:
        return self.vcs.commit(message, idempotency_key=idempotency_key)


class FilesystemClient:
    def __init__(self, http: StratumHttpClient) -> None:
        self._http = http

    def read_file(self, path: str) -> str:
        return self._http.request_text(fs_route(path), "GET")

    def read_file_bytes(self, path: str) -> bytes:
        return self._http.request_bytes(fs_route(path), "GET")

    def write_file(
        self,
        path: str,
        content: str | bytes,
        *,
        mime_type: str | None = None,
        idempotency_key: str | None = None,
    ) -> StratumWriteResult:
        headers = {"X-Stratum-Mime-Type": mime_type} if mime_type else None
        return cast(
            StratumWriteResult,
            self._http.request_json(
                fs_route(path),
                "PUT",
                headers=headers,
                body=content,
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def mkdir(self, path: str, *, idempotency_key: str | None = None) -> StratumMkdirResult:
        return cast(
            StratumMkdirResult,
            self._http.request_json(
                fs_route(path),
                "PUT",
                headers={"X-Stratum-Type": "directory"},
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def list_directory(self, path: str = "") -> StratumDirectoryListing:
        return cast(
            StratumDirectoryListing,
            self._http.request_json(fs_route(path), "GET"),
        )

    def stat(self, path: str) -> StratumStat:
        if normalize_route_path(path) == "":
            raise ValueError(
                "StratumClient.stat does not support the workspace root; use listDirectory instead"
            )
        return cast(
            StratumStat,
            self._http.request_json(fs_route(path), "GET", query=[("stat", "true")]),
        )

    def patch_metadata(
        self,
        path: str,
        patch: StratumMetadataPatch,
        *,
        idempotency_key: str | None = None,
    ) -> StratumMetadataPatchResult:
        return cast(
            StratumMetadataPatchResult,
            self._http.request_json(
                fs_route(path),
                "PATCH",
                body=patch,
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def delete_path(
        self,
        path: str,
        recursive: bool = False,
        *,
        idempotency_key: str | None = None,
    ) -> StratumDeleteResult:
        return cast(
            StratumDeleteResult,
            self._http.request_json(
                fs_route(path),
                "DELETE",
                query=[("recursive", str(recursive).lower())],
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def copy_path(
        self,
        source: str,
        destination: str,
        *,
        idempotency_key: str | None = None,
    ) -> StratumCopyResult:
        return cast(
            StratumCopyResult,
            self._http.request_json(
                fs_route(source),
                "POST",
                query=[("op", "copy"), ("dst", destination)],
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def move_path(
        self,
        source: str,
        destination: str,
        *,
        idempotency_key: str | None = None,
    ) -> StratumMoveResult:
        return cast(
            StratumMoveResult,
            self._http.request_json(
                fs_route(source),
                "POST",
                query=[("op", "move"), ("dst", destination)],
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )


class SearchClient:
    def __init__(self, http: StratumHttpClient) -> None:
        self._http = http

    def grep(self, pattern: str, *, path: str = "", recursive: bool = True) -> StratumGrepResult:
        return cast(
            StratumGrepResult,
            self._http.request_json(
                "search/grep",
                "GET",
                query=[
                    ("pattern", pattern),
                    ("path", path),
                    ("recursive", str(recursive).lower()),
                ],
            ),
        )

    def find(self, name: str, *, path: str = "") -> StratumFindResult:
        return cast(
            StratumFindResult,
            self._http.request_json(
                "search/find",
                "GET",
                query=[("path", path), ("name", name)],
            ),
        )

    def tree(self, path: str = "") -> str:
        return self._http.request_text(tree_route(path), "GET")

    def semantic(self, _query: str) -> None:
        raise UnsupportedFeatureError("Semantic search is not supported by the current Stratum backend.")


class VcsClient:
    def __init__(self, http: StratumHttpClient) -> None:
        self._http = http

    def commit(self, message: str, *, idempotency_key: str | None = None) -> StratumCommitResult:
        return cast(
            StratumCommitResult,
            self._http.request_json(
                "vcs/commit",
                "POST",
                body={"message": message},
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def log(self) -> StratumCommitLog:
        return cast(StratumCommitLog, self._http.request_json("vcs/log", "GET"))

    def revert(self, hash: str, *, idempotency_key: str | None = None) -> StratumRevertResult:
        return cast(
            StratumRevertResult,
            self._http.request_json(
                "vcs/revert",
                "POST",
                body={"hash": hash},
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def status(self) -> str:
        return self._http.request_text("vcs/status", "GET")

    def diff(self, path: str | None = None) -> str:
        query = None if path is None else [("path", path)]
        return self._http.request_text("vcs/diff", "GET", query=query)

    def list_refs(self) -> StratumRefsResult:
        return cast(StratumRefsResult, self._http.request_json("vcs/refs", "GET"))

    def create_ref(
        self,
        request: CreateRefRequest,
        *,
        idempotency_key: str | None = None,
    ) -> StratumRef:
        return cast(
            StratumRef,
            self._http.request_json(
                "vcs/refs",
                "POST",
                body=request,
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def update_ref(
        self,
        name: str,
        request: UpdateRefRequest,
        *,
        idempotency_key: str | None = None,
    ) -> StratumRef:
        return cast(
            StratumRef,
            self._http.request_json(
                ref_route(name),
                "PATCH",
                body=request,
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )


class ReviewsClient:
    def __init__(self, http: StratumHttpClient) -> None:
        self._http = http

    def create_protected_ref(
        self,
        request: ProtectedRefRuleRequest,
        *,
        idempotency_key: str | None = None,
    ) -> ProtectedRefRule:
        return cast(
            ProtectedRefRule,
            self._http.request_json(
                "protected/refs",
                "POST",
                body=request,
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def list_protected_refs(self) -> ProtectedRefRuleListResponse:
        return cast(
            ProtectedRefRuleListResponse,
            self._http.request_json("protected/refs", "GET"),
        )

    def create_protected_path(
        self,
        request: ProtectedPathRuleRequest,
        *,
        idempotency_key: str | None = None,
    ) -> ProtectedPathRule:
        return cast(
            ProtectedPathRule,
            self._http.request_json(
                "protected/paths",
                "POST",
                body=request,
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def list_protected_paths(self) -> ProtectedPathRuleListResponse:
        return cast(
            ProtectedPathRuleListResponse,
            self._http.request_json("protected/paths", "GET"),
        )

    def list_change_requests(self) -> ChangeRequestListResponse:
        return cast(
            ChangeRequestListResponse,
            self._http.request_json("change-requests", "GET"),
        )

    def get_change_request(self, change_request_id: str) -> ChangeRequestResponse:
        return cast(
            ChangeRequestResponse,
            self._http.request_json(
                f"change-requests/{encode_route_segment(change_request_id)}",
                "GET",
            ),
        )

    def create_change_request(
        self,
        request: ChangeRequestCreateRequest,
        *,
        idempotency_key: str | None = None,
    ) -> ChangeRequestResponse:
        return cast(
            ChangeRequestResponse,
            self._http.request_json(
                "change-requests",
                "POST",
                body=request,
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def approve(
        self,
        change_request_id: str,
        request: ApprovalRequest | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> ApprovalResponse:
        body: Any = request if request is not None else {}
        return cast(
            ApprovalResponse,
            self._http.request_json(
                f"change-requests/{encode_route_segment(change_request_id)}/approvals",
                "POST",
                body=body,
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def list_approvals(self, change_request_id: str) -> ApprovalListResponse:
        return cast(
            ApprovalListResponse,
            self._http.request_json(
                f"change-requests/{encode_route_segment(change_request_id)}/approvals",
                "GET",
            ),
        )

    def assign_reviewer(
        self,
        change_request_id: str,
        request: ReviewerRequest,
        *,
        idempotency_key: str | None = None,
    ) -> ReviewerResponse:
        return cast(
            ReviewerResponse,
            self._http.request_json(
                f"change-requests/{encode_route_segment(change_request_id)}/reviewers",
                "POST",
                body=request,
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def list_reviewers(self, change_request_id: str) -> ReviewerListResponse:
        return cast(
            ReviewerListResponse,
            self._http.request_json(
                f"change-requests/{encode_route_segment(change_request_id)}/reviewers",
                "GET",
            ),
        )

    def create_comment(
        self,
        change_request_id: str,
        request: CommentRequest,
        *,
        idempotency_key: str | None = None,
    ) -> CommentResponse:
        return cast(
            CommentResponse,
            self._http.request_json(
                f"change-requests/{encode_route_segment(change_request_id)}/comments",
                "POST",
                body=request,
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def list_comments(self, change_request_id: str) -> CommentListResponse:
        return cast(
            CommentListResponse,
            self._http.request_json(
                f"change-requests/{encode_route_segment(change_request_id)}/comments",
                "GET",
            ),
        )

    def dismiss_approval(
        self,
        change_request_id: str,
        approval_id: str,
        request: DismissApprovalRequest | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> ApprovalResponse:
        body: Any = request if request is not None else {}
        return cast(
            ApprovalResponse,
            self._http.request_json(
                f"change-requests/{encode_route_segment(change_request_id)}"
                f"/approvals/{encode_route_segment(approval_id)}/dismiss",
                "POST",
                body=body,
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def reject(self, change_request_id: str, *, idempotency_key: str | None = None) -> ChangeRequestResponse:
        return cast(
            ChangeRequestResponse,
            self._http.request_json(
                f"change-requests/{encode_route_segment(change_request_id)}/reject",
                "POST",
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def merge(self, change_request_id: str, *, idempotency_key: str | None = None) -> ChangeRequestResponse:
        return cast(
            ChangeRequestResponse,
            self._http.request_json(
                f"change-requests/{encode_route_segment(change_request_id)}/merge",
                "POST",
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )


class RunsClient:
    def __init__(self, http: StratumHttpClient) -> None:
        self._http = http

    def create(
        self,
        request: RunCreateRequest,
        *,
        idempotency_key: str | None = None,
    ) -> RunCreateResponse:
        return cast(
            RunCreateResponse,
            self._http.request_json(
                "runs",
                "POST",
                body=request,
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def get(self, run_id: str) -> RunRecord:
        return cast(
            RunRecord,
            self._http.request_json(f"runs/{encode_route_segment(run_id)}", "GET"),
        )

    def stdout(self, run_id: str) -> str:
        return self._http.request_text(f"runs/{encode_route_segment(run_id)}/stdout", "GET")

    def stderr(self, run_id: str) -> str:
        return self._http.request_text(f"runs/{encode_route_segment(run_id)}/stderr", "GET")


class WorkspacesClient:
    def __init__(self, http: StratumHttpClient) -> None:
        self._http = http

    def list(self) -> WorkspaceListResponse:
        return cast(WorkspaceListResponse, self._http.request_json("workspaces", "GET"))

    def get(self, workspace_id: str) -> WorkspaceRecord:
        return cast(
            WorkspaceRecord,
            self._http.request_json(f"workspaces/{encode_route_segment(workspace_id)}", "GET"),
        )

    def create(
        self,
        request: WorkspaceCreateRequest,
        *,
        idempotency_key: str | None = None,
    ) -> WorkspaceRecord:
        return cast(
            WorkspaceRecord,
            self._http.request_json(
                "workspaces",
                "POST",
                body=request,
                idempotency_key=idempotency_key,
                auto_idempotency=True,
            ),
        )

    def issue_token(
        self,
        workspace_id: str,
        request: IssueWorkspaceTokenOptions,
    ) -> IssueWorkspaceTokenResponse:
        return cast(
            IssueWorkspaceTokenResponse,
            self._http.request_json(
                f"workspaces/{encode_route_segment(workspace_id)}/tokens",
                "POST",
                body=request,
            ),
        )


def _resolve_auth(
    auth: AuthType,
    workspace_id: str | None,
    workspace_token: str | None,
) -> AuthType:
    if auth is not None:
        return auth
    if workspace_id is not None and workspace_token is not None:
        return WorkspaceAuth(workspace_id, workspace_token)
    return None
