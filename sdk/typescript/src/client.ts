import { UnsupportedFeatureError } from "./errors.js";
import { StratumHttpClient } from "./http.js";
import { encodeRouteSegment, fsRoute, normalizeRoutePath, refRoute, treeRoute } from "./paths.js";
import type {
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
  StratumAuth,
  StratumClientOptions,
  StratumCommitLog,
  StratumCommitResult,
  StratumCopyResult,
  StratumDeleteResult,
  StratumDirectoryListing,
  StratumFindOptions,
  StratumFindResult,
  StratumGrepOptions,
  StratumGrepResult,
  StratumMetadataPatch,
  StratumMetadataPatchResult,
  StratumMkdirResult,
  StratumMoveResult,
  StratumMutationOptions,
  StratumRef,
  StratumRefsResult,
  StratumRequestBody,
  StratumRevertResult,
  StratumStat,
  StratumWriteOptions,
  StratumWriteResult,
  UpdateRefRequest,
  WorkspaceCreateRequest,
  WorkspaceListResponse,
  WorkspaceRecord,
} from "./types.js";

export class StratumClient {
  readonly fs: FilesystemClient;
  readonly search: SearchClient;
  readonly vcs: VcsClient;
  readonly reviews: ReviewsClient;
  readonly runs: RunsClient;
  readonly workspaces: WorkspacesClient;

  private readonly http: StratumHttpClient;

  constructor(options: StratumClientOptions) {
    const fetchImpl = options.fetch ?? globalThis.fetch;
    if (!fetchImpl) {
      throw new Error("StratumClient requires fetch");
    }

    this.http = new StratumHttpClient({
      baseUrl: options.baseUrl,
      fetchImpl,
      auth: resolveAuth(options),
      idempotencyKeyPrefix: options.idempotencyKeyPrefix ?? "stratum-sdk",
    });
    this.fs = new FilesystemClient(this.http);
    this.search = new SearchClient(this.http);
    this.vcs = new VcsClient(this.http);
    this.reviews = new ReviewsClient(this.http);
    this.runs = new RunsClient(this.http);
    this.workspaces = new WorkspacesClient(this.http);
  }

  readFile(path: string): Promise<string> {
    return this.fs.readFile(path);
  }

  readFileBuffer(path: string): Promise<Uint8Array> {
    return this.fs.readFileBuffer(path);
  }

  writeFile(path: string, content: StratumRequestBody, options?: StratumWriteOptions): Promise<StratumWriteResult> {
    return this.fs.writeFile(path, content, options);
  }

  mkdir(path: string, options?: StratumMutationOptions): Promise<StratumMkdirResult> {
    return this.fs.mkdir(path, options);
  }

  listDirectory(path?: string): Promise<StratumDirectoryListing> {
    return this.fs.listDirectory(path);
  }

  stat(path: string): Promise<StratumStat> {
    return this.fs.stat(path);
  }

  deletePath(path: string, recursive?: boolean, options?: StratumMutationOptions): Promise<StratumDeleteResult> {
    return this.fs.deletePath(path, recursive, options);
  }

  copyPath(source: string, destination: string, options?: StratumMutationOptions): Promise<StratumCopyResult> {
    return this.fs.copyPath(source, destination, options);
  }

  movePath(source: string, destination: string, options?: StratumMutationOptions): Promise<StratumMoveResult> {
    return this.fs.movePath(source, destination, options);
  }

  grep(pattern: string, path = "", recursive = true): Promise<StratumGrepResult> {
    return this.search.grep(pattern, { path, recursive });
  }

  find(name: string, path = ""): Promise<StratumFindResult> {
    return this.search.find(name, { path });
  }

  tree(path = ""): Promise<string> {
    return this.search.tree(path);
  }

  status(): Promise<string> {
    return this.vcs.status();
  }

  diff(path?: string): Promise<string> {
    return this.vcs.diff(path);
  }

  commit(message: string, options?: StratumMutationOptions): Promise<StratumCommitResult> {
    return this.vcs.commit(message, options);
  }
}

export class FilesystemClient {
  constructor(private readonly http: StratumHttpClient) {}

  readFile(path: string): Promise<string> {
    return this.http.text(fsRoute(path), { method: "GET" });
  }

  readFileBuffer(path: string): Promise<Uint8Array> {
    return this.http.bytes(fsRoute(path), { method: "GET" });
  }

  writeFile(path: string, content: StratumRequestBody, options: StratumWriteOptions = {}): Promise<StratumWriteResult> {
    return this.http.json(fsRoute(path), {
      method: "PUT",
      body: content,
      headers: options.mimeType ? { "X-Stratum-Mime-Type": options.mimeType } : undefined,
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  mkdir(path: string, options: StratumMutationOptions = {}): Promise<StratumMkdirResult> {
    return this.http.json(fsRoute(path), {
      method: "PUT",
      headers: { "X-Stratum-Type": "directory" },
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  listDirectory(path = ""): Promise<StratumDirectoryListing> {
    return this.http.json(fsRoute(path), { method: "GET" });
  }

  stat(path: string): Promise<StratumStat> {
    if (normalizeRoutePath(path) === "") {
      return Promise.reject(new Error("StratumClient.stat does not support the workspace root; use listDirectory instead"));
    }

    return this.http.json(fsRoute(path), { method: "GET", query: [["stat", "true"]] });
  }

  patchMetadata(
    path: string,
    patch: StratumMetadataPatch,
    options: StratumMutationOptions = {},
  ): Promise<StratumMetadataPatchResult> {
    return this.http.json(fsRoute(path), {
      method: "PATCH",
      body: patch,
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  deletePath(path: string, recursive = false, options: StratumMutationOptions = {}): Promise<StratumDeleteResult> {
    return this.http.json(fsRoute(path), {
      method: "DELETE",
      query: [["recursive", String(recursive)]],
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  copyPath(source: string, destination: string, options: StratumMutationOptions = {}): Promise<StratumCopyResult> {
    return this.http.json(fsRoute(source), {
      method: "POST",
      query: [
        ["op", "copy"],
        ["dst", destination],
      ],
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  movePath(source: string, destination: string, options: StratumMutationOptions = {}): Promise<StratumMoveResult> {
    return this.http.json(fsRoute(source), {
      method: "POST",
      query: [
        ["op", "move"],
        ["dst", destination],
      ],
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }
}

export class SearchClient {
  constructor(private readonly http: StratumHttpClient) {}

  grep(pattern: string, options: StratumGrepOptions = {}): Promise<StratumGrepResult> {
    return this.http.json("search/grep", {
      method: "GET",
      query: [
        ["pattern", pattern],
        ["path", options.path ?? ""],
        ["recursive", String(options.recursive ?? true)],
      ],
    });
  }

  find(name: string, options: StratumFindOptions = {}): Promise<StratumFindResult> {
    return this.http.json("search/find", {
      method: "GET",
      query: [
        ["path", options.path ?? ""],
        ["name", name],
      ],
    });
  }

  tree(path = ""): Promise<string> {
    return this.http.text(treeRoute(path), { method: "GET" });
  }

  semantic(_query: string): never {
    throw new UnsupportedFeatureError("Semantic search is not supported by the current Stratum backend.");
  }
}

export class VcsClient {
  constructor(private readonly http: StratumHttpClient) {}

  commit(message: string, options: StratumMutationOptions = {}): Promise<StratumCommitResult> {
    return this.http.json("vcs/commit", {
      method: "POST",
      body: { message },
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  log(): Promise<StratumCommitLog> {
    return this.http.json("vcs/log", { method: "GET" });
  }

  revert(hash: string, options: StratumMutationOptions = {}): Promise<StratumRevertResult> {
    return this.http.json("vcs/revert", {
      method: "POST",
      body: { hash },
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  status(): Promise<string> {
    return this.http.text("vcs/status", { method: "GET" });
  }

  diff(path?: string): Promise<string> {
    return this.http.text("vcs/diff", {
      method: "GET",
      query: path === undefined ? undefined : [["path", path]],
    });
  }

  listRefs(): Promise<StratumRefsResult> {
    return this.http.json("vcs/refs", { method: "GET" });
  }

  createRef(request: CreateRefRequest, options: StratumMutationOptions = {}): Promise<StratumRef> {
    return this.http.json("vcs/refs", {
      method: "POST",
      body: request,
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  updateRef(name: string, request: UpdateRefRequest, options: StratumMutationOptions = {}): Promise<StratumRef> {
    return this.http.json(refRoute(name), {
      method: "PATCH",
      body: request,
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }
}

export class ReviewsClient {
  constructor(private readonly http: StratumHttpClient) {}

  createProtectedRef(
    request: ProtectedRefRuleRequest,
    options: StratumMutationOptions = {},
  ): Promise<ProtectedRefRule> {
    return this.http.json("protected/refs", {
      method: "POST",
      body: request,
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  listProtectedRefs(): Promise<ProtectedRefRuleListResponse> {
    return this.http.json("protected/refs", { method: "GET" });
  }

  createProtectedPath(
    request: ProtectedPathRuleRequest,
    options: StratumMutationOptions = {},
  ): Promise<ProtectedPathRule> {
    return this.http.json("protected/paths", {
      method: "POST",
      body: request,
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  listProtectedPaths(): Promise<ProtectedPathRuleListResponse> {
    return this.http.json("protected/paths", { method: "GET" });
  }

  listChangeRequests(): Promise<ChangeRequestListResponse> {
    return this.http.json("change-requests", { method: "GET" });
  }

  getChangeRequest(id: string): Promise<ChangeRequestResponse> {
    return this.http.json(`change-requests/${encodeRouteSegment(id)}`, { method: "GET" });
  }

  createChangeRequest(
    request: ChangeRequestCreateRequest,
    options: StratumMutationOptions = {},
  ): Promise<ChangeRequestResponse> {
    return this.http.json("change-requests", {
      method: "POST",
      body: request,
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  approve(id: string, request: ApprovalRequest = {}, options: StratumMutationOptions = {}): Promise<ApprovalResponse> {
    return this.http.json(`change-requests/${encodeRouteSegment(id)}/approvals`, {
      method: "POST",
      body: request,
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  listApprovals(id: string): Promise<ApprovalListResponse> {
    return this.http.json(`change-requests/${encodeRouteSegment(id)}/approvals`, { method: "GET" });
  }

  assignReviewer(id: string, request: ReviewerRequest, options: StratumMutationOptions = {}): Promise<ReviewerResponse> {
    return this.http.json(`change-requests/${encodeRouteSegment(id)}/reviewers`, {
      method: "POST",
      body: request,
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  listReviewers(id: string): Promise<ReviewerListResponse> {
    return this.http.json(`change-requests/${encodeRouteSegment(id)}/reviewers`, { method: "GET" });
  }

  createComment(
    id: string,
    request: CommentRequest,
    options: StratumMutationOptions = {},
  ): Promise<CommentResponse> {
    return this.http.json(`change-requests/${encodeRouteSegment(id)}/comments`, {
      method: "POST",
      body: request,
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  listComments(id: string): Promise<CommentListResponse> {
    return this.http.json(`change-requests/${encodeRouteSegment(id)}/comments`, { method: "GET" });
  }

  dismissApproval(
    id: string,
    approvalId: string,
    request: DismissApprovalRequest = {},
    options: StratumMutationOptions = {},
  ): Promise<ApprovalResponse> {
    return this.http.json(
      `change-requests/${encodeRouteSegment(id)}/approvals/${encodeRouteSegment(approvalId)}/dismiss`,
      {
        method: "POST",
        body: request,
        idempotencyKey: options.idempotencyKey,
        autoIdempotency: true,
      },
    );
  }

  reject(id: string, options: StratumMutationOptions = {}): Promise<ChangeRequestResponse> {
    return this.http.json(`change-requests/${encodeRouteSegment(id)}/reject`, {
      method: "POST",
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  merge(id: string, options: StratumMutationOptions = {}): Promise<ChangeRequestResponse> {
    return this.http.json(`change-requests/${encodeRouteSegment(id)}/merge`, {
      method: "POST",
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }
}

export class RunsClient {
  constructor(private readonly http: StratumHttpClient) {}

  create(request: RunCreateRequest, options: StratumMutationOptions = {}): Promise<RunCreateResponse> {
    return this.http.json("runs", {
      method: "POST",
      body: request,
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  get(runId: string): Promise<RunRecord> {
    return this.http.json(`runs/${encodeRouteSegment(runId)}`, { method: "GET" });
  }

  stdout(runId: string): Promise<string> {
    return this.http.text(`runs/${encodeRouteSegment(runId)}/stdout`, { method: "GET" });
  }

  stderr(runId: string): Promise<string> {
    return this.http.text(`runs/${encodeRouteSegment(runId)}/stderr`, { method: "GET" });
  }
}

export class WorkspacesClient {
  constructor(private readonly http: StratumHttpClient) {}

  list(): Promise<WorkspaceListResponse> {
    return this.http.json("workspaces", { method: "GET" });
  }

  get(workspaceId: string): Promise<WorkspaceRecord> {
    return this.http.json(`workspaces/${encodeRouteSegment(workspaceId)}`, { method: "GET" });
  }

  create(request: WorkspaceCreateRequest, options: StratumMutationOptions = {}): Promise<WorkspaceRecord> {
    return this.http.json("workspaces", {
      method: "POST",
      body: request,
      idempotencyKey: options.idempotencyKey,
      autoIdempotency: true,
    });
  }

  issueToken(workspaceId: string, request: IssueWorkspaceTokenOptions): Promise<IssueWorkspaceTokenResponse> {
    return this.http.json(`workspaces/${encodeRouteSegment(workspaceId)}/tokens`, {
      method: "POST",
      body: request,
    });
  }
}

function resolveAuth(options: StratumClientOptions): StratumAuth | undefined {
  if (options.auth !== undefined) {
    return options.auth;
  }

  if (options.workspaceId !== undefined && options.workspaceToken !== undefined) {
    return {
      type: "workspace",
      workspaceId: options.workspaceId,
      workspaceToken: options.workspaceToken,
    };
  }

  return undefined;
}
