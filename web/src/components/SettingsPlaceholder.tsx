import type {
  IssueWorkspaceTokenOptions,
  IssueWorkspaceTokenResponse,
  ProtectedPathRule,
  WorkspaceRecord,
} from "@stratum/sdk";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState, type FormEvent, type ReactNode } from "react";
import { useAuth } from "../lib/auth.tsx";
import { useCapabilities } from "../lib/capabilities.ts";
import { useStratumClient } from "../lib/stratum-client.ts";

const settingsKeys = {
  workspaces: ["settings", "workspaces"] as const,
  protectedRefs: ["settings", "protected-refs"] as const,
  protectedPaths: ["settings", "protected-paths"] as const,
};

export function SettingsPlaceholder() {
  const auth = useAuth();
  const client = useStratumClient();
  const queryClient = useQueryClient();
  const capabilities = useCapabilities();
  const settingsKnown = capabilities.data !== undefined;
  const workspaceListAvailable = capabilities.data?.routes.workspaces.list.available === true;
  const workspaceCreateAvailable = capabilities.data?.routes.workspaces.create.available === true;
  const workspaceTokenAvailable = capabilities.data?.routes.workspaces.issue_token.available === true;
  const hostedTokenIdentity =
    capabilities.data?.routes.workspaces.issue_token.requires?.includes("durable-principal-uid") === true;
  const refRulesAvailable = capabilities.data?.protection.ref_rules.available === true;
  const pathRulesAvailable = capabilities.data?.protection.path_rules.available === true;
  const workspaces = useQuery({
    queryKey: settingsKeys.workspaces,
    queryFn: () => client.workspaces.list(),
    enabled: workspaceListAvailable,
    staleTime: 15_000,
  });
  const protectedRefs = useQuery({
    queryKey: settingsKeys.protectedRefs,
    queryFn: () => client.reviews.listProtectedRefs(),
    enabled: refRulesAvailable,
    staleTime: 15_000,
  });
  const protectedPaths = useQuery({
    queryKey: settingsKeys.protectedPaths,
    queryFn: () => client.reviews.listProtectedPaths(),
    enabled: pathRulesAvailable,
    staleTime: 15_000,
  });

  const [workspaceForm, setWorkspaceForm] = useState({
    name: "",
    rootPath: "",
    baseRef: "main",
  });
  const [tokenForm, setTokenForm] = useState({
    workspaceId: "",
    name: "",
    agentToken: "",
    readPrefixes: "",
    writePrefixes: "",
  });
  const [branchForm, setBranchForm] = useState({
    refName: "main",
    approvals: "1",
    requireFiles: true,
  });
  const [pathForm, setPathForm] = useState({
    pathPrefix: "",
    targetRef: "main",
    approvals: "1",
    requireFiles: false,
  });
  const [workspaceStatus, setWorkspaceStatus] = useState<string | null>(null);
  const [branchStatus, setBranchStatus] = useState<string | null>(null);
  const [pathStatus, setPathStatus] = useState<string | null>(null);
  const [issuedToken, setIssuedToken] = useState<IssueWorkspaceTokenResponse | null>(null);

  const workspaceList = workspaceListAvailable ? (workspaces.data?.workspaces ?? []) : [];
  const protectedRefRules = refRulesAvailable ? (protectedRefs.data?.rules ?? []) : [];
  const protectedPathRules = pathRulesAvailable ? (protectedPaths.data?.rules ?? []) : [];
  const selectedWorkspaceId = tokenForm.workspaceId || workspaceList[0]?.id || "";
  const firstError =
    capabilities.error ??
    (workspaceListAvailable ? workspaces.error : null) ??
    (refRulesAvailable ? protectedRefs.error : null) ??
    (pathRulesAvailable ? protectedPaths.error : null);
  const loadingSettings = capabilities.isLoading;
  const settingsLoadFailed = !loadingSettings && !settingsKnown;

  const createWorkspace = useMutation({
    mutationFn: () => {
      if (!settingsKnown) {
        throw new Error("Settings could not be loaded.");
      }
      if (!workspaceCreateAvailable) {
        throw new Error("Workspace setup is not available in this hosted preview.");
      }
      return client.workspaces.create({
        name: workspaceForm.name.trim(),
        root_path: workspaceForm.rootPath.trim(),
        ...(workspaceForm.baseRef.trim() ? { base_ref: workspaceForm.baseRef.trim() } : {}),
      });
    },
    onSuccess: async () => {
      setWorkspaceStatus("Workspace ready.");
      setWorkspaceForm({ name: "", rootPath: "", baseRef: "main" });
      await queryClient.invalidateQueries({ queryKey: settingsKeys.workspaces });
    },
  });

  const issueToken = useMutation({
    mutationFn: () => {
      if (!settingsKnown) {
        throw new Error("Settings could not be loaded.");
      }
      if (!workspaceTokenAvailable) {
        throw new Error("Access token issuance is not available in this hosted preview.");
      }
      const identity = hostedTokenIdentity
        ? { principal_uid: parseAgentId(tokenForm.agentToken) }
        : { agent_token: tokenForm.agentToken.trim() };
      const request: IssueWorkspaceTokenOptions = {
        name: tokenForm.name.trim(),
        ...identity,
        read_prefixes: parsePrefixes(tokenForm.readPrefixes),
        write_prefixes: parsePrefixes(tokenForm.writePrefixes),
      };
      return client.workspaces.issueToken(selectedWorkspaceId, request);
    },
    onSuccess: (response) => {
      setIssuedToken(response);
      setTokenForm((current) => ({
        ...current,
        name: "",
        agentToken: "",
        readPrefixes: "",
        writePrefixes: "",
      }));
    },
  });

  const createBranchRule = useMutation({
    mutationFn: () => {
      if (!settingsKnown) {
        throw new Error("Settings could not be loaded.");
      }
      if (!refRulesAvailable) {
        throw new Error("Branch protection is not available here.");
      }
      return client.reviews.createProtectedRef({
        ref_name: branchForm.refName.trim(),
        required_approvals: positiveInt(branchForm.approvals),
        require_all_files_viewed: branchForm.requireFiles,
      });
    },
    onSuccess: async () => {
      setBranchStatus("Branch protected.");
      await queryClient.invalidateQueries({ queryKey: settingsKeys.protectedRefs });
    },
  });

  const createPathRule = useMutation({
    mutationFn: () => {
      if (!settingsKnown) {
        throw new Error("Settings could not be loaded.");
      }
      if (!pathRulesAvailable) {
        throw new Error("Path protection is not available here.");
      }
      return client.reviews.createProtectedPath({
        path_prefix: pathForm.pathPrefix.trim(),
        ...(pathForm.targetRef.trim() ? { target_ref: pathForm.targetRef.trim() } : {}),
        required_approvals: positiveInt(pathForm.approvals),
        require_all_files_viewed: pathForm.requireFiles,
      });
    },
    onSuccess: async () => {
      setPathStatus("Path protected.");
      await queryClient.invalidateQueries({ queryKey: settingsKeys.protectedPaths });
    },
  });

  const session = useMemo(() => sessionLabel(auth.state), [auth.state]);

  return (
    <div className="mx-auto max-w-7xl px-6 py-6">
      <header className="mb-5 flex flex-wrap items-end justify-between gap-4">
        <div>
          <div className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
            Control room
          </div>
          <h1 className="mt-1 text-[24px] font-medium leading-tight tracking-tight text-stone-950">
            Settings
          </h1>
        </div>
        <div className="rounded-[4px] border border-stone-200 bg-white px-3 py-2 text-right">
          <div className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
            Signed in
          </div>
          <div className="mt-0.5 text-[13px] font-medium text-stone-900">{session}</div>
        </div>
      </header>

      {firstError && (
        <p
          role="alert"
          className="mb-4 rounded-[4px] border border-rose-200 bg-rose-50 px-3 py-2 font-mono text-[11.5px] text-rose-800"
        >
          {firstError.message}
        </p>
      )}

      <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_390px]">
        <div className="space-y-4">
          <Panel title="Workspaces" meta={`${workspaceList.length}`}>
            {loadingSettings ? (
              <LoadingRows label="Loading settings" />
            ) : settingsLoadFailed ? (
              <UnavailableMessage>Settings could not be loaded.</UnavailableMessage>
            ) : !workspaceListAvailable ? (
              <UnavailableMessage>Workspace setup is not available in this hosted preview.</UnavailableMessage>
            ) : workspaces.isLoading ? (
              <LoadingRows label="Loading workspaces" />
            ) : (
              <WorkspaceList workspaces={workspaceList} />
            )}
          </Panel>

          <Panel title="Rules" meta={`${protectedRefRules.length + protectedPathRules.length}`}>
            <div className="grid gap-3 lg:grid-cols-2">
              <RuleList
                title="Branches"
                empty="No branch rules."
                loading={loadingSettings || (settingsKnown && refRulesAvailable && protectedRefs.isLoading)}
                unavailable={
                  settingsLoadFailed
                    ? "Settings could not be loaded."
                    : !loadingSettings && settingsKnown && !refRulesAvailable
                      ? "Branch protection is not available here."
                      : undefined
                }
                rules={protectedRefRules.map((rule) => ({
                  id: rule.id,
                  name: rule.ref_name,
                  detail: approvalLabel(rule.required_approvals),
                  strict: rule.require_all_files_viewed,
                }))}
              />
              <RuleList
                title="Paths"
                empty="No path rules."
                loading={loadingSettings || (settingsKnown && pathRulesAvailable && protectedPaths.isLoading)}
                unavailable={
                  settingsLoadFailed
                    ? "Settings could not be loaded."
                    : !loadingSettings && settingsKnown && !pathRulesAvailable
                      ? "Path protection is not available here."
                      : undefined
                }
                rules={protectedPathRules.map((rule) => ({
                  id: rule.id,
                  name: rule.path_prefix,
                  detail: pathRuleDetail(rule),
                  strict: rule.require_all_files_viewed,
                }))}
              />
            </div>
          </Panel>
        </div>

        <aside className="space-y-4">
          <Panel title="New workspace">
            {loadingSettings ? (
              <LoadingRows label="Loading workspace setup" rows={2} />
            ) : settingsLoadFailed ? (
              <UnavailableMessage>Settings could not be loaded.</UnavailableMessage>
            ) : !workspaceCreateAvailable ? (
              <UnavailableMessage>Workspace setup is not available in this hosted preview.</UnavailableMessage>
            ) : (
              <form className="space-y-3" onSubmit={(event) => submit(event, createWorkspace.mutate)}>
                <TextField
                  label="Workspace name"
                  value={workspaceForm.name}
                  onChange={(value) => setWorkspaceForm((current) => ({ ...current, name: value }))}
                  placeholder="Deal room"
                  required
                />
                <TextField
                  label="Workspace path"
                  value={workspaceForm.rootPath}
                  onChange={(value) =>
                    setWorkspaceForm((current) => ({ ...current, rootPath: value }))
                  }
                  placeholder="/contracts"
                  required
                />
                <TextField
                  label="Base branch"
                  value={workspaceForm.baseRef}
                  onChange={(value) => setWorkspaceForm((current) => ({ ...current, baseRef: value }))}
                  placeholder="main"
                />
                <ActionButton loading={createWorkspace.isPending}>Create workspace</ActionButton>
                <MutationState
                  status={workspaceStatus}
                  error={createWorkspace.error}
                  loading={createWorkspace.isPending}
                />
              </form>
            )}
          </Panel>

          <Panel title="Access token">
            {loadingSettings ? (
              <LoadingRows label="Loading token setup" rows={2} />
            ) : settingsLoadFailed ? (
              <UnavailableMessage>Settings could not be loaded.</UnavailableMessage>
            ) : !workspaceListAvailable || !workspaceTokenAvailable ? (
              <UnavailableMessage>
                Access token issuance is not available in this hosted preview.
              </UnavailableMessage>
            ) : (
              <form className="space-y-3" onSubmit={(event) => submit(event, issueToken.mutate)}>
                <label className="block">
                  <span className="mb-1 block font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
                    Workspace
                  </span>
                  <select
                    value={selectedWorkspaceId}
                    onChange={(event) =>
                      setTokenForm((current) => ({ ...current, workspaceId: event.target.value }))
                    }
                    className={inputClassName}
                    disabled={workspaceList.length === 0}
                    required
                  >
                    {workspaceList.map((workspace) => (
                      <option key={workspace.id} value={workspace.id}>
                        {workspace.name}
                      </option>
                    ))}
                  </select>
                </label>
                <TextField
                  label="Token name"
                  value={tokenForm.name}
                  onChange={(value) => setTokenForm((current) => ({ ...current, name: value }))}
                  placeholder="Review bot"
                  required
                />
                {hostedTokenIdentity ? (
                  <TextField
                    label="Agent ID"
                    value={tokenForm.agentToken}
                    onChange={(value) =>
                      setTokenForm((current) => ({ ...current, agentToken: value }))
                    }
                    placeholder="501"
                    type="number"
                    min={0}
                    required
                  />
                ) : (
                  <TextField
                    label="Agent token"
                    value={tokenForm.agentToken}
                    onChange={(value) =>
                      setTokenForm((current) => ({ ...current, agentToken: value }))
                    }
                    placeholder="Paste once"
                    type="password"
                    required
                  />
                )}
                <TextField
                  label="Read access"
                  value={tokenForm.readPrefixes}
                  onChange={(value) =>
                    setTokenForm((current) => ({ ...current, readPrefixes: value }))
                  }
                  placeholder="/contracts"
                />
                <TextField
                  label="Write access"
                  value={tokenForm.writePrefixes}
                  onChange={(value) =>
                    setTokenForm((current) => ({ ...current, writePrefixes: value }))
                  }
                  placeholder="/contracts/redlines"
                />
                <ActionButton loading={issueToken.isPending} disabled={!selectedWorkspaceId}>
                  Issue token
                </ActionButton>
                <MutationState error={issueToken.error} loading={issueToken.isPending} />
                {issuedToken && (
                  <div className="rounded-[4px] border border-orange-200 bg-orange-50 px-3 py-2">
                    <div className="font-mono text-[10.5px] uppercase tracking-wider text-orange-700">
                      Token
                    </div>
                    <div className="mt-1 break-all font-mono text-[12px] text-stone-950">
                      {issuedToken.workspace_token}
                    </div>
                  </div>
                )}
              </form>
            )}
          </Panel>

          <Panel title="Protect branch">
            {loadingSettings ? (
              <LoadingRows label="Loading branch protection" rows={2} />
            ) : settingsLoadFailed ? (
              <UnavailableMessage>Settings could not be loaded.</UnavailableMessage>
            ) : !refRulesAvailable ? (
              <UnavailableMessage>
                Branch protection is not available here.
              </UnavailableMessage>
            ) : (
              <form className="space-y-3" onSubmit={(event) => submit(event, createBranchRule.mutate)}>
                <TextField
                  label="Branch name"
                  value={branchForm.refName}
                  onChange={(value) => setBranchForm((current) => ({ ...current, refName: value }))}
                  required
                />
                <TextField
                  label="Branch approvals"
                  value={branchForm.approvals}
                  onChange={(value) =>
                    setBranchForm((current) => ({ ...current, approvals: value }))
                  }
                  type="number"
                  min={1}
                  required
                />
                <CheckboxField
                  label="Require file review"
                  checked={branchForm.requireFiles}
                  onChange={(checked) =>
                    setBranchForm((current) => ({ ...current, requireFiles: checked }))
                  }
                />
                <ActionButton loading={createBranchRule.isPending}>Protect branch</ActionButton>
                <MutationState
                  status={branchStatus}
                  error={createBranchRule.error}
                  loading={createBranchRule.isPending}
                />
              </form>
            )}
          </Panel>

          <Panel title="Protect path">
            {loadingSettings ? (
              <LoadingRows label="Loading path protection" rows={2} />
            ) : settingsLoadFailed ? (
              <UnavailableMessage>Settings could not be loaded.</UnavailableMessage>
            ) : !pathRulesAvailable ? (
              <UnavailableMessage>Path protection is not available here.</UnavailableMessage>
            ) : (
              <form className="space-y-3" onSubmit={(event) => submit(event, createPathRule.mutate)}>
                <TextField
                  label="Path prefix"
                  value={pathForm.pathPrefix}
                  onChange={(value) =>
                    setPathForm((current) => ({ ...current, pathPrefix: value }))
                  }
                  placeholder="/legal"
                  required
                />
                <TextField
                  label="Path branch"
                  value={pathForm.targetRef}
                  onChange={(value) => setPathForm((current) => ({ ...current, targetRef: value }))}
                  placeholder="main"
                />
                <TextField
                  label="Path approvals"
                  value={pathForm.approvals}
                  onChange={(value) => setPathForm((current) => ({ ...current, approvals: value }))}
                  type="number"
                  min={1}
                  required
                />
                <CheckboxField
                  label="Require file review"
                  checked={pathForm.requireFiles}
                  onChange={(checked) =>
                    setPathForm((current) => ({ ...current, requireFiles: checked }))
                  }
                />
                <ActionButton loading={createPathRule.isPending}>Protect path</ActionButton>
                <MutationState
                  status={pathStatus}
                  error={createPathRule.error}
                  loading={createPathRule.isPending}
                />
              </form>
            )}
          </Panel>
        </aside>
      </div>
    </div>
  );
}

function Panel({
  title,
  meta,
  children,
}: {
  readonly title: string;
  readonly meta?: string;
  readonly children: ReactNode;
}) {
  return (
    <section className="rounded-[4px] border border-stone-200 bg-white">
      <div className="flex items-center justify-between gap-3 border-b border-stone-200 px-4 py-3">
        <h2 className="text-[14px] font-medium text-stone-950">{title}</h2>
        {meta !== undefined && (
          <span className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
            {meta}
          </span>
        )}
      </div>
      <div className="p-4">{children}</div>
    </section>
  );
}

function WorkspaceList({ workspaces }: { readonly workspaces: readonly WorkspaceRecord[] }) {
  if (workspaces.length === 0) {
    return <p className="text-[13px] text-stone-500">No workspaces yet.</p>;
  }

  return (
    <ul className="divide-y divide-stone-100">
      {workspaces.map((workspace) => (
        <li key={workspace.id} className="grid gap-3 py-3 first:pt-0 last:pb-0 sm:grid-cols-[1fr_auto]">
          <div className="min-w-0">
            <div className="truncate text-[14px] font-medium text-stone-950">{workspace.name}</div>
            <div className="mt-1 flex flex-wrap gap-x-2 gap-y-1 font-mono text-[11px] text-stone-500">
              <span>Root {workspace.root_path}</span>
              <span aria-hidden className="text-stone-300">
                |
              </span>
              <span>{workspace.base_ref ?? "main"}</span>
            </div>
          </div>
          <div className="font-mono text-[11px] text-stone-500">
            {workspace.head_commit ? shortHash(workspace.head_commit) : "empty"}
          </div>
        </li>
      ))}
    </ul>
  );
}

function UnavailableMessage({ children }: { readonly children: ReactNode }) {
  return (
    <p className="rounded-[4px] border border-stone-200 bg-stone-50 px-3 py-2 text-[13px] text-stone-500">
      {children}
    </p>
  );
}

function RuleList({
  title,
  empty,
  loading,
  unavailable,
  rules,
}: {
  readonly title: string;
  readonly empty: string;
  readonly loading: boolean;
  readonly unavailable?: string | undefined;
  readonly rules: readonly {
    readonly id: string;
    readonly name: string;
    readonly detail: string;
    readonly strict: boolean;
  }[];
}) {
  return (
    <div>
      <div className="mb-2 font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
        {title}
      </div>
      {loading ? (
        <LoadingRows label={`Loading ${title.toLowerCase()}`} rows={2} />
      ) : unavailable ? (
        <UnavailableMessage>{unavailable}</UnavailableMessage>
      ) : rules.length === 0 ? (
        <p className="rounded-[4px] border border-stone-200 bg-stone-50 px-3 py-2 text-[13px] text-stone-500">
          {empty}
        </p>
      ) : (
        <ul className="divide-y divide-stone-100 rounded-[4px] border border-stone-200">
          {rules.map((rule) => (
            <li key={rule.id} className="px-3 py-2.5">
              <div className="truncate font-mono text-[12px] text-stone-950">{rule.name}</div>
              <div className="mt-1 flex flex-wrap gap-x-2 gap-y-1 font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
                <span>{rule.detail}</span>
                {rule.strict && (
                  <>
                    <span aria-hidden className="text-stone-300">
                      |
                    </span>
                    <span>Files reviewed</span>
                  </>
                )}
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function LoadingRows({ label, rows = 3 }: { readonly label: string; readonly rows?: number }) {
  return (
    <div aria-busy="true" aria-label={label} className="space-y-2">
      {Array.from({ length: rows }).map((_, index) => (
        <div key={index} className="h-12 animate-pulse rounded-[4px] bg-stone-100" />
      ))}
    </div>
  );
}

const inputClassName =
  "block h-9 w-full rounded-[4px] border border-stone-200 bg-white px-2.5 font-mono text-[12px] text-stone-950 outline-none transition focus:border-stone-950 disabled:bg-stone-100 disabled:text-stone-400";

function TextField({
  label,
  value,
  onChange,
  placeholder,
  type = "text",
  min,
  required,
}: {
  readonly label: string;
  readonly value: string;
  readonly onChange: (value: string) => void;
  readonly placeholder?: string;
  readonly type?: string;
  readonly min?: number;
  readonly required?: boolean;
}) {
  return (
    <label className="block">
      <span className="mb-1 block font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
        {label}
      </span>
      <input
        className={inputClassName}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        type={type}
        min={min}
        required={required}
      />
    </label>
  );
}

function CheckboxField({
  label,
  checked,
  onChange,
}: {
  readonly label: string;
  readonly checked: boolean;
  readonly onChange: (checked: boolean) => void;
}) {
  return (
    <label className="flex items-center gap-2 text-[13px] text-stone-700">
      <input
        type="checkbox"
        checked={checked}
        onChange={(event) => onChange(event.target.checked)}
        className="h-4 w-4 rounded-[4px] border-stone-300 text-stone-950 focus:ring-stone-950"
      />
      <span>{label}</span>
    </label>
  );
}

function ActionButton({
  children,
  loading,
  disabled,
}: {
  readonly children: ReactNode;
  readonly loading?: boolean;
  readonly disabled?: boolean;
}) {
  return (
    <button
      type="submit"
      disabled={disabled || loading}
      className="inline-flex h-9 w-full items-center justify-center rounded-[4px] bg-stone-950 px-3 text-[13px] font-medium text-white transition hover:bg-stone-800 disabled:cursor-not-allowed disabled:bg-stone-300"
    >
      {loading ? "Working" : children}
    </button>
  );
}

function MutationState({
  status,
  error,
  loading,
}: {
  readonly status?: string | null;
  readonly error?: Error | null;
  readonly loading: boolean;
}) {
  if (loading) return null;
  if (error) {
    return (
      <p role="alert" className="font-mono text-[11.5px] text-rose-700">
        {error.message}
      </p>
    );
  }
  if (!status) return null;
  return <p className="font-mono text-[11.5px] text-stone-600">{status}</p>;
}

function submit(event: FormEvent<HTMLFormElement>, mutate: () => void) {
  event.preventDefault();
  mutate();
}

function sessionLabel(state: ReturnType<typeof useAuth>["state"]): string {
  if (state.status !== "authed") return "Loading";
  const credentials = state.credentials;
  if (credentials.type === "user") return credentials.username;
  if (credentials.type === "workspace") return "Workspace";
  return "Agent";
}

function parsePrefixes(value: string): readonly string[] {
  return value
    .split(/[,\n]/)
    .map((part) => part.trim())
    .filter(Boolean);
}

function positiveInt(value: string): number {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 1;
}

function parseAgentId(value: string): number {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error("Enter a valid agent ID.");
  }
  const parsed = Number(trimmed);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error("Enter a valid agent ID.");
  }
  return parsed;
}

function approvalLabel(count: number): string {
  return count === 1 ? "1 approval" : `${count} approvals`;
}

function pathRuleDetail(rule: ProtectedPathRule): string {
  const approvals = approvalLabel(rule.required_approvals);
  return rule.target_ref ? `${rule.target_ref} | ${approvals}` : approvals;
}

function shortHash(hash: string): string {
  return hash.slice(0, 8);
}
