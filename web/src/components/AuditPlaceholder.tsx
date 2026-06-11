import type { AuditEvent, AuditOutcome } from "@stratum/sdk";
import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { useCapabilities } from "../lib/capabilities.ts";
import { useStratumClient } from "../lib/stratum-client.ts";

const auditKeys = {
  list: (limit: number) => ["audit", "list", limit] as const,
};

const FALLBACK_AUDIT_LIMIT = 100;
const DETAIL_PREVIEW_LIMIT = 6;
const SAFE_DETAIL_KEYS = new Set([
  "route",
  "change_request_id",
  "source_ref",
  "target_ref",
  "base_commit",
  "head_commit",
  "reverted_to",
  "target_commit",
  "expected_head",
  "path",
  "viewed",
  "viewed_by",
  "version",
  "ref",
  "rule",
  "quota_kind",
  "route_family",
  "has_workspace",
]);
const RISKY_DETAIL_KEY =
  /(^|_)(token|hash|secret|sql|body|content|provider_error|db_url|database_url|r2_endpoint|endpoint|object_key|idempotency_key|commit_message)(_|$)/i;

export function AuditPlaceholder() {
  const client = useStratumClient();
  const [selectedLimit, setSelectedLimit] = useState<number | null>(null);
  const capabilities = useCapabilities();
  const auditSupportKnown = capabilities.data !== undefined;
  const auditAvailable = capabilities.data?.routes.audit.available === true;
  const auditUnavailable = auditSupportKnown && !auditAvailable;
  const auditLoadFailed = !capabilities.isLoading && !auditSupportKnown;
  const auditDefaultLimit = capabilities.data?.limits.audit_default_limit ?? FALLBACK_AUDIT_LIMIT;
  const auditMaxLimit = capabilities.data?.limits.audit_max_limit ?? FALLBACK_AUDIT_LIMIT;
  const limit = Math.min(selectedLimit ?? auditDefaultLimit, auditMaxLimit);
  const limitOptions = auditLimitOptions(limit, auditMaxLimit);
  const audit = useQuery({
    queryKey: auditKeys.list(limit),
    queryFn: () => client.audit.list({ limit }),
    enabled: auditAvailable,
    staleTime: 10_000,
  });

  const events = auditAvailable ? (audit.data?.events ?? []) : [];
  const stats = useMemo(() => summarize(events), [events]);
  const metricsAvailable = !auditUnavailable && !auditLoadFailed;

  return (
    <div className="mx-auto max-w-7xl px-6 py-6">
      <header className="mb-5 flex flex-wrap items-end justify-between gap-4">
        <div>
          <div className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
            Event stream
          </div>
          <h1 className="mt-1 text-[24px] font-medium leading-tight tracking-tight text-stone-950">
            Audit
          </h1>
        </div>
        <label className="block">
          <span className="mb-1 block font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
            Show
          </span>
          <select
            value={limit}
            onChange={(event) => setSelectedLimit(Number(event.target.value))}
            disabled={capabilities.isLoading || auditUnavailable || auditLoadFailed}
            className="h-9 rounded-[4px] border border-stone-200 bg-white px-2.5 font-mono text-[12px] text-stone-950 outline-none transition focus:border-stone-950"
          >
            {limitOptions.map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>
        </label>
      </header>

      {capabilities.error && (
        <p
          role="alert"
          className="mb-4 rounded-[4px] border border-rose-200 bg-rose-50 px-3 py-2 text-[13px] text-rose-800"
        >
          {capabilities.error.message}
        </p>
      )}

      {auditAvailable && audit.error && (
        <p
          role="alert"
          className="mb-4 rounded-[4px] border border-rose-200 bg-rose-50 px-3 py-2 text-[13px] text-rose-800"
        >
          Audit needs an admin user session.
        </p>
      )}

      <div className="mb-4 grid gap-3 sm:grid-cols-3">
        <Metric label="Events" value={metricsAvailable ? events.length.toString() : "Unavailable"} />
        <Metric label="Success" value={metricsAvailable ? stats.success.toString() : "Unavailable"} />
        <Metric label="Needs review" value={metricsAvailable ? stats.partial.toString() : "Unavailable"} />
      </div>

      <section className="rounded-[4px] border border-stone-200 bg-white">
        <div className="flex items-center justify-between gap-3 border-b border-stone-200 px-4 py-3">
          <h2 className="text-[14px] font-medium text-stone-950">Recent events</h2>
          <span className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
            {capabilities.isLoading || audit.isFetching
              ? "Refreshing"
              : auditUnavailable || auditLoadFailed
                ? "Unavailable"
                : "Live"}
          </span>
        </div>
        {capabilities.isLoading ? (
          <LoadingEvents />
        ) : auditLoadFailed ? (
          <p className="p-4 text-[13px] text-stone-500">Audit settings could not be loaded.</p>
        ) : auditUnavailable ? (
          <p className="p-4 text-[13px] text-stone-500">
            Audit events are not available in this hosted preview.
          </p>
        ) : audit.isLoading ? (
          <LoadingEvents />
        ) : events.length === 0 ? (
          <p className="p-4 text-[13px] text-stone-500">No events yet.</p>
        ) : (
          <ol className="divide-y divide-stone-100">
            {events.map((event) => (
              <li key={event.id}>
                <AuditEventRow event={event} />
              </li>
            ))}
          </ol>
        )}
      </section>
    </div>
  );
}

function Metric({ label, value }: { readonly label: string; readonly value: string }) {
  return (
    <div className="rounded-[4px] border border-stone-200 bg-white px-4 py-3">
      <div className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
        {label}
      </div>
      <div className="mt-1 font-mono text-[22px] text-stone-950">{value}</div>
    </div>
  );
}

function AuditEventRow({ event }: { readonly event: AuditEvent }) {
  const target = event.resource.path ?? event.resource.id ?? event.resource.kind;
  const workspace = event.workspace?.session_ref ?? event.workspace?.base_ref ?? null;
  const detailPreview = auditDetailPreview(event.details);
  return (
    <article className="grid gap-3 px-4 py-3 md:grid-cols-[170px_1fr_auto]">
      <div className="font-mono text-[11px] text-stone-500">
        <div>{formatTimestamp(event.timestamp)}</div>
        <div className="mt-1">#{event.sequence}</div>
      </div>

      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <span className="rounded-[4px] bg-stone-950 px-2 py-1 text-[11px] font-medium text-white">
            {actionLabel(event.action)}
          </span>
          <Outcome outcome={event.outcome} />
          {workspace && (
            <span className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
              {workspace}
            </span>
          )}
        </div>
        <div className="mt-2 min-w-0 truncate font-mono text-[12.5px] text-stone-950" title={target}>
          {shortTarget(target)}
        </div>
        {detailPreview.length > 0 && (
          <dl className="mt-2 flex flex-wrap gap-x-3 gap-y-1 font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
            {detailPreview.map(([key, value]) => (
              <div key={key} className="flex gap-1">
                <dt>{key}</dt>
                <dd className="text-stone-700">{value}</dd>
              </div>
            ))}
          </dl>
        )}
      </div>

      <div className="text-left md:text-right">
        <div className="text-[13px] font-medium text-stone-950">{event.actor.username}</div>
        <div className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
          uid {event.actor.uid}
        </div>
      </div>
    </article>
  );
}

function Outcome({ outcome }: { readonly outcome: AuditOutcome }) {
  const partial = outcome === "partial";
  return (
    <span
      className={`rounded-[4px] border px-2 py-1 font-mono text-[10.5px] uppercase tracking-wider ${
        partial
          ? "border-orange-200 bg-orange-50 text-orange-700"
          : "border-stone-200 bg-stone-50 text-stone-600"
      }`}
    >
      {partial ? "Partial" : "Success"}
    </span>
  );
}

function LoadingEvents() {
  return (
    <div aria-busy="true" aria-label="Loading audit events" className="space-y-2 p-4">
      {Array.from({ length: 6 }).map((_, index) => (
        <div key={index} className="h-16 animate-pulse rounded-[4px] bg-stone-100" />
      ))}
    </div>
  );
}

function summarize(events: readonly AuditEvent[]) {
  return events.reduce(
    (acc, event) => {
      if (event.outcome === "partial") acc.partial += 1;
      else acc.success += 1;
      return acc;
    },
    { success: 0, partial: 0 },
  );
}

function actionLabel(action: string): string {
  const labels: Record<string, string> = {
    policy_decision_allow: "Allowed",
    policy_decision_deny: "Blocked",
    fs_write_file: "Wrote file",
    fs_mkdir: "Created folder",
    fs_delete: "Deleted",
    fs_copy: "Copied",
    fs_move: "Moved",
    fs_metadata_update: "Updated metadata",
    vcs_commit: "Committed",
    vcs_revert: "Reverted",
    vcs_ref_create: "Created branch",
    vcs_ref_update: "Moved branch",
    protected_ref_rule_create: "Protected branch",
    protected_path_rule_create: "Protected path",
    change_request_create: "Opened review",
    change_request_approve: "Approved",
    change_request_approval_dismiss: "Dismissed approval",
    change_request_comment_create: "Commented",
    change_request_file_view: "Viewed file",
    change_request_reviewer_assign: "Assigned reviewer",
    change_request_reject: "Rejected",
    change_request_merge: "Merged",
    workspace_create: "Created workspace",
    workspace_token_issue: "Issued token",
    workspace_token_revoke: "Revoked token",
    run_create: "Logged run",
    run_execute_create: "Queued run",
    run_execute_start: "Started run",
    run_execute_finish: "Finished run",
    run_execute_cancel: "Cancelled run",
    run_execute_failure: "Run failed",
    idempotency_quota_exceeded: "Rate limited",
  };
  return labels[action] ?? sentenceCase(action);
}

function auditLimitOptions(current: number, max: number): number[] {
  return [...new Set([25, 50, 100, 250, current])]
    .filter((value) => value <= max)
    .sort((left, right) => left - right);
}

function auditDetailPreview(details: AuditEvent["details"]): [string, string][] {
  return Object.entries(details)
    .filter(([key]) => SAFE_DETAIL_KEYS.has(key) && !RISKY_DETAIL_KEY.test(key))
    .slice(0, DETAIL_PREVIEW_LIMIT);
}

function sentenceCase(value: string): string {
  const normalized = value.replace(/_/g, " ");
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function formatTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en", {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function shortTarget(value: string): string {
  if (/^[a-f0-9]{64}$/i.test(value)) return value.slice(0, 8);
  return value;
}
