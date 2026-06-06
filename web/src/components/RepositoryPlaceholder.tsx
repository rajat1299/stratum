import type { StratumCommitInfo, StratumRef } from "@stratum/sdk";
import { useQuery } from "@tanstack/react-query";
import { useStratumClient } from "../lib/stratum-client.ts";

const repositoryKeys = {
  refs: ["repository", "refs"] as const,
  status: ["repository", "status"] as const,
  tree: ["repository", "tree"] as const,
  log: ["repository", "log"] as const,
};

export function RepositoryPlaceholder() {
  const client = useStratumClient();
  const refs = useQuery({
    queryKey: repositoryKeys.refs,
    queryFn: () => client.vcs.listRefs(),
    staleTime: 15_000,
  });
  const status = useQuery({
    queryKey: repositoryKeys.status,
    queryFn: () => client.vcs.status(),
    staleTime: 15_000,
  });
  const tree = useQuery({
    queryKey: repositoryKeys.tree,
    queryFn: () => client.search.tree(""),
    staleTime: 15_000,
  });
  const log = useQuery({
    queryKey: repositoryKeys.log,
    queryFn: () => client.vcs.log(),
    staleTime: 15_000,
  });

  const firstError = refs.error ?? status.error ?? tree.error ?? log.error;

  return (
    <div className="mx-auto max-w-6xl px-8 py-8">
      <header className="mb-6 flex flex-wrap items-end justify-between gap-4">
        <div>
          <div className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
            Workspace state
          </div>
          <h1 className="mt-1 text-[24px] font-medium leading-tight tracking-tight text-stone-900">
            Repository
          </h1>
        </div>
        <div className="rounded-md border border-stone-200 bg-stone-50 px-3 py-2 font-mono text-[11px] text-stone-600">
          {status.isLoading ? "checking" : status.data?.trim() || "clean"}
        </div>
      </header>

      {firstError && (
        <p
          role="alert"
          className="mb-4 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 font-mono text-[11.5px] text-rose-800"
        >
          {firstError.message}
        </p>
      )}

      <div className="grid gap-4 xl:grid-cols-[minmax(0,1.15fr)_minmax(320px,0.85fr)]">
        <section aria-labelledby="repository-tree-heading">
          <SectionHeader id="repository-tree-heading" title="Tree" meta="/" />
          {tree.isLoading ? (
            <LoadingBlock label="Loading tree" className="h-[420px]" />
          ) : (
            <pre className="min-h-[420px] overflow-auto rounded-md border border-stone-200 bg-white p-4 font-mono text-[12px] leading-relaxed text-stone-800 shadow-sm">
              {tree.data?.trim() || "/"}
            </pre>
          )}
        </section>

        <div className="space-y-4">
          <section aria-labelledby="repository-refs-heading">
            <SectionHeader
              id="repository-refs-heading"
              title="Refs"
              meta={`${refs.data?.refs.length ?? 0}`}
            />
            {refs.isLoading ? (
              <LoadingBlock label="Loading refs" className="h-[164px]" />
            ) : (
              <ul className="overflow-hidden rounded-md border border-stone-200 bg-white shadow-sm">
                {(refs.data?.refs ?? []).map((ref) => (
                  <li key={ref.name} className="border-b border-stone-100 last:border-b-0">
                    <RefRow refItem={ref} />
                  </li>
                ))}
                {refs.data?.refs.length === 0 && (
                  <li className="px-4 py-3 text-[13px] text-stone-500">No refs yet.</li>
                )}
              </ul>
            )}
          </section>

          <section aria-labelledby="repository-log-heading">
            <SectionHeader
              id="repository-log-heading"
              title="Commits"
              meta={`${log.data?.commits.length ?? 0}`}
            />
            {log.isLoading ? (
              <LoadingBlock label="Loading commits" className="h-[220px]" />
            ) : (
              <ul className="overflow-hidden rounded-md border border-stone-200 bg-white shadow-sm">
                {(log.data?.commits ?? []).slice(0, 12).map((commit) => (
                  <li key={commit.hash} className="border-b border-stone-100 last:border-b-0">
                    <CommitRow commit={commit} />
                  </li>
                ))}
                {log.data?.commits.length === 0 && (
                  <li className="px-4 py-3 text-[13px] text-stone-500">No commits yet.</li>
                )}
              </ul>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}

function SectionHeader({
  id,
  title,
  meta,
}: {
  readonly id: string;
  readonly title: string;
  readonly meta: string;
}) {
  return (
    <div className="mb-2 flex items-center justify-between gap-3">
      <h2 id={id} className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
        {title}
      </h2>
      <span className="font-mono text-[10.5px] uppercase tracking-wider text-stone-400">
        {meta}
      </span>
    </div>
  );
}

function RefRow({ refItem }: { readonly refItem: StratumRef }) {
  return (
    <div className="grid grid-cols-[1fr_auto] items-center gap-3 px-4 py-2.5">
      <div className="min-w-0">
        <div className="truncate font-mono text-[12.5px] text-stone-900">{refItem.name}</div>
        <div className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
          v{refItem.version}
        </div>
      </div>
      <span className="font-mono text-[11px] text-stone-500" title={refItem.target}>
        {shortHash(refItem.target)}
      </span>
    </div>
  );
}

function CommitRow({ commit }: { readonly commit: StratumCommitInfo }) {
  return (
    <div className="px-4 py-2.5">
      <div className="truncate text-[13px] font-medium text-stone-900">{commit.message}</div>
      <div className="mt-0.5 flex flex-wrap items-center gap-x-2 gap-y-0.5 font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
        <span title={commit.hash}>{shortHash(commit.hash)}</span>
        <span aria-hidden className="text-stone-300">
          |
        </span>
        <span>{commit.author}</span>
        <span aria-hidden className="text-stone-300">
          |
        </span>
        <span>{formatTimestamp(commit.timestamp)}</span>
      </div>
    </div>
  );
}

function LoadingBlock({
  label,
  className,
}: {
  readonly label: string;
  readonly className: string;
}) {
  return (
    <div
      aria-busy="true"
      aria-label={label}
      className={`${className} animate-pulse rounded-md border border-stone-200 bg-stone-50`}
    />
  );
}

function shortHash(hash: string): string {
  return hash.slice(0, 8);
}

function formatTimestamp(timestamp: number): string {
  if (!Number.isFinite(timestamp) || timestamp <= 0) return "unknown";
  return new Intl.DateTimeFormat("en", {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(timestamp * 1000));
}
