export type ReviewActorKind = "reviewer" | "agent" | "system";

export function formatReviewActor(
  uid: number | null | undefined,
  options: { readonly kind?: ReviewActorKind } = {},
): string {
  if (uid === null || uid === undefined) return "Unknown actor";
  if (uid === 0) return "root";
  if (options.kind === "agent") return `Agent ${uid}`;
  if (options.kind === "system") return `System ${uid}`;
  return `Reviewer ${uid}`;
}

export function formatReviewActorList(values: readonly number[]): string {
  if (values.length === 0) return "-";
  return values.map((uid) => formatReviewActor(uid)).join(", ");
}
