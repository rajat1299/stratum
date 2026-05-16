/**
 * Placeholder — a small editorial card used by routes that don't have
 * real content yet. Phase D / E / F replace each of these with the
 * actual surface. Lives in components/ rather than per-route so we
 * have one shared visual treatment to migrate away from.
 */

import type { ReactNode } from "react";

export interface PlaceholderProps {
  readonly phase: string;
  readonly title: string;
  readonly description: string;
  readonly children?: ReactNode;
}

export function Placeholder({ phase, title, description, children }: PlaceholderProps) {
  return (
    <div className="mx-auto max-w-2xl px-8 py-12">
      <div className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500">{phase}</div>
      <h1 className="mt-1 text-[24px] font-medium leading-tight tracking-tight text-stone-900">
        {title}
      </h1>
      <p className="mt-1 font-serif text-[14px] italic text-stone-500">{description}</p>
      {children !== undefined && <div className="mt-8">{children}</div>}
    </div>
  );
}
